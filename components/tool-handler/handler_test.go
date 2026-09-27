package toolhandler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

func TestEnteredSignalCreatesEffectIntentAndCallbackTimer(t *testing.T) {
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	handler := New(func() time.Time { return now })
	outcome, err := handler.OnState(context.Background(), execution(nil), signal("harness/state.entered", nil))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != effects.OutcomeWaiting {
		t.Fatalf("Outcome = %s，期望 waiting", outcome.Kind)
	}
	if len(outcome.Effects) != 1 || outcome.Effects[0].EffectID != "call-1" {
		t.Fatalf("Effect 意图错误：%+v", outcome.Effects)
	}
	// 幂等键以 Run 为作用域，保证重投/恢复时同一调用可被对端去重（§11 幂等作用域）。
	if outcome.Effects[0].IdempotencyKey != "run-1/call-1" {
		t.Fatalf("幂等键 = %q", outcome.Effects[0].IdempotencyKey)
	}
	if len(outcome.Timers) != 1 || outcome.Timers[0].DueAt != now.Add(30*time.Second) {
		t.Fatalf("Callback Timer 未按期限落账：%+v", outcome.Timers)
	}
	if !outcome.Timers[0].SignalType.Equal(qname.MustParse("harness/timer.fired")) {
		t.Fatalf("Timer 信号类型错误：%s", outcome.Timers[0].SignalType)
	}
}

// §11.1「回调丢失」：Callback 期限内未收到回执时，timer.fired 必须收敛为 timedOut，
// 而不是带着同一 effectId 重新生成意图（那会把同一外部调用执行两次）。
func TestTimerFiredConvergesToCallbackTimeout(t *testing.T) {
	handler := New(nil)
	outcome, err := handler.OnState(context.Background(), execution(nil), signal("harness/timer.fired", nil))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != effects.OutcomeTerminalFailure || outcome.Result != "timedOut" {
		t.Fatalf("Outcome = %s/%q，期望 terminalFailure/timedOut", outcome.Kind, outcome.Result)
	}
	if len(outcome.Effects) != 0 {
		t.Fatalf("超时收敛不得重新生成 Effect 意图：%+v", outcome.Effects)
	}
	var patch map[string]any
	if err := json.Unmarshal(outcome.ContextPatch, &patch); err != nil {
		t.Fatal(err)
	}
	calls, exists := patch["pendingToolCalls"]
	if !exists || calls != nil {
		t.Fatalf("超时必须清掉 pendingToolCalls，避免恢复后重放：%v", patch)
	}
}

func TestEffectCompletedFoldsToolResult(t *testing.T) {
	handler := New(nil)
	payload, _ := json.Marshal(map[string]any{"effectId": "call-1", "result": map[string]string{"echo": "你好"}})
	outcome, err := handler.OnState(context.Background(), execution(nil), signal("harness/effect.completed", payload))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != effects.OutcomeSucceeded || outcome.Result != "succeeded" {
		t.Fatalf("Outcome = %s/%q，期望 succeeded", outcome.Kind, outcome.Result)
	}
	var patch map[string]any
	if err := json.Unmarshal(outcome.ContextPatch, &patch); err != nil {
		t.Fatal(err)
	}
	if patch["pendingToolCalls"] != nil {
		t.Fatal("完成后必须清掉 pendingToolCalls")
	}
	if _, exists := patch["toolResult"]; !exists {
		t.Fatalf("工具结果必须写回 Context 供下一轮模型使用：%v", patch)
	}
}

func TestRejectsPureStateBinding(t *testing.T) {
	handler := New(nil)
	node := domain.StateNode{SideEffect: domain.SideEffectPure}
	exec := ports.StateExecutionContext{Snapshot: snapshot(json.RawMessage(`{"pendingToolCalls":[]}`)), Node: node}
	if _, err := handler.OnState(context.Background(), exec, signal("harness/state.entered", nil)); err == nil {
		t.Fatal("改变世界的能力不得绑定 pure 状态（§5.3.2 两相派发前提）")
	}
}

func TestEnteredSignalWithoutPendingToolCallsFails(t *testing.T) {
	handler := New(nil)
	for _, runContext := range []json.RawMessage{
		json.RawMessage(`{}`),                      // 完全没有 pendingToolCalls 字段
		json.RawMessage(`{"prompt":"你好"}`),         // 有其他字段但没有待调用列表
		json.RawMessage(`{"pendingToolCalls":[]}`), // 空列表
	} {
		if _, err := handler.OnState(context.Background(), execution(runContext), signal("harness/state.entered", nil)); err == nil {
			t.Fatalf("Context %s 缺少待调用工具时必须显式失败，而不是悄悄创建空调用", runContext)
		}
	}
}

func execution(contextPatch json.RawMessage) ports.StateExecutionContext {
	node := domain.StateNode{
		Type: domain.StateWait, SideEffect: domain.SideEffectIdempotent,
		Timeouts: domain.Timeouts{StartToClose: 10 * time.Second, Callback: 30 * time.Second},
	}
	if contextPatch == nil {
		contextPatch = json.RawMessage(`{"pendingToolCalls":[{"callId":"call-1","tool":"harness/echo","arguments":{"text":"hi"}}]}`)
	}
	return ports.StateExecutionContext{Snapshot: snapshot(contextPatch), Node: node, Attempt: 1}
}

func snapshot(context json.RawMessage) domain.RunSnapshot {
	return domain.RunSnapshot{TenantID: "tenant-1", RunID: "run-1", Context: context}
}

func signal(signalType string, payload json.RawMessage) effects.StateSignal {
	return effects.StateSignal{
		SignalID: "sig-1", RunID: "run-1", Type: qname.MustParse(signalType), Payload: payload,
	}
}
