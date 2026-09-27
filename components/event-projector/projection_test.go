package projector

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

// 不变量 #5：单个 Run 流内 sequence 严格单调；重复投递与乱序到达都不得污染视图。
func TestApplyIgnoresDuplicateAndOutOfOrderEvents(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	apply(t, &view, assistantEvent("run-1", 1, "第一句"))
	apply(t, &view, assistantEvent("run-1", 2, "第二句"))
	apply(t, &view, assistantEvent("run-1", 2, "第二句（重复投递）"))
	apply(t, &view, assistantEvent("run-1", 1, "第一句（迟到）"))

	if len(view.Messages) != 2 {
		t.Fatalf("消息数 = %d，期望 2；重复/迟到事件不应被折叠", len(view.Messages))
	}
	if view.RunCursors["run-1"] != 2 {
		t.Fatalf("游标 = %d，期望 2", view.RunCursors["run-1"])
	}
	if view.Messages[1].Content != "第二句" {
		t.Fatalf("消息内容被改写：%q", view.Messages[1].Content)
	}
}

func TestApplyIsIdempotentOnReplay(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	events := []domain.EventEnvelope{assistantEvent("run-1", 1, "第一句"), assistantEvent("run-1", 2, "第二句")}
	for round := 0; round < 3; round++ {
		for _, event := range events {
			apply(t, &view, event)
		}
	}
	// 全量回放（§6.2 投影从持久事件追平）必须收敛到同一结果。
	if len(view.Messages) != 2 {
		t.Fatalf("回放三次后消息数 = %d，期望 2", len(view.Messages))
	}
}

func TestApplyTracksActiveRuns(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	apply(t, &view, transitionEvent("run-1", 1, domain.LifecycleRunnable))
	apply(t, &view, transitionEvent("run-2", 1, domain.LifecycleWaiting))
	apply(t, &view, transitionEvent("run-1", 2, domain.LifecycleTerminal))

	if len(view.ActiveRuns) != 1 || view.ActiveRuns[0] != "run-2" {
		t.Fatalf("活跃 Run 集合错误：%+v", view.ActiveRuns)
	}
}

func TestApplyIgnoresEventWithoutCorrelation(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	event := assistantEvent("", 1, "无归属")
	changed, err := view.Apply(event)
	if err != nil || changed {
		t.Fatalf("缺少 correlationId 的事件应被忽略：changed=%v err=%v", changed, err)
	}
	if view.LastActive != "" {
		t.Fatal("被忽略的事件不应推进视图水位")
	}
}

func TestApplyIgnoresNonAssistantMessages(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	payload, _ := json.Marshal(map[string]string{"role": "user", "content": "用户输入"})
	apply(t, &view, event("run-1", 1, "harness/message.completed", payload))
	if len(view.Messages) != 0 {
		t.Fatalf("用户消息不应进入 assistant 视图：%+v", view.Messages)
	}
}

func TestApplyRejectsMalformedMessagePayload(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	if _, err := view.Apply(event("run-1", 1, "harness/message.completed", []byte(`{`))); err == nil {
		t.Fatal("非法载荷应返回错误，而不是静默吞掉")
	}
	// 关键边界：载荷无法解析时不得推进水位，否则重投也救不回这条事件。
	if view.RunCursors["run-1"] != 0 {
		t.Fatalf("非法载荷不应推进水位：%d", view.RunCursors["run-1"])
	}
}

func TestApplyRejectsMalformedTransitionPayload(t *testing.T) {
	view := NewChatView("tenant-1", "chat-1")
	if _, err := view.Apply(event("run-1", 1, "harness/state.transitioned", []byte(`{`))); err == nil {
		t.Fatal("非法迁移载荷应返回错误")
	}
	if view.RunCursors["run-1"] != 0 {
		t.Fatal("非法迁移载荷不应推进水位")
	}
}

func TestProjectionResolvesChatFromAuthoritativeState(t *testing.T) {
	sink := NewMemorySink()
	projection := NewProjection(stubLocator{runs: map[string]domain.RunSnapshot{
		"run-1": {TenantID: "tenant-1", ChatID: "chat-1", RunID: "run-1"},
	}}, sink)
	applied, err := projection.ConsumeOnce(context.Background(), []ports.Message{message(t, assistantEvent("run-1", 1, "第一句"))})
	if err != nil || applied != 1 {
		t.Fatalf("消费结果 = %d/%v，期望 1/nil", applied, err)
	}
	view, _ := sink.View("tenant-1", "chat-1")
	if len(view.Messages) != 1 {
		t.Fatalf("视图未落库：%+v", view)
	}
}

// 一条毒消息不得让整批投影停摆（§17 风险 5：至少一次投递下的现实噪音）。
func TestProjectionContinuesPastPoisonMessage(t *testing.T) {
	sink := NewMemorySink()
	projection := NewProjection(stubLocator{runs: map[string]domain.RunSnapshot{
		"run-1": {TenantID: "tenant-1", ChatID: "chat-1", RunID: "run-1"},
	}}, sink)
	applied, err := projection.ConsumeOnce(context.Background(), []ports.Message{
		{Key: "run-1", Body: []byte(`{`)},
		message(t, assistantEvent("run-1", 1, "第一句")),
	})
	if err == nil {
		t.Fatal("毒消息应被记录为失败")
	}
	if applied != 1 {
		t.Fatalf("有效消息仍应被应用：applied=%d", applied)
	}
}

func TestProjectionFailsWhenRunUnknown(t *testing.T) {
	projection := NewProjection(stubLocator{runs: map[string]domain.RunSnapshot{}}, NewMemorySink())
	_, err := projection.ConsumeOnce(context.Background(), []ports.Message{message(t, assistantEvent("run-x", 1, "第一句"))})
	if err == nil {
		t.Fatal("Run 不存在时应返回错误")
	}
}

type stubLocator struct{ runs map[string]domain.RunSnapshot }

func (s stubLocator) GetRun(_ context.Context, runID string) (domain.RunSnapshot, error) {
	run, exists := s.runs[runID]
	if !exists {
		return domain.RunSnapshot{}, errors.New("run 不存在")
	}
	return run, nil
}

func assistantEvent(runID string, sequence int64, content string) domain.EventEnvelope {
	payload, _ := json.Marshal(map[string]string{"role": "assistant", "content": content})
	return event(runID, sequence, "harness/message.completed", payload)
}

func transitionEvent(runID string, sequence int64, lifecycle domain.LifecycleStatus) domain.EventEnvelope {
	payload, _ := json.Marshal(map[string]any{"lifecycleStatus": string(lifecycle), "state": "call-model"})
	return event(runID, sequence, "harness/state.transitioned", payload)
}

func event(runID string, sequence int64, eventType string, payload []byte) domain.EventEnvelope {
	return domain.EventEnvelope{
		EventID: "evt-1", EventType: qname.MustParse(eventType), TenantID: "tenant-1",
		StreamID: "run/" + runID, Sequence: sequence, OccurredAt: time.Unix(1700000000+sequence, 0).UTC(),
		CorrelationID: runID, Producer: "harness/onstate-runtime", SchemaVersion: 1, Payload: payload,
	}
}

func apply(t *testing.T, view *ChatView, event domain.EventEnvelope) {
	t.Helper()
	if _, err := view.Apply(event); err != nil {
		t.Fatalf("应用事件失败：%v", err)
	}
}

func message(t *testing.T, event domain.EventEnvelope) ports.Message {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return ports.Message{Key: event.CorrelationID, Body: body}
}
