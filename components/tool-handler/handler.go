package toolhandler

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type Handler struct {
	catalog ports.ToolCatalog
	now     func() time.Time
}

func New(catalog ports.ToolCatalog, now func() time.Time) *Handler {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Handler{catalog: catalog, now: now}
}

func (h *Handler) OnState(ctx context.Context, execution ports.StateExecutionContext, signal effects.StateSignal) (effects.StateOutcome, error) {
	if execution.Node.SideEffect == domain.SideEffectPure {
		return effects.StateOutcome{}, errors.New("Tool Handler 不得绑定 pure 状态")
	}
	// 回调 Timer 到期即「丢失回调」（§11.1）：等待中的工具调用未在 Callback 期限内
	// 回执，按 timedOut 终态收敛。不得重新生成 Effect 意图——同一 effectId 再次落账
	// 会造成同一外部调用被执行两次。
	if signal.Type.Equal(qname.MustParse("harness/timer.fired")) {
		patch, _ := json.Marshal(map[string]any{"pendingToolCalls": nil})
		return effects.StateOutcome{
			Kind: effects.OutcomeTerminalFailure, Result: "timedOut", ErrorCode: "timedOut", ContextPatch: patch,
		}, nil
	}
	var runContext struct {
		PendingToolCalls []ports.ToolCall `json:"pendingToolCalls"`
		ToolResults      map[string]any   `json:"toolResults"`
	}
	if err := json.Unmarshal(execution.Snapshot.Context, &runContext); err != nil {
		return effects.StateOutcome{}, err
	}
	if signal.Type.Equal(qname.MustParse("harness/effect.completed")) {
		return completedOutcome(runContext.PendingToolCalls, runContext.ToolResults, signal)
	}
	if len(runContext.PendingToolCalls) == 0 {
		return effects.StateOutcome{}, errors.New("Run Context 缺少 pendingToolCalls")
	}
	intents := make([]effects.EffectIntent, 0, len(runContext.PendingToolCalls))
	invocations := make([]effects.ToolInvocation, 0, len(runContext.PendingToolCalls))
	for _, call := range runContext.PendingToolCalls {
		if h.catalog == nil {
			return effects.StateOutcome{}, errors.New("Tool Handler 缺少 ToolCatalog")
		}
		registration, err := h.catalog.ResolveTool(ctx, execution.Snapshot.TenantID, call.Tool)
		if err != nil {
			return effects.StateOutcome{}, err
		}
		resolved := effects.ToolEffectIntent{
			CallID: call.CallID, Tool: call.Tool, Arguments: call.Arguments,
			RegistrationID: registration.RegistrationID, RegistrationRevision: registration.Revision,
			Endpoint: registration.Endpoint,
		}
		intent, _ := json.Marshal(resolved)
		idempotencyKey := execution.Snapshot.RunID + "/" + call.CallID
		intents = append(intents, effects.EffectIntent{
			EffectID: call.CallID, Kind: qname.MustParse("harness/tool.invoke"), Intent: intent,
			IdempotencyKey: idempotencyKey,
			Deadline:       h.now().Add(execution.Node.Timeouts.StartToClose), SideEffect: execution.Node.SideEffect,
		})
		invocations = append(invocations, effects.ToolInvocation{
			InvocationID: call.CallID, TenantID: execution.Snapshot.TenantID, RunID: execution.Snapshot.RunID,
			ToolID: call.Tool, RegistrationID: registration.RegistrationID,
			RegistrationRevision: registration.Revision, Status: effects.InvocationPending,
			InvocationVersion: 1, Attempt: 1, IdempotencyKey: idempotencyKey,
			CallbackDeadline: h.now().Add(execution.Node.Timeouts.Callback),
		})
	}
	timerDue := h.now().Add(execution.Node.Timeouts.Callback)
	return effects.StateOutcome{
		Kind: effects.OutcomeWaiting, Effects: intents, ToolInvocations: invocations,
		Timers: []effects.TimerIntent{{Kind: "callback", DueAt: timerDue, SignalType: qname.MustParse("harness/timer.fired")}},
	}, nil
}

func completedOutcome(pending []ports.ToolCall, results map[string]any, signal effects.StateSignal) (effects.StateOutcome, error) {
	var result map[string]any
	if err := json.Unmarshal(signal.Payload, &result); err != nil {
		return effects.StateOutcome{}, err
	}
	effectID, _ := result["effectId"].(string)
	remaining := make([]ports.ToolCall, 0, len(pending))
	found := false
	for _, call := range pending {
		if call.CallID == effectID {
			found = true
			continue
		}
		remaining = append(remaining, call)
	}
	if !found {
		return effects.StateOutcome{}, errors.New("完成回执不属于当前待处理工具调用")
	}
	if results == nil {
		results = make(map[string]any)
	}
	results[effectID] = result
	if len(remaining) == 0 {
		remaining = nil
	}
	patch, _ := json.Marshal(map[string]any{"pendingToolCalls": remaining, "toolResults": results})
	if len(remaining) > 0 {
		return effects.StateOutcome{Kind: effects.OutcomeWaiting, ContextPatch: patch}, nil
	}
	return effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "succeeded", ContextPatch: patch}, nil
}
