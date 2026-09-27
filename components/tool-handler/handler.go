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
	now func() time.Time
}

func New(now func() time.Time) *Handler {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Handler{now: now}
}

func (h *Handler) OnState(_ context.Context, execution ports.StateExecutionContext, signal effects.StateSignal) (effects.StateOutcome, error) {
	if execution.Node.SideEffect == domain.SideEffectPure {
		return effects.StateOutcome{}, errors.New("Tool Handler 不得绑定 pure 状态")
	}
	if signal.Type.Equal(qname.MustParse("harness/effect.completed")) {
		var result map[string]any
		if err := json.Unmarshal(signal.Payload, &result); err != nil {
			return effects.StateOutcome{}, err
		}
		patch, _ := json.Marshal(map[string]any{"pendingToolCalls": nil, "toolResult": result})
		return effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "succeeded", ContextPatch: patch}, nil
	}
	var runContext struct {
		PendingToolCalls []ports.ToolCall `json:"pendingToolCalls"`
	}
	if err := json.Unmarshal(execution.Snapshot.Context, &runContext); err != nil {
		return effects.StateOutcome{}, err
	}
	if len(runContext.PendingToolCalls) == 0 {
		return effects.StateOutcome{}, errors.New("Run Context 缺少 pendingToolCalls")
	}
	intents := make([]effects.EffectIntent, 0, len(runContext.PendingToolCalls))
	for _, call := range runContext.PendingToolCalls {
		intent, _ := json.Marshal(call)
		intents = append(intents, effects.EffectIntent{
			EffectID: call.CallID, Kind: qname.MustParse("harness/tool.invoke"), Intent: intent,
			IdempotencyKey: execution.Snapshot.RunID + "/" + call.CallID,
			Deadline:       h.now().Add(execution.Node.Timeouts.StartToClose), SideEffect: execution.Node.SideEffect,
		})
	}
	timerDue := h.now().Add(execution.Node.Timeouts.Callback)
	return effects.StateOutcome{
		Kind: effects.OutcomeWaiting, Effects: intents,
		Timers: []effects.TimerIntent{{Kind: "callback", DueAt: timerDue, SignalType: qname.MustParse("harness/timer.fired")}},
	}, nil
}
