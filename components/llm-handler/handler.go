package llmhandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ai-sea/elastic-harness/components/tool-registry"
	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type Handler struct {
	provider ports.ModelProvider
	tools    *toolregistry.Registry
	model    string
}

func New(provider ports.ModelProvider, tools *toolregistry.Registry, model string) *Handler {
	return &Handler{provider: provider, tools: tools, model: model}
}

func (h *Handler) OnState(ctx context.Context, execution ports.StateExecutionContext, _ effects.StateSignal) (effects.StateOutcome, error) {
	if execution.Node.SideEffect != domain.SideEffectPure {
		return effects.StateOutcome{}, errors.New("LLM Handler 只能绑定 pure 状态")
	}
	var runContext map[string]any
	if err := json.Unmarshal(execution.Snapshot.Context, &runContext); err != nil {
		return effects.StateOutcome{}, err
	}
	prompt, _ := runContext["prompt"].(string)
	if prompt == "" {
		return effects.StateOutcome{}, errors.New("Run Context 缺少 prompt")
	}
	if toolResults, exists := runContext["toolResults"]; exists {
		encoded, _ := json.Marshal(toolResults)
		prompt = fmt.Sprintf("%s\n\ntoolResult: %s", prompt, encoded)
	}
	response, err := h.provider.Invoke(ctx, ports.ModelRequest{
		RunID: execution.Snapshot.RunID, Prompt: prompt, Model: h.model,
		Tools: h.tools.Definitions(ctx, execution.Snapshot.TenantID),
	})
	if err != nil {
		return effects.StateOutcome{Kind: effects.OutcomeRetryableFailure, Result: "failed", ErrorCode: "model.invoke"}, nil
	}
	trace := domain.InvocationTrace{
		Kind: "InvokeModel", Provider: response.Provider, Model: response.Model,
		ModelVersion: response.ModelVersion, ProfileRevision: execution.Snapshot.ExecutionProfile.Revision,
		RouteReason: "execution-profile", CallPath: "pure-inline",
	}
	if len(response.ToolCalls) > 0 {
		patch, _ := json.Marshal(map[string]any{"pendingToolCalls": response.ToolCalls})
		return effects.StateOutcome{
			Kind: effects.OutcomeSucceeded, Result: "toolRequested", ContextPatch: patch,
			Usage: response.Usage, Invocations: []domain.InvocationTrace{trace},
		}, nil
	}
	patch, _ := json.Marshal(map[string]any{"assistant": response.Content})
	eventPayload, _ := json.Marshal(map[string]string{"role": "assistant", "content": response.Content})
	return effects.StateOutcome{
		Kind: effects.OutcomeSucceeded, Result: "completed", ContextPatch: patch,
		Usage: response.Usage, Invocations: []domain.InvocationTrace{trace},
		Events: []domain.EventEnvelope{{
			EventType: qname.MustParse("harness/message.completed"), Producer: "harness/llm-handler", Payload: eventPayload,
		}},
	}, nil
}
