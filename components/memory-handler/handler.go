// Package memoryhandler 提供 Phase 1 的最小记忆注入能力：
// 实现 §5 定义中 hydrate 状态所需的 harness/context.hydrate 能力。
//
// Phase 1 不引入长期 Memory（那是 §16 Phase 2 的范围），因此本组件只在
// Run Context 中做"补齐 prompt + 按上限注入检索片段"的确定性工作：
// 未装配 Source 时退化为纯校验，保证 default-agent-loop 的定义可完整校验与执行。
package memoryhandler

import (
	"context"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

// MaxSnippets 是单次注入的片段上限，用于对齐 §17 风险 4「上下文膨胀」：
// 记忆片段必须有硬上限，不能随历史增长。
const MaxSnippets = 8

// Scope 描述一次记忆检索的作用域。§12 要求默认禁止跨租户检索，
// 因此 TenantID 为必填项，由 Run 快照提供而非调用方自由指定。
type Scope struct {
	TenantID string
	ChatID   string
	RunID    string
}

// Snippet 是一条被检索到的记忆片段。Source 不返回二进制或大对象，
// 大内容应以 ArtifactRef 形式引用（§6 大对象模型）。
type Snippet struct {
	Source  string
	Content string
}

// Source 是记忆检索的消费侧接口。Phase 1 没有内存后端适配器，
// 故暂留在组件内；Phase 2 引入真实 Memory 实现时提升为 core/ports 的正式 Port。
type Source interface {
	Retrieve(ctx context.Context, scope Scope, query string) ([]Snippet, error)
}

// Handler 实现 harness/context.hydrate 状态语义。它只做 pure 工作：
// 读取 Run Context、按上限拼接片段、写回 contextPatch，不改变外部世界。
type Handler struct {
	source Source
}

func New(source Source) *Handler { return &Handler{source: source} }

func (h *Handler) OnState(ctx context.Context, execution ports.StateExecutionContext, _ effects.StateSignal) (effects.StateOutcome, error) {
	if execution.Node.SideEffect != domain.SideEffectPure {
		return effects.StateOutcome{}, errSideEffect
	}
	prompt, err := promptOf(execution.Snapshot.Context)
	if err != nil {
		return effects.StateOutcome{}, err
	}
	snippets, err := h.retrieve(ctx, execution.Snapshot, prompt)
	if err != nil {
		return retryableFailure("memory.retrieve"), nil
	}
	patch, err := hydratePatch(prompt, snippets)
	if err != nil {
		return effects.StateOutcome{}, err
	}
	return effects.StateOutcome{
		Kind: effects.OutcomeSucceeded, Result: "succeeded",
		ContextPatch: patch, Events: []domain.EventEnvelope{hydratedEvent(snippets)},
	}, nil
}

// retrieve 在未装配 Source 时返回空结果，使 hydrate 退化为可验证的空操作。
func (h *Handler) retrieve(ctx context.Context, snapshot domain.RunSnapshot, query string) ([]Snippet, error) {
	if h.source == nil {
		return nil, nil
	}
	snippets, err := h.source.Retrieve(ctx, Scope{
		TenantID: snapshot.TenantID, ChatID: snapshot.ChatID, RunID: snapshot.RunID,
	}, query)
	if err != nil {
		return nil, err
	}
	if len(snippets) > MaxSnippets {
		return snippets[:MaxSnippets], nil
	}
	return snippets, nil
}

func retryableFailure(code string) effects.StateOutcome {
	return effects.StateOutcome{Kind: effects.OutcomeRetryableFailure, Result: "failed", ErrorCode: code}
}

func hydratedEvent(snippets []Snippet) domain.EventEnvelope {
	return domain.EventEnvelope{
		EventType: qname.MustParse("harness/context.hydrated"),
		Producer:  "harness/memory-handler",
		Payload:   mustJSON(map[string]any{"snippetCount": len(snippets)}),
	}
}
