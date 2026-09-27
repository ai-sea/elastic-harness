package memoryhandler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

func TestHydrateSucceedsWithoutSource(t *testing.T) {
	outcome, err := New(nil).OnState(context.Background(), pureExecution(`{"prompt":"你好"}`), effects.StateSignal{})
	if err != nil {
		t.Fatalf("未装配 Source 时 hydrate 应成功，实际：%v", err)
	}
	if outcome.Kind != effects.OutcomeSucceeded || outcome.Result != "succeeded" {
		t.Fatalf("Outcome 不符：%+v", outcome)
	}
	assertPrompt(t, outcome.ContextPatch, "你好")
}

func TestHydrateRejectsMissingPrompt(t *testing.T) {
	for name, runContext := range map[string]string{
		"字段缺失": `{}`,
		"空字符串": `{"prompt":""}`,
		"纯空白":  `{"prompt":"   "}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(nil).OnState(context.Background(), pureExecution(runContext), effects.StateSignal{}); err == nil {
				t.Fatal("prompt 无效时应返回错误")
			}
		})
	}
}

func TestHydrateRejectsMalformedContext(t *testing.T) {
	if _, err := New(nil).OnState(context.Background(), pureExecution(`{`), effects.StateSignal{}); err == nil {
		t.Fatal("Run Context 非法 JSON 时应返回错误")
	}
}

func TestHydrateRejectsNonPureState(t *testing.T) {
	execution := pureExecution(`{"prompt":"你好"}`)
	execution.Node.SideEffect = domain.SideEffectIdempotent
	if _, err := New(nil).OnState(context.Background(), execution, effects.StateSignal{}); err == nil {
		t.Fatal("非 pure 状态绑定 Memory Handler 应被拒绝")
	}
}

func TestHydrateInjectsSnippets(t *testing.T) {
	source := stubSource{snippets: []Snippet{{Source: "history", Content: "上次讨论的是预算"}}}
	outcome, err := New(source).OnState(context.Background(), pureExecution(`{"prompt":"继续"}`), effects.StateSignal{})
	if err != nil {
		t.Fatal(err)
	}
	assertPrompt(t, outcome.ContextPatch, "继续\n\n[memory]\n- (history) 上次讨论的是预算")
}

func TestHydrateCapsSnippetCount(t *testing.T) {
	over := make([]Snippet, MaxSnippets+5)
	for index := range over {
		over[index] = Snippet{Source: "history", Content: "片段"}
	}
	outcome, err := New(stubSource{snippets: over}).OnState(context.Background(), pureExecution(`{"prompt":"继续"}`), effects.StateSignal{})
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		SnippetCount int `json:"snippetCount"`
	}
	if err := json.Unmarshal(outcome.Events[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	if event.SnippetCount != MaxSnippets {
		t.Fatalf("注入片段数 = %d，期望被截断到 %d", event.SnippetCount, MaxSnippets)
	}
	if outcome.Events[0].EventType != qname.MustParse("harness/context.hydrated") {
		t.Fatalf("事件类型错误：%s", outcome.Events[0].EventType)
	}
}

// 检索失败属瞬时错误（§11.3），必须映射为可重试 Outcome 而不是向上抛错，
// 否则定义中的 retry 策略不会生效。
func TestHydrateMapsRetrievalErrorToRetryable(t *testing.T) {
	outcome, err := New(stubSource{err: errors.New("后端不可用")}).OnState(
		context.Background(), pureExecution(`{"prompt":"继续"}`), effects.StateSignal{})
	if err != nil {
		t.Fatalf("检索失败不应向上抛错：%v", err)
	}
	if outcome.Kind != effects.OutcomeRetryableFailure || outcome.ErrorCode != "memory.retrieve" {
		t.Fatalf("Outcome 应为可重试失败：%+v", outcome)
	}
}

func TestHydrateUsesSnapshotScope(t *testing.T) {
	source := &recordingSource{}
	execution := pureExecution(`{"prompt":"继续"}`)
	execution.Snapshot.TenantID = "tenant-a"
	execution.Snapshot.ChatID = "chat-a"
	if _, err := New(source).OnState(context.Background(), execution, effects.StateSignal{}); err != nil {
		t.Fatal(err)
	}
	// §12 要求默认禁止跨租户检索：作用域必须取自 Run 快照而非入参。
	if source.scope.TenantID != "tenant-a" || source.scope.ChatID != "chat-a" {
		t.Fatalf("检索作用域未取自快照：%+v", source.scope)
	}
}

type stubSource struct {
	snippets []Snippet
	err      error
}

func (s stubSource) Retrieve(context.Context, Scope, string) ([]Snippet, error) {
	return s.snippets, s.err
}

type recordingSource struct{ scope Scope }

func (s *recordingSource) Retrieve(_ context.Context, scope Scope, _ string) ([]Snippet, error) {
	s.scope = scope
	return nil, nil
}

func pureExecution(runContext string) ports.StateExecutionContext {
	return ports.StateExecutionContext{
		Snapshot: domain.RunSnapshot{RunID: "run-1", Context: json.RawMessage(runContext)},
		Node:     domain.StateNode{Type: domain.StateNormal, SideEffect: domain.SideEffectPure},
	}
}

func assertPrompt(t *testing.T, patch json.RawMessage, want string) {
	t.Helper()
	var decoded struct {
		Prompt   string `json:"prompt"`
		Hydrated bool   `json:"hydrated"`
	}
	if err := json.Unmarshal(patch, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Prompt != want {
		t.Fatalf("prompt = %q，期望 %q", decoded.Prompt, want)
	}
	if !decoded.Hydrated {
		t.Fatal("contextPatch 未标记 hydrated")
	}
}
