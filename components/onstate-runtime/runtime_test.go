package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

func TestRunnableTransitionWritesContinuationInSameCommit(t *testing.T) {
	fixture := newFixture(t, effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "next"})
	if err := fixture.engine.ProcessRun(context.Background(), "run-1", "worker-1"); err != nil {
		t.Fatal(err)
	}
	commit := fixture.store.commit
	if commit.Continuation == nil {
		t.Fatal("RUNNABLE 迁移必须在同一事务写 entered Signal")
	}
	if got, want := commit.Continuation.DedupeKey, "run-1/2"; got != want {
		t.Fatalf("续跑 dedupeKey = %q，期望 %q", got, want)
	}
	if commit.Snapshot.StateEnterCounter != 2 {
		t.Fatalf("进入新状态时 stateEnterCounter = %d，期望 2", commit.Snapshot.StateEnterCounter)
	}
	if commit.Snapshot.LifecycleStatus != domain.LifecycleRunnable {
		t.Fatalf("生命周期 = %s，期望 RUNNABLE", commit.Snapshot.LifecycleStatus)
	}
}

func TestWaitingDoesNotChangeStateInstance(t *testing.T) {
	fixture := newFixture(t, effects.StateOutcome{Kind: effects.OutcomeWaiting})
	if err := fixture.engine.ProcessRun(context.Background(), "run-1", "worker-1"); err != nil {
		t.Fatal(err)
	}
	commit := fixture.store.commit
	if commit.Continuation != nil {
		t.Fatal("WAITING 不得创建内部续跑 Signal")
	}
	if commit.Snapshot.StateEnterCounter != 1 {
		t.Fatalf("原地等待不得增加 stateEnterCounter，得到 %d", commit.Snapshot.StateEnterCounter)
	}
	if commit.Snapshot.LifecycleStatus != domain.LifecycleWaiting {
		t.Fatalf("生命周期 = %s，期望 WAITING", commit.Snapshot.LifecycleStatus)
	}
}

func TestPureStateCannotCreateEffectIntent(t *testing.T) {
	outcome := effects.StateOutcome{
		Kind:    effects.OutcomeWaiting,
		Effects: []effects.EffectIntent{{Kind: qname.MustParse("harness/tool.invoke"), SideEffect: domain.SideEffectIdempotent}},
	}
	fixture := newFixture(t, outcome)
	err := fixture.engine.ProcessRun(context.Background(), "run-1", "worker-1")
	if !errors.Is(err, ErrInvalidOutcome) {
		t.Fatalf("pure 状态创建 Effect 必须失败，得到：%v", err)
	}
	if fixture.store.committed {
		t.Fatal("非法 Outcome 不得提交任何状态")
	}
}

func TestTerminalStateCannotBeRevived(t *testing.T) {
	fixture := newFixture(t, effects.StateOutcome{Kind: effects.OutcomeSucceeded, Result: "completed"})
	fixture.definition.States["model"] = domain.StateNode{
		Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
		Requires:    fixture.definition.States["model"].Requires,
		Transitions: map[string]string{"completed": "done"},
	}
	if err := fixture.engine.ProcessRun(context.Background(), "run-1", "worker-1"); err != nil {
		t.Fatal(err)
	}
	commit := fixture.store.commit
	if commit.Snapshot.LifecycleStatus != domain.LifecycleTerminal || commit.Snapshot.TerminalReason == nil || *commit.Snapshot.TerminalReason != domain.TerminalCompleted {
		t.Fatalf("终局推导错误：%+v", commit.Snapshot)
	}
	if commit.Continuation != nil {
		t.Fatal("终态不得创建续跑 Signal")
	}
}

type fixture struct {
	engine     *Engine
	store      *recordingStore
	definition domain.HarnessDefinition
}

func newFixture(t *testing.T, outcome effects.StateOutcome) fixture {
	t.Helper()
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	binding := domain.HandlerBinding{
		HandlerID: qname.MustParse("harness/test.handler"), HandlerVersion: "1.0.0", Deployment: "in-process",
	}
	definition := domain.HarnessDefinition{
		ID: "agent", Version: "1.0.0", Initial: "model", Published: true,
		States: map[string]domain.StateNode{
			"model": {
				Type: domain.StateNormal, SideEffect: domain.SideEffectPure,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.handler"), Version: "^1"},
				Transitions: map[string]string{"next": "tools", "completed": "done"},
			},
			"tools": {
				Type: domain.StateWait, SideEffect: domain.SideEffectNonIdempotent,
				Requires:    domain.RequirementSpec{Capability: qname.MustParse("harness/test.handler"), Version: "^1"},
				Transitions: map[string]string{"completed": "done"},
			},
			"done": {Type: domain.StateTerminal},
		},
	}
	snapshot := domain.RunSnapshot{
		TenantID: "tenant-1", ChatID: "chat-1", RunID: "run-1", LifecycleStatus: domain.LifecycleRunnable,
		CurrentState: "model", StateVersion: 1, StateEnterCounter: 1,
		Harness:          domain.HarnessRef{ID: definition.ID, Version: definition.Version},
		ResolvedBindings: map[string]domain.HandlerBinding{"model": binding, "tools": binding},
		PendingCount:     1, Context: json.RawMessage(`{"prompt":"hello"}`), Budget: domain.Budget{MaxSteps: 10}, UpdatedAt: now,
	}
	store := &recordingStore{load: ports.ExecutionLoad{
		Snapshot: snapshot, FencingToken: 7,
		Signal: effects.StateSignal{
			SignalID: "signal-1", RunID: "run-1", Type: qname.MustParse("harness/state.entered"),
			DedupeKey: "run-1/1", OccurredAt: now,
		},
	}}
	repository := staticDefinitions{value: definition}
	resolver := staticResolver{binding: binding, handler: staticHandler{outcome: outcome}}
	engine := New(store, repository, resolver, Options{Now: func() time.Time { return now }, IDs: &sequentialIDs{}})
	return fixture{engine: engine, store: store, definition: definition}
}

type sequentialIDs struct{ next int }

func (g *sequentialIDs) New(prefix string) string {
	g.next++
	return fmt.Sprintf("%s%d", prefix, g.next)
}

type staticHandler struct{ outcome effects.StateOutcome }

func (h staticHandler) OnState(context.Context, ports.StateExecutionContext, effects.StateSignal) (effects.StateOutcome, error) {
	return h.outcome, nil
}

type staticDefinitions struct{ value domain.HarnessDefinition }

func (r staticDefinitions) PutDraft(context.Context, domain.HarnessDefinition) error { return nil }
func (r staticDefinitions) Publish(context.Context, string, string) (domain.HarnessDefinition, error) {
	return r.value, nil
}
func (r staticDefinitions) Get(context.Context, string, string) (domain.HarnessDefinition, error) {
	return r.value, nil
}

type staticResolver struct {
	binding domain.HandlerBinding
	handler ports.StateHandler
}

func (r staticResolver) Resolve(context.Context, string, domain.RequirementSpec) (domain.HandlerBinding, error) {
	return r.binding, nil
}
func (r staticResolver) Handler(context.Context, domain.HandlerBinding) (ports.StateHandler, error) {
	return r.handler, nil
}

type recordingStore struct {
	load      ports.ExecutionLoad
	commit    ports.TransitionCommit
	committed bool
}

func (s *recordingStore) CreateChat(context.Context, domain.Chat) error       { return nil }
func (s *recordingStore) AppendMessage(context.Context, domain.Message) error { return nil }
func (s *recordingStore) CreateRun(context.Context, domain.RunSnapshot, effects.StateSignal, []ports.OutboxRecord) error {
	return nil
}
func (s *recordingStore) GetRun(context.Context, string) (domain.RunSnapshot, error) {
	return s.load.Snapshot, nil
}
func (s *recordingStore) PutSignal(context.Context, effects.StateSignal, ports.OutboxRecord) (bool, error) {
	return true, nil
}
func (s *recordingStore) AcquireExecution(context.Context, string, string, time.Duration) (ports.ExecutionLoad, error) {
	return s.load, nil
}
func (s *recordingStore) CommitTransition(_ context.Context, commit ports.TransitionCommit) error {
	s.commit, s.committed = commit, true
	return nil
}
func (s *recordingStore) ReleaseLease(context.Context, string, int64) error    { return nil }
func (s *recordingStore) Steps(context.Context, string) ([]domain.Step, error) { return nil, nil }
func (s *recordingStore) Events(context.Context, string, int64, int) ([]domain.EventEnvelope, error) {
	return nil, nil
}
func (s *recordingStore) Inbox(context.Context, string) ([]effects.StateSignal, error) {
	return nil, nil
}
func (s *recordingStore) Effects(context.Context, string) ([]effects.EffectLedgerEntry, error) {
	return nil, nil
}
func (s *recordingStore) PendingEffects(context.Context, int) ([]effects.EffectLedgerEntry, error) {
	return nil, nil
}
func (s *recordingStore) ClaimEffect(context.Context, string, int64, string) (effects.EffectLedgerEntry, error) {
	return effects.EffectLedgerEntry{}, nil
}
func (s *recordingStore) CommitEffect(context.Context, string, int64, string, string) error {
	return nil
}
func (s *recordingStore) MarkEffectManual(context.Context, string, int64) error { return nil }
func (s *recordingStore) DueTimers(context.Context, time.Time, int) ([]effects.Timer, error) {
	return nil, nil
}
func (s *recordingStore) FireTimer(context.Context, string, int64, effects.StateSignal, ports.OutboxRecord) (bool, error) {
	return false, nil
}
func (s *recordingStore) PendingOutbox(context.Context, int) ([]ports.OutboxRecord, error) {
	return nil, nil
}
func (s *recordingStore) MarkOutboxPublished(context.Context, string) error { return nil }
func (s *recordingStore) RunnableWithoutSignal(context.Context, int) ([]domain.RunSnapshot, error) {
	return nil, nil
}
