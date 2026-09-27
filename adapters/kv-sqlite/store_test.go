package kvsqlite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

func TestRunCASAndFencingTokenAreTheOnlyCommitAuthority(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	createTestRun(t, store, now)

	first, err := store.AcquireExecution(context.Background(), "run-1", "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	second, err := store.AcquireExecution(context.Background(), "run-1", "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	stale := transitionFrom(first, now)
	if err := store.CommitTransition(context.Background(), stale); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("过期 fencing token 必须提交失败，得到：%v", err)
	}
	commit := transitionFrom(second, now)
	if err := store.CommitTransition(context.Background(), commit); err != nil {
		t.Fatalf("当前持有者提交失败：%v", err)
	}
	run, err := store.GetRun(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.StateVersion != 2 {
		t.Fatalf("stateVersion = %d，期望 2", run.StateVersion)
	}
}

func TestInboxDeduplicatesAndEffectCASIsIndependent(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	createTestRun(t, store, now)
	signal := testSignal("signal-duplicate", "message/one", now.Add(time.Second))
	outbox := testOutbox("outbox-duplicate", "run-1", signal)
	inserted, err := store.PutSignal(context.Background(), signal, outbox)
	if err != nil || !inserted {
		t.Fatalf("首次写入 Signal 失败：inserted=%v err=%v", inserted, err)
	}
	signal.SignalID = "signal-duplicate-2"
	inserted, err = store.PutSignal(context.Background(), signal, outbox)
	if err != nil || inserted {
		t.Fatalf("重复 dedupeKey 必须折叠：inserted=%v err=%v", inserted, err)
	}

	load, err := store.AcquireExecution(context.Background(), "run-1", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	commit := transitionFrom(load, now)
	effect := effects.EffectLedgerEntry{
		EffectID: "effect-1", RunID: "run-1", Kind: qname.MustParse("harness/tool.invoke"),
		Status: effects.EffectPending, LedgerVersion: 1, IdempotencyKey: "run-1/effect-1",
		SideEffect: domain.SideEffectIdempotent, Deadline: now.Add(time.Minute), CreatedAt: now,
	}
	commit.Effects = []effects.EffectLedgerEntry{effect}
	if err := store.CommitTransition(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimEffect(context.Background(), effect.EffectID, 1, "dispatcher-a")
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != effects.EffectDispatched || claimed.LedgerVersion != 2 {
		t.Fatalf("派发权状态错误：%+v", claimed)
	}
	if _, err := store.ClaimEffect(context.Background(), effect.EffectID, 1, "dispatcher-b"); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("第二个 Dispatcher 不得取得派发权：%v", err)
	}
	if err := store.CommitEffect(context.Background(), effect.EffectID, 2, "external-1", "artifact://result"); err != nil {
		t.Fatal(err)
	}
}

func TestTimerUsesStateEnterCounter(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	createTestRun(t, store, now)
	load, err := store.AcquireExecution(context.Background(), "run-1", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	commit := transitionFrom(load, now)
	commit.Snapshot.CurrentState = "next"
	commit.Snapshot.StateEnterCounter = 2
	timer := effects.Timer{
		TimerID: "timer-1", RunID: "run-1", Kind: "state", DueAt: now,
		SignalType: qname.MustParse("harness/timer.fired"), StateVersionAtSchedule: 2,
		EnteredAtCounter: 1, Status: effects.TimerScheduled,
	}
	commit.Timers = []effects.Timer{timer}
	if err := store.CommitTransition(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	signal := testSignal("timer-signal", "timer-1/fired", now)
	fired, err := store.FireTimer(context.Background(), timer.TimerID, timer.EnteredAtCounter, signal, testOutbox("timer-outbox", "run-1", signal))
	if err != nil {
		t.Fatal(err)
	}
	if fired {
		t.Fatal("旧状态实例的 Timer 必须被取消，不得写入 Signal")
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func createTestRun(t *testing.T, store *Store, now time.Time) {
	t.Helper()
	chat := domain.Chat{TenantID: "tenant-1", ChatID: "chat-1", CreatedAt: now}
	if err := store.CreateChat(context.Background(), chat); err != nil {
		t.Fatal(err)
	}
	signal := testSignal("signal-entered", "run-1/1", now)
	snapshot := domain.RunSnapshot{
		TenantID: "tenant-1", ChatID: "chat-1", RunID: "run-1", LifecycleStatus: domain.LifecycleRunnable,
		CurrentState: "start", StateVersion: 1, StateEnterCounter: 1, PendingCount: 1,
		Harness:          domain.HarnessRef{ID: "agent", Version: "1.0.0"},
		ResolvedBindings: map[string]domain.HandlerBinding{}, UpdatedAt: now,
	}
	if err := store.CreateRun(context.Background(), snapshot, signal, []ports.OutboxRecord{testOutbox("outbox-entered", "run-1", signal)}); err != nil {
		t.Fatal(err)
	}
}

func transitionFrom(load ports.ExecutionLoad, now time.Time) ports.TransitionCommit {
	snapshot := load.Snapshot
	snapshot.StateVersion++
	snapshot.PendingCount--
	snapshot.UpdatedAt = now
	return ports.TransitionCommit{
		ExpectedStateVersion: load.Snapshot.StateVersion, FencingToken: load.FencingToken,
		Snapshot: snapshot, SignalID: load.Signal.SignalID,
		Step: domain.Step{
			RunID: snapshot.RunID, StepSeq: snapshot.StateVersion, Attempt: 1, State: load.Snapshot.CurrentState,
			Handler:   domain.HandlerBinding{HandlerID: qname.MustParse("harness/test.handler"), HandlerVersion: "1.0.0", Deployment: "in-process"},
			SignalIDs: []string{load.Signal.SignalID}, StartedAt: now, FinishedAt: now,
			Outcome: string(effects.OutcomeWaiting), StateVersionBefore: load.Snapshot.StateVersion,
			StateVersionAfter: snapshot.StateVersion,
		},
	}
}

func testSignal(id, dedupe string, now time.Time) effects.StateSignal {
	return effects.StateSignal{
		SignalID: id, RunID: "run-1", Type: qname.MustParse("harness/state.entered"),
		DedupeKey: dedupe, OccurredAt: now,
	}
}

func testOutbox(id, runID string, signal effects.StateSignal) ports.OutboxRecord {
	payload, _ := json.Marshal(map[string]string{"runId": runID, "dedupeKey": signal.DedupeKey})
	return ports.OutboxRecord{ID: id, Channel: "state", Key: runID, Payload: payload, CreatedAt: signal.OccurredAt}
}
