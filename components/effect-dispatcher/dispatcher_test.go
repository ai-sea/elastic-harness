package dispatcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

// 不变量 #4 + #11：同一 effectId 的副作用至多由一个执行者发起。
// 两个 Dispatcher 同时看到同一条 PENDING 时，只有取得 Ledger CAS 的一方可以调用执行器。
func TestDispatchClaimIsExclusiveAcrossConcurrentDispatchers(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectIdempotent))
	executor := &recordingExecutor{}
	first, second := New(store, executor, "dispatcher-a"), New(store, executor, "dispatcher-b")

	var wait sync.WaitGroup
	for _, dispatcher := range []*Dispatcher{first, second} {
		wait.Add(1)
		go func(d *Dispatcher) {
			defer wait.Done()
			_, _ = d.DispatchOnce(context.Background(), 10)
		}(dispatcher)
	}
	wait.Wait()

	if calls := executor.count(); calls != 1 {
		t.Fatalf("外部调用次数 = %d，期望恰好 1（至多一次）", calls)
	}
}

// 不变量 #9：改变世界的动作不得早于提交。派发顺序必须是
// 抢占派发权 → 调用外部 → 写回结果 → 投递完成 Signal。
func TestDispatchClaimsBeforeInvokingExecutor(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectIdempotent))
	executor := &recordingExecutor{trace: &store.trace, mutex: &store.mutex}
	if _, err := New(store, executor, "dispatcher-a").DispatchOnce(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if got, want := store.ledgerTrace(), []string{"claimed", "executed", "committed", "signaled"}; !equal(got, want) {
		t.Fatalf("Ledger 事件序列 = %v，期望 %v", got, want)
	}
}

func TestDispatchSkipsEntryClaimedByAnotherDispatcher(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectIdempotent))
	store.forceClaimConflict = true
	executor := &recordingExecutor{}
	dispatched, err := New(store, executor, "dispatcher-b").DispatchOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("派发权竞争落败应静默跳过，得到：%v", err)
	}
	if dispatched != 0 || executor.count() != 0 {
		t.Fatalf("落败方不得产生任何外部调用：dispatched=%d calls=%d", dispatched, executor.count())
	}
}

// §11.3「不确定结果」：非幂等 Effect 调用失败后必须先 recover 确认，
// 无法确认时标记 NEEDS_MANUAL，绝不盲目重试。
func TestNonIdempotentUncertainEffectGoesToManual(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectNonIdempotent))
	executor := &recordingExecutor{executeErr: errors.New("连接中断"), recoverCompleted: false}
	_, err := New(store, executor, "dispatcher-a").DispatchOnce(context.Background(), 10)
	if !errors.Is(err, ErrUncertainEffect) {
		t.Fatalf("期望 ErrUncertainEffect，得到：%v", err)
	}
	if !store.manual {
		t.Fatal("无法确认结果时必须标记 NEEDS_MANUAL")
	}
	if executor.count() != 1 {
		t.Fatalf("不得盲目重试：外部调用次数 = %d", executor.count())
	}
}

func TestNonIdempotentEffectRecoveredAsCompleted(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectNonIdempotent))
	executor := &recordingExecutor{executeErr: errors.New("连接中断"), recoverCompleted: true}
	dispatched, err := New(store, executor, "dispatcher-a").DispatchOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("recover 确认完成后应正常收敛：%v", err)
	}
	if dispatched != 1 || store.manual {
		t.Fatalf("dispatched=%d manual=%v，期望 1/false", dispatched, store.manual)
	}
}

// 不变量 #10：终态 Run 不接受新 Signal——派发完成信号被拒不算失败。
func TestCompletionSignalIsDroppedForTerminalRun(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectIdempotent))
	store.signalErr = ports.ErrTerminal
	dispatched, err := New(store, &recordingExecutor{}, "dispatcher-a").DispatchOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("终态 Run 拒绝完成 Signal 不应报错：%v", err)
	}
	if dispatched != 1 {
		t.Fatalf("Effect 本身已派发完成，dispatched = %d，期望 1", dispatched)
	}
}

// 幂等 Effect 的瞬时失败按 §11.3 走重试，不由 Dispatcher 吞掉。
func TestIdempotentEffectFailureSurfacesForRetry(t *testing.T) {
	store := newFakeStore(pendingEntry("effect-1", domain.SideEffectIdempotent))
	executor := &recordingExecutor{executeErr: errors.New("provider 5xx")}
	if _, err := New(store, executor, "dispatcher-a").DispatchOnce(context.Background(), 10); err == nil {
		t.Fatal("幂等 Effect 的瞬时失败必须上报以触发重试")
	}
	if store.manual {
		t.Fatal("幂等 Effect 不应进入人工处理")
	}
}

func pendingEntry(effectID string, sideEffect domain.SideEffect) effects.EffectLedgerEntry {
	return effects.EffectLedgerEntry{
		EffectID: effectID, TenantID: "tenant-1", RunID: "run-1",
		Kind: qname.MustParse("harness/tool.invoke"), Status: effects.EffectPending, LedgerVersion: 1,
		IdempotencyKey: "run-1/" + effectID, SideEffect: sideEffect,
		Deadline: time.Now().UTC().Add(time.Minute), CreatedAt: time.Now().UTC(),
	}
}

type recordingExecutor struct {
	calls            int64
	executeErr       error
	recoverCompleted bool
	// trace/mutex 与 fakeStore 共享，用于验证"先抢权、后调用"的先后次序。
	trace *[]string
	mutex *sync.Mutex
}

func (e *recordingExecutor) Execute(context.Context, effects.EffectLedgerEntry) (string, string, error) {
	atomic.AddInt64(&e.calls, 1)
	if e.trace != nil {
		e.mutex.Lock()
		*e.trace = append(*e.trace, "executed")
		e.mutex.Unlock()
	}
	if e.executeErr != nil {
		return "", "", e.executeErr
	}
	return "external-1", "artifact://result", nil
}

func (e *recordingExecutor) Recover(context.Context, effects.EffectLedgerEntry) (string, string, bool, error) {
	return "external-1", "artifact://result", e.recoverCompleted, nil
}

func (e *recordingExecutor) count() int { return int(atomic.LoadInt64(&e.calls)) }

// fakeStore 只实现 Effect Ledger 与 Signal 相关方法，其余返回零值——
// 本组测试的验证目标是派发语义，不是状态存储本身（后者由 kv-sqlite 的 Port 契约测试覆盖）。
type fakeStore struct {
	mutex              sync.Mutex
	entries            map[string]effects.EffectLedgerEntry
	trace              []string
	manual             bool
	signalErr          error
	forceClaimConflict bool
}

func newFakeStore(entries ...effects.EffectLedgerEntry) *fakeStore {
	store := &fakeStore{entries: map[string]effects.EffectLedgerEntry{}}
	for _, entry := range entries {
		store.entries[entry.EffectID] = entry
	}
	return store
}

func (s *fakeStore) PendingEffects(context.Context, int) ([]effects.EffectLedgerEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	pending := make([]effects.EffectLedgerEntry, 0)
	for _, entry := range s.entries {
		if entry.Status == effects.EffectPending {
			pending = append(pending, entry)
		}
	}
	return pending, nil
}

func (s *fakeStore) ClaimEffect(_ context.Context, effectID string, ledgerVersion int64, dispatcher string) (effects.EffectLedgerEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.forceClaimConflict {
		return effects.EffectLedgerEntry{}, ports.ErrConflict
	}
	entry, exists := s.entries[effectID]
	// 派发权判据是 effectId + ledgerVersion CAS，与 Run 的 stateVersion 相互独立（不变量 #11）。
	if !exists || entry.LedgerVersion != ledgerVersion || entry.Status != effects.EffectPending {
		return effects.EffectLedgerEntry{}, ports.ErrConflict
	}
	entry.Status, entry.LedgerVersion, entry.DispatcherRef = effects.EffectDispatched, ledgerVersion+1, dispatcher
	s.entries[effectID] = entry
	s.trace = append(s.trace, "claimed")
	return entry, nil
}

func (s *fakeStore) CommitEffectResult(_ context.Context, commit ports.EffectResultCommit) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	entry, exists := s.entries[commit.EffectID]
	if !exists || entry.LedgerVersion != commit.ExpectedLedgerVersion {
		return ports.ErrConflict
	}
	entry.Status, entry.LedgerVersion = effects.EffectCommitted, commit.ExpectedLedgerVersion+1
	entry.ExternalRef, entry.ResultRef = commit.ExternalRef, commit.ResultRef
	s.entries[commit.EffectID] = entry
	s.trace = append(s.trace, "committed")
	if !errors.Is(s.signalErr, ports.ErrTerminal) {
		if s.signalErr != nil {
			return s.signalErr
		}
		s.trace = append(s.trace, "signaled")
	}
	return nil
}

func (s *fakeStore) MarkEffectManual(_ context.Context, effectID string, ledgerVersion int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	entry, exists := s.entries[effectID]
	if !exists || entry.LedgerVersion != ledgerVersion {
		return ports.ErrConflict
	}
	entry.Status, entry.LedgerVersion = effects.EffectManual, ledgerVersion+1
	s.entries[effectID] = entry
	s.manual = true
	return nil
}

func (s *fakeStore) PutSignal(context.Context, effects.StateSignal, ports.OutboxRecord) (bool, error) {
	if s.signalErr != nil {
		return false, s.signalErr
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.trace = append(s.trace, "signaled")
	return true, nil
}

func (s *fakeStore) ledgerTrace() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]string(nil), s.trace...)
}

func (s *fakeStore) CreateChat(context.Context, domain.Chat) error       { return nil }
func (s *fakeStore) AppendMessage(context.Context, domain.Message) error { return nil }
func (s *fakeStore) CreateRun(context.Context, domain.RunSnapshot, effects.StateSignal, []ports.OutboxRecord) error {
	return nil
}
func (s *fakeStore) GetRun(context.Context, string) (domain.RunSnapshot, error) {
	return domain.RunSnapshot{}, nil
}
func (s *fakeStore) AcquireExecution(context.Context, string, string, time.Duration) (ports.ExecutionLoad, error) {
	return ports.ExecutionLoad{}, nil
}
func (s *fakeStore) CommitTransition(context.Context, ports.TransitionCommit) error { return nil }
func (s *fakeStore) ReleaseLease(context.Context, string, int64) error              { return nil }
func (s *fakeStore) Steps(context.Context, string) ([]domain.Step, error)           { return nil, nil }
func (s *fakeStore) Events(context.Context, string, int64, int) ([]domain.EventEnvelope, error) {
	return nil, nil
}
func (s *fakeStore) Inbox(context.Context, string) ([]effects.StateSignal, error) { return nil, nil }
func (s *fakeStore) Effects(context.Context, string) ([]effects.EffectLedgerEntry, error) {
	return nil, nil
}
func (s *fakeStore) Invocations(context.Context, string) ([]effects.ToolInvocation, error) {
	return nil, nil
}
func (s *fakeStore) DueTimers(context.Context, time.Time, int) ([]effects.Timer, error) {
	return nil, nil
}
func (s *fakeStore) FireTimer(context.Context, string, int64, effects.StateSignal, ports.OutboxRecord) (bool, error) {
	return false, nil
}
func (s *fakeStore) PendingOutbox(context.Context, int) ([]ports.OutboxRecord, error) {
	return nil, nil
}
func (s *fakeStore) MarkOutboxPublished(context.Context, string) error { return nil }
func (s *fakeStore) RunnableWithoutSignal(context.Context, int) ([]domain.RunSnapshot, error) {
	return nil, nil
}
func (s *fakeStore) StalledRuns(context.Context, time.Time, int) ([]domain.RunSnapshot, error) {
	return nil, nil
}
func (s *fakeStore) EnqueueHint(context.Context, ports.OutboxRecord) error { return nil }

func equal(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
