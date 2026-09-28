package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

// §8.7：Timer 有效性判据是 stateEnterCounter，不是 stateVersion——
// 同状态内迁移会推进 stateVersion，误用它会杀死仍有效的 Timer。
func TestFireDueTimersUsesEnteredAtCounterAsValidityKey(t *testing.T) {
	store := &fakeStore{timers: []effects.Timer{{
		TimerID: "timer-1", RunID: "run-1", Status: effects.TimerScheduled,
		SignalType: qname.MustParse("harness/timer.fired"),
		// 同状态内已发生 4 次迁移（stateVersion=6），但状态实例只进入过 2 次。
		StateVersionAtSchedule: 2, EnteredAtCounter: 2,
	}}}
	reconciler := New(store, &sequentialIDs{}, nil)
	fired, err := reconciler.FireDueTimers(context.Background(), 10)
	if err != nil || fired != 1 {
		t.Fatalf("FireDueTimers = %d/%v，期望 1/nil", fired, err)
	}
	if store.firedCounter["timer-1"] != 2 {
		t.Fatalf("判据 = %d，必须使用 EnteredAtCounter=2 而非 StateVersionAtSchedule", store.firedCounter["timer-1"])
	}
}

func TestRepairRunnableReissuesEnteredSignal(t *testing.T) {
	store := &fakeStore{runnable: []domain.RunSnapshot{{RunID: "run-1", StateVersion: 3}}}
	reconciler := New(store, &sequentialIDs{}, nil)
	repaired, err := reconciler.RepairRunnable(context.Background(), 10)
	if err != nil || repaired != 1 {
		t.Fatalf("RepairRunnable = %d/%v，期望 1/nil", repaired, err)
	}
	if len(store.signals) != 1 {
		t.Fatalf("应补写 1 条 entered Signal：%+v", store.signals)
	}
	signal := store.signals[0]
	if !signal.Type.Equal(qname.MustParse("harness/state.entered")) || signal.DedupeKey != "run-1/3" {
		t.Fatalf("补写 Signal 错误：%+v", signal)
	}
	// 不变量 #13 的修复必须与提示一起原子落账。
	if len(store.signalOutbox) != 1 || store.signalOutbox[0].Channel != "state" {
		t.Fatalf("补写必须携带 state 通道 Outbox 提示：%+v", store.signalOutbox)
	}
}

// §11.1：Worker 崩溃后租约到期、提示已丢，Reconciler 只补发提示，不改业务状态。
func TestReawakenStalledPublishesHintWithoutTouchingState(t *testing.T) {
	store := &fakeStore{stalled: []domain.RunSnapshot{{RunID: "run-1"}, {RunID: "run-2"}}}
	reconciler := New(store, &sequentialIDs{}, nil)
	reawakened, err := reconciler.ReawakenStalled(context.Background(), 10)
	if err != nil || reawakened != 2 {
		t.Fatalf("ReawakenStalled = %d/%v，期望 2/nil", reawakened, err)
	}
	if len(store.hints) != 2 {
		t.Fatalf("应为每个停滞 Run 补发 1 条提示：%+v", store.hints)
	}
	for index, runID := range []string{"run-1", "run-2"} {
		if store.hints[index].Channel != "state" || store.hints[index].Key != runID {
			t.Fatalf("提示必须走 state 通道并以 runId 为键：%+v", store.hints[index])
		}
	}
	if len(store.signals) != 0 || store.commits != 0 {
		t.Fatal("重唤醒不得写 Inbox 或业务状态——最终迁移仍由 onState 经 CAS 提交")
	}
}

type sequentialIDs struct{ next int }

func (s *sequentialIDs) New(prefix string) string {
	s.next++
	return prefix + string(rune('0'+s.next))
}

// fakeStore 只实现 Reconciler 触及的方法有行为，其余返回零值——
// 存储语义本身由 kv-sqlite 的 Port 契约测试覆盖，这里验证的是 Reconciler 的编排。
type fakeStore struct {
	timers       []effects.Timer
	firedCounter map[string]int64
	runnable     []domain.RunSnapshot
	stalled      []domain.RunSnapshot
	signals      []effects.StateSignal
	signalOutbox []ports.OutboxRecord
	hints        []ports.OutboxRecord
	commits      int
}

func (s *fakeStore) DueTimers(_ context.Context, _ time.Time, limit int) ([]effects.Timer, error) {
	if len(s.timers) > limit {
		return s.timers[:limit], nil
	}
	return s.timers, nil
}

func (s *fakeStore) FireTimer(_ context.Context, timerID string, enteredAtCounter int64, _ effects.StateSignal, _ ports.OutboxRecord) (bool, error) {
	if s.firedCounter == nil {
		s.firedCounter = map[string]int64{}
	}
	s.firedCounter[timerID] = enteredAtCounter
	return true, nil
}

func (s *fakeStore) RunnableWithoutSignal(context.Context, int) ([]domain.RunSnapshot, error) {
	return s.runnable, nil
}

func (s *fakeStore) PutSignal(_ context.Context, signal effects.StateSignal, outbox ports.OutboxRecord) (bool, error) {
	s.signals = append(s.signals, signal)
	s.signalOutbox = append(s.signalOutbox, outbox)
	return true, nil
}

func (s *fakeStore) StalledRuns(context.Context, time.Time, int) ([]domain.RunSnapshot, error) {
	return s.stalled, nil
}

func (s *fakeStore) EnqueueHint(_ context.Context, record ports.OutboxRecord) error {
	s.hints = append(s.hints, record)
	return nil
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
func (s *fakeStore) CommitTransition(context.Context, ports.TransitionCommit) error {
	s.commits++
	return nil
}
func (s *fakeStore) ReleaseLease(context.Context, string, int64) error    { return nil }
func (s *fakeStore) Steps(context.Context, string) ([]domain.Step, error) { return nil, nil }
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
func (s *fakeStore) PendingEffects(context.Context, int) ([]effects.EffectLedgerEntry, error) {
	return nil, nil
}
func (s *fakeStore) DispatchedEffects(context.Context, time.Time, int) ([]effects.EffectLedgerEntry, error) {
	return nil, nil
}
func (s *fakeStore) ClaimEffect(context.Context, string, int64, string) (effects.EffectLedgerEntry, error) {
	return effects.EffectLedgerEntry{}, nil
}
func (s *fakeStore) ReclaimEffect(context.Context, string, int64, string, time.Time) (effects.EffectLedgerEntry, error) {
	return effects.EffectLedgerEntry{}, nil
}
func (s *fakeStore) CommitEffectResult(context.Context, ports.EffectResultCommit) error { return nil }
func (s *fakeStore) MarkEffectManual(context.Context, string, int64) error              { return nil }
func (s *fakeStore) PendingOutbox(context.Context, int) ([]ports.OutboxRecord, error) {
	return nil, nil
}
func (s *fakeStore) MarkOutboxPublished(context.Context, string) error { return nil }
