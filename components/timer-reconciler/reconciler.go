package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type IDGenerator interface {
	New(string) string
}

type Reconciler struct {
	store ports.StateStore
	ids   IDGenerator
	now   func() time.Time
}

func New(store ports.StateStore, ids IDGenerator, now func() time.Time) *Reconciler {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Reconciler{store: store, ids: ids, now: now}
}

func (r *Reconciler) FireDueTimers(ctx context.Context, limit int) (int, error) {
	timers, err := r.store.DueTimers(ctx, r.now(), limit)
	if err != nil {
		return 0, err
	}
	fired := 0
	for _, timer := range timers {
		now := r.now()
		signal := effects.StateSignal{
			SignalID: r.ids.New("sig_"), RunID: timer.RunID, Type: timer.SignalType,
			DedupeKey: timer.TimerID + "/fired", OccurredAt: now,
		}
		payload, _ := json.Marshal(map[string]string{"runId": timer.RunID, "dedupeKey": signal.DedupeKey})
		outbox := ports.OutboxRecord{
			ID: r.ids.New("out_"), Channel: "state", Key: timer.RunID, Payload: payload, CreatedAt: now,
		}
		accepted, err := r.store.FireTimer(ctx, timer.TimerID, timer.EnteredAtCounter, signal, outbox)
		if err != nil {
			return fired, err
		}
		if accepted {
			fired++
		}
	}
	return fired, nil
}

// RepairRunnable 只补写 entered Signal，不直接修改 Run 业务状态。
func (r *Reconciler) RepairRunnable(ctx context.Context, limit int) (int, error) {
	runs, err := r.store.RunnableWithoutSignal(ctx, limit)
	if err != nil {
		return 0, err
	}
	repaired := 0
	for _, run := range runs {
		now := r.now()
		signal := effects.StateSignal{
			SignalID: r.ids.New("sig_"), RunID: run.RunID,
			Type:      qname.MustParse("harness/state.entered"),
			DedupeKey: fmt.Sprintf("%s/%d", run.RunID, run.StateVersion), OccurredAt: now,
		}
		payload, _ := json.Marshal(map[string]string{"runId": run.RunID, "dedupeKey": signal.DedupeKey})
		outbox := ports.OutboxRecord{
			ID: r.ids.New("out_"), Channel: "state", Key: run.RunID, Payload: payload, CreatedAt: now,
		}
		inserted, err := r.store.PutSignal(ctx, signal, outbox)
		if err != nil {
			return repaired, err
		}
		if inserted {
			repaired++
		}
	}
	return repaired, nil
}

// ReawakenStalled 为「Inbox 仍有未消费 Signal、但租约已失效」的 Run 补发唤醒提示
// （§11.1：Worker 执行中崩溃、StateEventQueue 提示丢失的兜底路径）。
// 只补提示、不改业务状态；重复提示无害——消费端由 Inbox 去重与 CAS 裁决。
func (r *Reconciler) ReawakenStalled(ctx context.Context, limit int) (int, error) {
	runs, err := r.store.StalledRuns(ctx, r.now(), limit)
	if err != nil {
		return 0, err
	}
	reawakened := 0
	for _, run := range runs {
		payload, _ := json.Marshal(map[string]string{"runId": run.RunID})
		hint := ports.OutboxRecord{
			ID: r.ids.New("out_"), Channel: "state", Key: run.RunID, Payload: payload, CreatedAt: r.now(),
		}
		if err := r.store.EnqueueHint(ctx, hint); err != nil {
			return reawakened, err
		}
		reawakened++
	}
	return reawakened, nil
}
