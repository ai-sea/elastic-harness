package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

var ErrUncertainEffect = errors.New("Effect 外部结果不确定")

type Dispatcher struct {
	store      ports.StateStore
	executor   ports.EffectExecutor
	dispatcher string
	now        func() time.Time
	retryAfter time.Duration
}

func New(store ports.StateStore, executor ports.EffectExecutor, dispatcherID string) *Dispatcher {
	return &Dispatcher{
		store: store, executor: executor, dispatcher: dispatcherID,
		now: func() time.Time { return time.Now().UTC() }, retryAfter: 30 * time.Second,
	}
}

// DispatchOnce 必须先取得 Ledger CAS 派发权，胜出者才可调用外部执行器。
func (d *Dispatcher) DispatchOnce(ctx context.Context, limit int) (int, error) {
	entries, err := d.store.PendingEffects(ctx, limit)
	if err != nil {
		return 0, err
	}
	dispatched := 0
	for _, entry := range entries {
		claimed, err := d.store.ClaimEffect(ctx, entry.EffectID, entry.LedgerVersion, d.dispatcher)
		if errors.Is(err, ports.ErrConflict) {
			continue
		}
		if err != nil {
			return dispatched, err
		}
		externalRef, resultRef, executeErr := d.executor.Execute(ctx, claimed)
		if executeErr == nil {
			if err := d.commitCompleted(ctx, claimed, externalRef, resultRef); err != nil {
				return dispatched, err
			}
			dispatched++
			continue
		}
		if claimed.SideEffect == domain.SideEffectNonIdempotent {
			recovered, resolveErr := d.resolveUncertainNonIdempotent(ctx, claimed)
			if resolveErr != nil {
				// ErrUncertainEffect 必须作为被包裹哨兵返回，否则 §11.3 的
				// 「不确定结果 → 人工处置」分类在调用链上丢失，退化为普通瞬时错误。
				return dispatched, fmt.Errorf("%w：%v", resolveErr, executeErr)
			}
			if recovered {
				dispatched++
			}
			continue
		}
		return dispatched, executeErr
	}
	return dispatched, nil
}

// RecoverOnce 接管超过派发期限的 Effect。接管本身仍以 effectId + ledgerVersion
// 做 CAS；幂等调用沿用原幂等键重投，非幂等调用只能查询恢复或转人工处置。
func (d *Dispatcher) RecoverOnce(ctx context.Context, limit int) (int, error) {
	now := d.now()
	entries, err := d.store.DispatchedEffects(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, entry := range entries {
		claimed, err := d.store.ReclaimEffect(ctx, entry.EffectID, entry.LedgerVersion, d.dispatcher, now.Add(d.retryAfter))
		if errors.Is(err, ports.ErrConflict) {
			continue
		}
		if err != nil {
			return recovered, err
		}
		externalRef, resultRef, completed, recoverErr := d.executor.Recover(ctx, claimed)
		if recoverErr != nil {
			return recovered, recoverErr
		}
		if completed {
			if err := d.commitCompleted(ctx, claimed, externalRef, resultRef); err != nil {
				return recovered, err
			}
			recovered++
			continue
		}
		if claimed.SideEffect == domain.SideEffectNonIdempotent {
			if err := d.store.MarkEffectManual(ctx, claimed.EffectID, claimed.LedgerVersion); err != nil {
				return recovered, err
			}
			continue
		}
		externalRef, resultRef, executeErr := d.executor.Execute(ctx, claimed)
		if executeErr != nil {
			return recovered, executeErr
		}
		if err := d.commitCompleted(ctx, claimed, externalRef, resultRef); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}

// resolveUncertainNonIdempotent 返回 recovered=true 表示 recover 确认结果已提交。
func (d *Dispatcher) resolveUncertainNonIdempotent(ctx context.Context, entry effects.EffectLedgerEntry) (bool, error) {
	externalRef, resultRef, completed, err := d.executor.Recover(ctx, entry)
	if err != nil {
		return false, err
	}
	if !completed {
		if err := d.store.MarkEffectManual(ctx, entry.EffectID, entry.LedgerVersion); err != nil {
			return false, err
		}
		return false, ErrUncertainEffect
	}
	if err := d.commitCompleted(ctx, entry, externalRef, resultRef); err != nil {
		return false, err
	}
	return true, nil
}

func (d *Dispatcher) commitCompleted(ctx context.Context, entry effects.EffectLedgerEntry, externalRef, resultRef string) error {
	now := d.now()
	payload, _ := json.Marshal(map[string]string{"effectId": entry.EffectID, "resultRef": resultRef})
	signal := effects.StateSignal{
		SignalID: "sig_" + entry.EffectID, RunID: entry.RunID,
		Type: qname.MustParse("harness/effect.completed"), DedupeKey: entry.EffectID + "/completed",
		Payload: payload, OccurredAt: now,
	}
	wakeup, _ := json.Marshal(map[string]string{"runId": entry.RunID, "dedupeKey": signal.DedupeKey})
	return d.store.CommitEffectResult(ctx, ports.EffectResultCommit{
		EffectID: entry.EffectID, ExpectedLedgerVersion: entry.LedgerVersion,
		InvocationID: entry.InvocationID, ExpectedInvocationVersion: 2,
		ExternalRef: externalRef, ResultRef: resultRef, Signal: signal,
		Outbox: ports.OutboxRecord{
			ID: "out_" + entry.EffectID, Channel: "state", Key: entry.RunID, Payload: wakeup, CreatedAt: now,
		},
	})
}
