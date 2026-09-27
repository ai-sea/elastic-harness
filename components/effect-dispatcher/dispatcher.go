package dispatcher

import (
	"context"
	"errors"
	"fmt"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
)

var ErrUncertainEffect = errors.New("Effect 外部结果不确定")

type Dispatcher struct {
	store      ports.StateStore
	executor   ports.EffectExecutor
	dispatcher string
}

func New(store ports.StateStore, executor ports.EffectExecutor, dispatcherID string) *Dispatcher {
	return &Dispatcher{store: store, executor: executor, dispatcher: dispatcherID}
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
			if err := d.store.CommitEffect(ctx, claimed.EffectID, claimed.LedgerVersion, externalRef, resultRef); err != nil {
				return dispatched, err
			}
			dispatched++
			continue
		}
		if claimed.SideEffect == domain.SideEffectNonIdempotent {
			if err := d.resolveUncertainNonIdempotent(ctx, claimed); err != nil {
				return dispatched, fmt.Errorf("%w：%v", executeErr, err)
			}
			continue
		}
		return dispatched, executeErr
	}
	return dispatched, nil
}

func (d *Dispatcher) resolveUncertainNonIdempotent(ctx context.Context, entry effects.EffectLedgerEntry) error {
	externalRef, resultRef, completed, err := d.executor.Recover(ctx, entry)
	if err != nil {
		return err
	}
	if completed {
		return d.store.CommitEffect(ctx, entry.EffectID, entry.LedgerVersion, externalRef, resultRef)
	}
	if err := d.store.MarkEffectManual(ctx, entry.EffectID, entry.LedgerVersion); err != nil {
		return err
	}
	return ErrUncertainEffect
}
