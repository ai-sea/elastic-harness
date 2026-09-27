package kvsqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	_ "modernc.org/sqlite"
)

// Store 使用 SQLite WAL 实现 standalone 的权威状态与多键原子提交。
type Store struct {
	database *sql.DB
	now      func() time.Time
}

func Open(path string) (*Store, error) {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	dataSource := path + separator + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	database, err := sql.Open("sqlite", dataSource)
	if err != nil {
		return nil, fmt.Errorf("打开 SQLite：%w", err)
	}
	database.SetMaxOpenConns(1)
	store := &Store{database: database, now: func() time.Time { return time.Now().UTC() }}
	if err := store.migrate(context.Background()); err != nil {
		database.Close()
		return nil, err
	}
	return store, nil
}

func OpenMemory() (*Store, error) {
	return Open("file:elastic-harness?mode=memory&cache=shared")
}

func (s *Store) Close() error { return s.database.Close() }

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS chats (
  chat_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, body BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY, chat_id TEXT NOT NULL, body BLOB NOT NULL,
  FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
);
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, chat_id TEXT NOT NULL,
  state_version INTEGER NOT NULL, lifecycle TEXT NOT NULL, current_state TEXT NOT NULL,
  state_enter_counter INTEGER NOT NULL, body BLOB NOT NULL,
  lease_owner TEXT, lease_token INTEGER NOT NULL DEFAULT 0, lease_expires_at TEXT
);
CREATE TABLE IF NOT EXISTS inbox (
  signal_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, dedupe_key TEXT NOT NULL,
  priority INTEGER NOT NULL, occurred_at TEXT NOT NULL, consumed_step INTEGER, body BLOB NOT NULL,
  UNIQUE(run_id, dedupe_key), FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS inbox_pending ON inbox(run_id, consumed_step, priority DESC, occurred_at);
CREATE TABLE IF NOT EXISTS steps (
  run_id TEXT NOT NULL, step_seq INTEGER NOT NULL, attempt INTEGER NOT NULL, body BLOB NOT NULL,
  PRIMARY KEY(run_id, step_seq, attempt)
);
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, sequence INTEGER NOT NULL, body BLOB NOT NULL,
  UNIQUE(run_id, sequence)
);
CREATE TABLE IF NOT EXISTS effects (
  effect_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL,
  ledger_version INTEGER NOT NULL, deadline TEXT NOT NULL, body BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS effects_pending ON effects(status, deadline);
CREATE TABLE IF NOT EXISTS timers (
  timer_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL,
  due_at TEXT NOT NULL, entered_at_counter INTEGER NOT NULL, body BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS timers_due ON timers(status, due_at);
CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY, channel TEXT NOT NULL, key_value TEXT NOT NULL,
  payload BLOB NOT NULL, created_at TEXT NOT NULL, published_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(published_at, created_at);
`
	if _, err := s.database.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("迁移 SQLite schema：%w", err)
	}
	return nil
}

func (s *Store) CreateChat(ctx context.Context, chat domain.Chat) error {
	body, err := marshal(chat)
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, `INSERT INTO chats(chat_id, tenant_id, body) VALUES(?,?,?)`, chat.ChatID, chat.TenantID, body)
	return mapWriteError(err)
}

func (s *Store) AppendMessage(ctx context.Context, message domain.Message) error {
	body, err := marshal(message)
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, `INSERT INTO messages(message_id, chat_id, body) VALUES(?,?,?)`, message.MessageID, message.ChatID, body)
	return mapWriteError(err)
}

func (s *Store) CreateRun(ctx context.Context, snapshot domain.RunSnapshot, signal effects.StateSignal, outbox []ports.OutboxRecord) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	return s.inTransaction(ctx, func(tx *sql.Tx) error {
		body, err := marshal(snapshot)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO runs(
run_id,tenant_id,chat_id,state_version,lifecycle,current_state,state_enter_counter,body
) VALUES(?,?,?,?,?,?,?,?)`, snapshot.RunID, snapshot.TenantID, snapshot.ChatID, snapshot.StateVersion,
			snapshot.LifecycleStatus, snapshot.CurrentState, snapshot.StateEnterCounter, body); err != nil {
			return mapWriteError(err)
		}
		if err := insertSignal(ctx, tx, signal); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, outbox)
	})
}

func (s *Store) GetRun(ctx context.Context, runID string) (domain.RunSnapshot, error) {
	var body []byte
	if err := s.database.QueryRowContext(ctx, `SELECT body FROM runs WHERE run_id=?`, runID).Scan(&body); err != nil {
		return domain.RunSnapshot{}, mapReadError(err)
	}
	return unmarshal[domain.RunSnapshot](body)
}

func (s *Store) PutSignal(ctx context.Context, signal effects.StateSignal, outbox ports.OutboxRecord) (bool, error) {
	inserted := false
	err := s.inTransaction(ctx, func(tx *sql.Tx) error {
		var lifecycle string
		if err := tx.QueryRowContext(ctx, `SELECT lifecycle FROM runs WHERE run_id=?`, signal.RunID).Scan(&lifecycle); err != nil {
			return mapReadError(err)
		}
		if domain.LifecycleStatus(lifecycle) == domain.LifecycleTerminal {
			return ports.ErrTerminal
		}
		result, err := insertSignalIgnoreDuplicate(ctx, tx, signal)
		if err != nil {
			return err
		}
		inserted = result
		if inserted {
			if err := adjustPendingCount(ctx, tx, signal.RunID, 1, signal.OccurredAt); err != nil {
				return err
			}
			return insertOutbox(ctx, tx, []ports.OutboxRecord{outbox})
		}
		return nil
	})
	return inserted, err
}

func (s *Store) AcquireExecution(ctx context.Context, runID, workerID string, duration time.Duration) (ports.ExecutionLoad, error) {
	var load ports.ExecutionLoad
	err := s.inTransaction(ctx, func(tx *sql.Tx) error {
		now := s.now()
		expiresAt := now.Add(duration)
		result, err := tx.ExecContext(ctx, `UPDATE runs SET lease_owner=?, lease_token=lease_token+1, lease_expires_at=?
WHERE run_id=? AND lifecycle<>? AND (lease_expires_at IS NULL OR lease_expires_at<=? OR lease_owner=?)`,
			workerID, formatTime(expiresAt), runID, domain.LifecycleTerminal, formatTime(now), workerID)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return ports.ErrConflict
		}
		var snapshotBody []byte
		if err := tx.QueryRowContext(ctx, `SELECT body, lease_token FROM runs WHERE run_id=?`, runID).Scan(&snapshotBody, &load.FencingToken); err != nil {
			return mapReadError(err)
		}
		load.Snapshot, err = unmarshal[domain.RunSnapshot](snapshotBody)
		if err != nil {
			return err
		}
		var signalBody []byte
		if err := tx.QueryRowContext(ctx, `SELECT body FROM inbox WHERE run_id=? AND consumed_step IS NULL
ORDER BY priority DESC, occurred_at ASC LIMIT 1`, runID).Scan(&signalBody); err != nil {
			return mapReadError(err)
		}
		load.Signal, err = unmarshal[effects.StateSignal](signalBody)
		return err
	})
	return load, err
}

func (s *Store) CommitTransition(ctx context.Context, commit ports.TransitionCommit) error {
	if err := commit.Snapshot.Validate(); err != nil {
		return err
	}
	return s.inTransaction(ctx, func(tx *sql.Tx) error {
		body, err := marshal(commit.Snapshot)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE runs SET state_version=?, lifecycle=?, current_state=?,
state_enter_counter=?, body=? WHERE run_id=? AND state_version=? AND lease_token=?`,
			commit.Snapshot.StateVersion, commit.Snapshot.LifecycleStatus, commit.Snapshot.CurrentState,
			commit.Snapshot.StateEnterCounter, body, commit.Snapshot.RunID,
			commit.ExpectedStateVersion, commit.FencingToken)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return ports.ErrConflict
		}
		result, err = tx.ExecContext(ctx, `UPDATE inbox SET consumed_step=?
WHERE signal_id=? AND run_id=? AND consumed_step IS NULL`, commit.Step.StepSeq, commit.SignalID, commit.Snapshot.RunID)
		if err != nil {
			return err
		}
		count, _ = result.RowsAffected()
		if count != 1 {
			return ports.ErrConflict
		}
		if err := insertJSON(ctx, tx, `INSERT INTO steps(run_id,step_seq,attempt,body) VALUES(?,?,?,?)`,
			commit.Step, commit.Step.RunID, commit.Step.StepSeq, commit.Step.Attempt); err != nil {
			return err
		}
		for _, effect := range commit.Effects {
			if err := insertEffect(ctx, tx, effect); err != nil {
				return err
			}
		}
		for _, timer := range commit.Timers {
			if err := insertTimer(ctx, tx, timer); err != nil {
				return err
			}
		}
		for _, event := range commit.Events {
			if err := insertEvent(ctx, tx, commit.Snapshot.RunID, event); err != nil {
				return err
			}
		}
		if commit.Continuation != nil {
			if err := insertSignal(ctx, tx, *commit.Continuation); err != nil {
				return err
			}
		}
		return insertOutbox(ctx, tx, commit.Outbox)
	})
}

func (s *Store) ReleaseLease(ctx context.Context, runID string, token int64) error {
	_, err := s.database.ExecContext(ctx, `UPDATE runs SET lease_owner=NULL, lease_expires_at=NULL
WHERE run_id=? AND lease_token=?`, runID, token)
	return err
}

func (s *Store) Steps(ctx context.Context, runID string) ([]domain.Step, error) {
	return queryJSON[domain.Step](ctx, s.database, `SELECT body FROM steps WHERE run_id=? ORDER BY step_seq, attempt`, runID)
}

func (s *Store) Events(ctx context.Context, runID string, after int64, limit int) ([]domain.EventEnvelope, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	return queryJSON[domain.EventEnvelope](ctx, s.database,
		`SELECT body FROM events WHERE run_id=? AND sequence>? ORDER BY sequence LIMIT ?`, runID, after, limit)
}

func (s *Store) Inbox(ctx context.Context, runID string) ([]effects.StateSignal, error) {
	return queryJSON[effects.StateSignal](ctx, s.database,
		`SELECT body FROM inbox WHERE run_id=? ORDER BY occurred_at`, runID)
}

func (s *Store) Effects(ctx context.Context, runID string) ([]effects.EffectLedgerEntry, error) {
	return queryJSON[effects.EffectLedgerEntry](ctx, s.database,
		`SELECT body FROM effects WHERE run_id=? ORDER BY effect_id`, runID)
}

func (s *Store) PendingEffects(ctx context.Context, limit int) ([]effects.EffectLedgerEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	return queryJSON[effects.EffectLedgerEntry](ctx, s.database,
		`SELECT body FROM effects WHERE status=? ORDER BY deadline LIMIT ?`, effects.EffectPending, limit)
}

func (s *Store) ClaimEffect(ctx context.Context, effectID string, ledgerVersion int64, dispatcher string) (effects.EffectLedgerEntry, error) {
	var claimed effects.EffectLedgerEntry
	err := s.inTransaction(ctx, func(tx *sql.Tx) error {
		var body []byte
		if err := tx.QueryRowContext(ctx, `SELECT body FROM effects WHERE effect_id=?`, effectID).Scan(&body); err != nil {
			return mapReadError(err)
		}
		entry, err := unmarshal[effects.EffectLedgerEntry](body)
		if err != nil {
			return err
		}
		if entry.Status != effects.EffectPending || entry.LedgerVersion != ledgerVersion {
			return ports.ErrConflict
		}
		entry.Status = effects.EffectDispatched
		entry.LedgerVersion++
		entry.DispatcherRef = dispatcher
		updatedBody, err := marshal(entry)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE effects SET status=?,ledger_version=?,body=?
WHERE effect_id=? AND status=? AND ledger_version=?`, entry.Status, entry.LedgerVersion, updatedBody,
			effectID, effects.EffectPending, ledgerVersion)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return ports.ErrConflict
		}
		claimed = entry
		return nil
	})
	return claimed, err
}

func (s *Store) CommitEffect(ctx context.Context, effectID string, ledgerVersion int64, externalRef, resultRef string) error {
	return s.updateEffect(ctx, effectID, ledgerVersion, func(entry *effects.EffectLedgerEntry) error {
		if entry.Status != effects.EffectDispatched {
			return ports.ErrConflict
		}
		now := s.now()
		entry.Status = effects.EffectCommitted
		entry.ExternalRef = externalRef
		entry.ResultRef = resultRef
		entry.ResolvedAt = &now
		return nil
	})
}

func (s *Store) MarkEffectManual(ctx context.Context, effectID string, ledgerVersion int64) error {
	return s.updateEffect(ctx, effectID, ledgerVersion, func(entry *effects.EffectLedgerEntry) error {
		if entry.Status != effects.EffectDispatched {
			return ports.ErrConflict
		}
		entry.Status = effects.EffectManual
		return nil
	})
}

func (s *Store) updateEffect(ctx context.Context, effectID string, ledgerVersion int64, mutate func(*effects.EffectLedgerEntry) error) error {
	return s.inTransaction(ctx, func(tx *sql.Tx) error {
		var body []byte
		if err := tx.QueryRowContext(ctx, `SELECT body FROM effects WHERE effect_id=?`, effectID).Scan(&body); err != nil {
			return mapReadError(err)
		}
		entry, err := unmarshal[effects.EffectLedgerEntry](body)
		if err != nil {
			return err
		}
		if entry.LedgerVersion != ledgerVersion {
			return ports.ErrConflict
		}
		if err := mutate(&entry); err != nil {
			return err
		}
		entry.LedgerVersion++
		updatedBody, err := marshal(entry)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE effects SET status=?,ledger_version=?,body=?
WHERE effect_id=? AND ledger_version=?`, entry.Status, entry.LedgerVersion, updatedBody, effectID, ledgerVersion)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return ports.ErrConflict
		}
		return nil
	})
}

func (s *Store) DueTimers(ctx context.Context, now time.Time, limit int) ([]effects.Timer, error) {
	if limit <= 0 {
		limit = 100
	}
	return queryJSON[effects.Timer](ctx, s.database,
		`SELECT body FROM timers WHERE status=? AND due_at<=? ORDER BY due_at LIMIT ?`,
		effects.TimerScheduled, formatTime(now), limit)
}

func (s *Store) FireTimer(ctx context.Context, timerID string, enteredAtCounter int64, signal effects.StateSignal, outbox ports.OutboxRecord) (bool, error) {
	fired := false
	err := s.inTransaction(ctx, func(tx *sql.Tx) error {
		var timerBody []byte
		if err := tx.QueryRowContext(ctx, `SELECT body FROM timers WHERE timer_id=?`, timerID).Scan(&timerBody); err != nil {
			return mapReadError(err)
		}
		timer, err := unmarshal[effects.Timer](timerBody)
		if err != nil {
			return err
		}
		if timer.Status != effects.TimerScheduled {
			return ports.ErrConflict
		}
		var currentCounter int64
		if err := tx.QueryRowContext(ctx, `SELECT state_enter_counter FROM runs WHERE run_id=?`, timer.RunID).Scan(&currentCounter); err != nil {
			return mapReadError(err)
		}
		if currentCounter != enteredAtCounter {
			timer.Status = effects.TimerCancelled
		} else {
			timer.Status = effects.TimerFired
			if err := insertSignal(ctx, tx, signal); err != nil {
				return err
			}
			if err := adjustPendingCount(ctx, tx, signal.RunID, 1, signal.OccurredAt); err != nil {
				return err
			}
			if err := insertOutbox(ctx, tx, []ports.OutboxRecord{outbox}); err != nil {
				return err
			}
			fired = true
		}
		updatedBody, err := marshal(timer)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE timers SET status=?,body=? WHERE timer_id=? AND status=?`,
			timer.Status, updatedBody, timerID, effects.TimerScheduled)
		return err
	})
	return fired, err
}

func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]ports.OutboxRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.database.QueryContext(ctx, `SELECT id,channel,key_value,payload,created_at
FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ports.OutboxRecord, 0)
	for rows.Next() {
		var record ports.OutboxRecord
		var createdAt string
		if err := rows.Scan(&record.ID, &record.Channel, &record.Key, &record.Payload, &createdAt); err != nil {
			return nil, err
		}
		record.CreatedAt, err = parseTime(createdAt)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	result, err := s.database.ExecContext(ctx, `UPDATE outbox SET published_at=? WHERE id=? AND published_at IS NULL`, formatTime(s.now()), id)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return ports.ErrConflict
	}
	return nil
}

func (s *Store) RunnableWithoutSignal(ctx context.Context, limit int) ([]domain.RunSnapshot, error) {
	if limit <= 0 {
		limit = 100
	}
	return queryJSON[domain.RunSnapshot](ctx, s.database, `SELECT r.body FROM runs r
WHERE r.lifecycle=? AND NOT EXISTS (
  SELECT 1 FROM inbox i WHERE i.run_id=r.run_id AND i.consumed_step IS NULL
) LIMIT ?`, domain.LifecycleRunnable, limit)
}

func insertSignal(ctx context.Context, tx *sql.Tx, signal effects.StateSignal) error {
	body, err := marshal(signal)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO inbox(
signal_id,run_id,dedupe_key,priority,occurred_at,consumed_step,body
) VALUES(?,?,?,?,?,?,?)`, signal.SignalID, signal.RunID, signal.DedupeKey, signal.Priority,
		formatTime(signal.OccurredAt), signal.ConsumedByStep, body)
	return mapWriteError(err)
}

func insertSignalIgnoreDuplicate(ctx context.Context, tx *sql.Tx, signal effects.StateSignal) (bool, error) {
	body, err := marshal(signal)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO inbox(
signal_id,run_id,dedupe_key,priority,occurred_at,consumed_step,body
) VALUES(?,?,?,?,?,?,?)`, signal.SignalID, signal.RunID, signal.DedupeKey, signal.Priority,
		formatTime(signal.OccurredAt), signal.ConsumedByStep, body)
	if err != nil {
		return false, err
	}
	count, _ := result.RowsAffected()
	return count == 1, nil
}

func adjustPendingCount(ctx context.Context, tx *sql.Tx, runID string, delta int64, occurredAt time.Time) error {
	var body []byte
	if err := tx.QueryRowContext(ctx, `SELECT body FROM runs WHERE run_id=?`, runID).Scan(&body); err != nil {
		return mapReadError(err)
	}
	snapshot, err := unmarshal[domain.RunSnapshot](body)
	if err != nil {
		return err
	}
	snapshot.PendingCount += delta
	if snapshot.PendingCount < 0 {
		return errors.New("pendingCount 不得为负数")
	}
	if delta > 0 && (snapshot.OldestPendingAt == nil || occurredAt.Before(*snapshot.OldestPendingAt)) {
		value := occurredAt
		snapshot.OldestPendingAt = &value
	}
	updated, err := marshal(snapshot)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE runs SET body=? WHERE run_id=?`, updated, runID)
	return err
}

func insertEffect(ctx context.Context, tx *sql.Tx, entry effects.EffectLedgerEntry) error {
	body, err := marshal(entry)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO effects(effect_id,run_id,status,ledger_version,deadline,body)
VALUES(?,?,?,?,?,?)`, entry.EffectID, entry.RunID, entry.Status, entry.LedgerVersion,
		formatTime(entry.Deadline), body)
	return mapWriteError(err)
}

func insertTimer(ctx context.Context, tx *sql.Tx, timer effects.Timer) error {
	body, err := marshal(timer)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO timers(timer_id,run_id,status,due_at,entered_at_counter,body)
VALUES(?,?,?,?,?,?)`, timer.TimerID, timer.RunID, timer.Status, formatTime(timer.DueAt),
		timer.EnteredAtCounter, body)
	return mapWriteError(err)
}

func insertEvent(ctx context.Context, tx *sql.Tx, runID string, event domain.EventEnvelope) error {
	body, err := marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO events(event_id,run_id,sequence,body) VALUES(?,?,?,?)`,
		event.EventID, runID, event.Sequence, body)
	return mapWriteError(err)
}

func insertOutbox(ctx context.Context, tx *sql.Tx, records []ports.OutboxRecord) error {
	for _, record := range records {
		if _, err := tx.ExecContext(ctx, `INSERT INTO outbox(id,channel,key_value,payload,created_at)
VALUES(?,?,?,?,?)`, record.ID, record.Channel, record.Key, []byte(record.Payload), formatTime(record.CreatedAt)); err != nil {
			return mapWriteError(err)
		}
	}
	return nil
}

func insertJSON[T any](ctx context.Context, tx *sql.Tx, query string, value T, arguments ...any) error {
	body, err := marshal(value)
	if err != nil {
		return err
	}
	arguments = append(arguments, body)
	_, err = tx.ExecContext(ctx, query, arguments...)
	return mapWriteError(err)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func queryJSON[T any](ctx context.Context, database queryer, query string, arguments ...any) ([]T, error) {
	rows, err := database.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]T, 0)
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		value, err := unmarshal[T](body)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) inTransaction(ctx context.Context, operation func(*sql.Tx) error) error {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := operation(transaction); err != nil {
		_ = transaction.Rollback()
		return err
	}
	return transaction.Commit()
}

func marshal[T any](value T) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("编码存储记录：%w", err)
	}
	return body, nil
}

func unmarshal[T any](body []byte) (T, error) {
	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		return value, fmt.Errorf("解码存储记录：%w", err)
	}
	return value, nil
}

func mapReadError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ports.ErrNotFound
	}
	return err
}

func mapWriteError(err error) error {
	if err == nil {
		return nil
	}
	if isConstraintError(err) {
		return fmt.Errorf("%w：%v", ports.ErrConflict, err)
	}
	return err
}

func isConstraintError(err error) bool {
	message := err.Error()
	return contains(message, "constraint failed") || contains(message, "UNIQUE constraint") || contains(message, "FOREIGN KEY constraint")
}

func contains(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func parseTime(value string) (time.Time, error) { return time.Parse(time.RFC3339Nano, value) }
