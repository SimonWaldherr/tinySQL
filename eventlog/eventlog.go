// Package eventlog provides a transactional outbox with durable consumer cursors.
// It uses ordinary tinySQL tables and inherits the database's durability policy.
package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/SimonWaldherr/tinySQL/driver"
	"time"
)

var ErrCursorExpired = errors.New("event cursor precedes retention boundary")
var ErrAckConflict = errors.New("consumer cursor changed; read a fresh batch")

type Log struct{ db *sql.DB }
type Event struct {
	Sequence   int64
	Key, Topic string
	Payload    []byte
	CreatedAt  time.Time
}
type Batch struct {
	Events   []Event
	From, To int64
	log      *Log
	consumer string
	from, to int64
}

// Open initializes reserved _tiny_event_* tables. The caller owns db. Concurrent
// initialization or writes may return a transaction conflict: retry the transaction.
func openOnce(ctx context.Context, db *sql.DB) (*Log, error) {
	if db == nil {
		return nil, errors.New("database is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, q := range []string{
		"CREATE TABLE IF NOT EXISTS _tiny_event_log (seq INT PRIMARY KEY, event_key TEXT, topic TEXT, payload BLOB, created_ms INT)",
		"CREATE TABLE IF NOT EXISTS _tiny_event_state (name TEXT PRIMARY KEY, value INT)",
		"CREATE TABLE IF NOT EXISTS _tiny_event_consumers (name TEXT PRIMARY KEY, position INT)",
	} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"sequence", "floor"} {
		var n int64
		err = tx.QueryRowContext(ctx, "SELECT value FROM _tiny_event_state WHERE name = ?", name).Scan(&n)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, "INSERT INTO _tiny_event_state VALUES (?, ?)", name, int64(0))
		}
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &Log{db: db}, nil
}

// Append adds an event to the caller's transaction so business changes and the
// event commit together. A returned sequence is provisional until Commit succeeds.
// eventKey should be a stable application idempotency key. Duplicate keys are
// allowed: downstream consumers decide whether an effect was already applied.
func Append(ctx context.Context, tx *sql.Tx, eventKey, topic string, payload []byte) (int64, error) {
	if tx == nil || eventKey == "" || topic == "" {
		return 0, errors.New("transaction, event key and topic are required")
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, "SELECT value FROM _tiny_event_state WHERE name = ?", "sequence").Scan(&seq); err != nil {
		return 0, err
	}
	if seq == 1<<63-1 {
		return 0, errors.New("event sequence exhausted")
	}
	seq++
	if _, err := tx.ExecContext(ctx, "UPDATE _tiny_event_state SET value = ? WHERE name = ?", seq, "sequence"); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO _tiny_event_log VALUES (?, ?, ?, ?, ?)", seq, eventKey, topic, payload, time.Now().UnixMilli()); err != nil {
		return 0, err
	}
	return seq, nil
}

// Publish commits a standalone event. Use Append for an atomic business outbox.
func (l *Log) publishOnce(ctx context.Context, key, topic string, payload []byte) (int64, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	seq, err := Append(ctx, tx, key, topic, payload)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return seq, nil
}

// RegisterConsumer persists the last processed sequence. Existing consumers
// retain their cursor. Zero starts at the beginning when history is retained.
func (l *Log) registerConsumerOnce(ctx context.Context, name string, after int64) error {
	if name == "" || after < 0 {
		return errors.New("valid consumer name and cursor required")
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current int64
	err = tx.QueryRowContext(ctx, "SELECT position FROM _tiny_event_consumers WHERE name = ?", name).Scan(&current)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	floor, last, err := bounds(ctx, tx)
	if err != nil {
		return err
	}
	if after < floor {
		return ErrCursorExpired
	}
	if after > last {
		return errors.New("cursor is beyond the end of the log")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO _tiny_event_consumers VALUES (?, ?)", name, after); err != nil {
		return err
	}
	return tx.Commit()
}

// Read returns a bounded batch after the persisted cursor. Until Ack succeeds,
// another Read returns the same events (at-least-once processing). Consumers
// sharing a name must coordinate effects; Read is not an exclusive work lease.
func (l *Log) Read(ctx context.Context, consumer string, limit int) (*Batch, error) {
	if limit < 1 || limit > 10000 {
		return nil, errors.New("batch size must be between 1 and 10000")
	}
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var cursor int64
	if err = tx.QueryRowContext(ctx, "SELECT position FROM _tiny_event_consumers WHERE name = ?", consumer).Scan(&cursor); err != nil {
		return nil, err
	}
	floor, _, err := bounds(ctx, tx)
	if err != nil {
		return nil, err
	}
	if cursor < floor {
		return nil, ErrCursorExpired
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT seq, event_key, topic, payload, created_ms FROM _tiny_event_log WHERE seq > ? ORDER BY seq LIMIT %d", limit), cursor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	batch := &Batch{From: cursor, To: cursor, from: cursor, to: cursor, consumer: consumer, log: l}
	for rows.Next() {
		var e Event
		var ms int64
		if err = rows.Scan(&e.Sequence, &e.Key, &e.Topic, &e.Payload, &ms); err != nil {
			return nil, err
		}
		e.CreatedAt = time.UnixMilli(ms)
		batch.Events = append(batch.Events, e)
		batch.To = e.Sequence
		batch.to = e.Sequence
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	// This is a read-only snapshot; rollback releases it without a needless commit.
	return batch, nil
}

// Ack confirms all events in this batch after the caller has processed them.
// Repeating the same Ack is harmless; a stale competing batch cannot advance
// a newer cursor. External effects must be idempotent across crash/retry windows.
func (b *Batch) ackOnce(ctx context.Context) error {
	if b == nil || b.log == nil {
		return errors.New("invalid event batch")
	}
	tx, err := b.log.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current int64
	if err = tx.QueryRowContext(ctx, "SELECT position FROM _tiny_event_consumers WHERE name = ?", b.consumer).Scan(&current); err != nil {
		return err
	}
	if current >= b.to {
		return nil
	}
	if current != b.from {
		return ErrAckConflict
	}
	floor, _, err := bounds(ctx, tx)
	if err != nil {
		return err
	}
	if current < floor {
		return ErrCursorExpired
	}
	if _, err = tx.ExecContext(ctx, "UPDATE _tiny_event_consumers SET position = ? WHERE name = ?", b.to, b.consumer); err != nil {
		return err
	}
	return tx.Commit()
}

// Prune permanently removes events through a sequence and advances the retention
// boundary atomically. Lagging consumers then receive ErrCursorExpired rather
// than silently skipping missing events. Applications decide their retention policy.
func (l *Log) pruneOnce(ctx context.Context, through int64) error {
	if through < 0 {
		return errors.New("negative retention cursor")
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	floor, last, err := bounds(ctx, tx)
	if err != nil {
		return err
	}
	if through > last {
		through = last
	}
	if through <= floor {
		return nil
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM _tiny_event_log WHERE seq <= ?", through); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE _tiny_event_state SET value = ? WHERE name = ?", through, "floor"); err != nil {
		return err
	}
	return tx.Commit()
}

func bounds(ctx context.Context, tx *sql.Tx) (floor, last int64, err error) {
	if err = tx.QueryRowContext(ctx, "SELECT value FROM _tiny_event_state WHERE name = ?", "floor").Scan(&floor); err != nil {
		return
	}
	err = tx.QueryRowContext(ctx, "SELECT value FROM _tiny_event_state WHERE name = ?", "sequence").Scan(&last)
	return
}

// ConsumerStats exposes persistent progress and backlog. Lag is a sequence
// distance; no per-consumer copy of the event data is retained.
type ConsumerStats struct {
	Position, Latest, RetainedAfter, Lag int64
	Expired                              bool
}

func (l *Log) Stats(ctx context.Context, consumer string) (ConsumerStats, error) {
	var out ConsumerStats
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if err = tx.QueryRowContext(ctx, "SELECT position FROM _tiny_event_consumers WHERE name = ?", consumer).Scan(&out.Position); err != nil {
		return out, err
	}
	out.RetainedAfter, out.Latest, err = bounds(ctx, tx)
	if err != nil {
		return out, err
	}
	out.Lag = out.Latest - out.Position
	out.Expired = out.Position < out.RetainedAfter
	return out, nil
}

// Open creates the log schema and retries definite optimistic conflicts.
func Open(ctx context.Context, db *sql.DB) (*Log, error) {
	return retryConflict(ctx, func() (*Log, error) { return openOnce(ctx, db) })
}
func (l *Log) Publish(ctx context.Context, key, topic string, payload []byte) (int64, error) {
	return retryConflict(ctx, func() (int64, error) { return l.publishOnce(ctx, key, topic, payload) })
}
func (l *Log) RegisterConsumer(ctx context.Context, name string, after int64) error {
	_, err := retryConflict(ctx, func() (struct{}, error) { return struct{}{}, l.registerConsumerOnce(ctx, name, after) })
	return err
}
func (b *Batch) Ack(ctx context.Context) error {
	_, err := retryConflict(ctx, func() (struct{}, error) { return struct{}{}, b.ackOnce(ctx) })
	return err
}
func (l *Log) Prune(ctx context.Context, through int64) error {
	_, err := retryConflict(ctx, func() (struct{}, error) { return struct{}{}, l.pruneOnce(ctx, through) })
	return err
}

// Only conflict errors guarantee that the transaction was not committed. Other
// errors (including ambiguous I/O failures) are returned without republishing.
func retryConflict[T any](ctx context.Context, work func() (T, error)) (T, error) {
	var result T
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		result, err = work()
		if !errors.Is(err, driver.ErrTransactionConflict) {
			return result, err
		}
		if attempt == 3 {
			break
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	return result, err
}
