package eventlog_test

import (
	"errors"
	"github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/eventlog"
	"testing"
)

func TestOutboxRollbackReplayAckAndRetention(t *testing.T) {
	db, err := driver.OpenInMemory("")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	log, err := eventlog.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.RegisterConsumer(t.Context(), "worker", 0); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = eventlog.Append(t.Context(), tx, "rolled-back", "orders", []byte("no")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	batch, err := log.Read(t.Context(), "worker", 10)
	if err != nil || len(batch.Events) != 0 {
		t.Fatalf("rollback: %v %v", batch, err)
	}
	for _, key := range []string{"one", "two", "three"} {
		if _, err = log.Publish(t.Context(), key, "orders", []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	batch, err = log.Read(t.Context(), "worker", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 2 || batch.From != 0 || batch.To != 2 {
		t.Fatal(batch)
	}
	replay, err := log.Read(t.Context(), "worker", 2)
	if err != nil || replay.To != batch.To {
		t.Fatalf("replay: %v %v", replay, err)
	}
	// Public metadata cannot be changed to acknowledge events never delivered.
	batch.To = 999
	if err = batch.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = batch.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	tail, err := log.Read(t.Context(), "worker", 2)
	if err != nil || len(tail.Events) != 1 || tail.Events[0].Key != "three" {
		t.Fatalf("tail: %v %v", tail, err)
	}
	if err = log.RegisterConsumer(t.Context(), "lagging", 0); err != nil {
		t.Fatal(err)
	}
	if err = log.Prune(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	if _, err = log.Read(t.Context(), "lagging", 10); !errors.Is(err, eventlog.ErrCursorExpired) {
		t.Fatalf("retention: %v", err)
	}
	if err = tail.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestEventCursorSurvivesReopen(t *testing.T) {
	path := t.TempDir() + "/events.db"
	db, err := driver.Open("file:" + path + "?mode=disk")
	if err != nil {
		t.Fatal(err)
	}
	log, err := eventlog.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.RegisterConsumer(t.Context(), "worker", 0); err != nil {
		t.Fatal(err)
	}
	if _, err = log.Publish(t.Context(), "order-1", "orders", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	batch, err := log.Read(t.Context(), "worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = log.Publish(t.Context(), "order-2", "orders", []byte("world")); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = driver.Open("file:" + path + "?mode=disk")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	log, err = eventlog.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	batch, err = log.Read(t.Context(), "worker", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 1 || batch.Events[0].Key != "order-2" {
		t.Fatal(batch)
	}
}

func TestOutboxAtomicBusinessCommitAndConflict(t *testing.T) {
	db, err := driver.OpenInMemory("")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	log, err := eventlog.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(t.Context(), "CREATE TABLE orders (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err = log.RegisterConsumer(t.Context(), "worker", 0); err != nil {
		t.Fatal(err)
	}
	first, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	second, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	if _, err = first.ExecContext(t.Context(), "INSERT INTO orders VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err = eventlog.Append(t.Context(), first, "order-1", "orders", nil); err != nil {
		t.Fatal(err)
	}
	if _, err = second.ExecContext(t.Context(), "INSERT INTO orders VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if _, err = eventlog.Append(t.Context(), second, "order-2", "orders", nil); err != nil {
		t.Fatal(err)
	}
	if err = first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = second.Commit(); !errors.Is(err, driver.ErrTransactionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	var count int
	if err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM orders").Scan(&count); err != nil || count != 1 {
		t.Fatalf("orders: %d %v", count, err)
	}
	batch, err := log.Read(t.Context(), "worker", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 1 || batch.Events[0].Key != "order-1" {
		t.Fatal(batch)
	}
	stats, err := log.Stats(t.Context(), "worker")
	if err != nil || stats.Lag != 1 {
		t.Fatalf("stats: %v %v", stats, err)
	}
}
