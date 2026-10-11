package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSubscribeSQLRejectsCanceledContextBeforeParsing(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sub, err := SubscribeSQL(ctx, db, "default", "invalid SQL")
	if sub != nil {
		sub.Close()
		t.Fatal("canceled subscription started a worker")
	}
	if !errors.Is(err, context.Canceled) || db.HasChangeWatchers() {
		t.Fatalf("canceled subscription: err=%v, watching=%v", err, db.HasChangeWatchers())
	}
}

func TestSubscribeSQLAcceptsNilContext(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { db.Close() })
	sub, err := SubscribeSQL(nil, db, "default", "SELECT 1 AS id")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	change, ok := <-sub.Changes
	if !ok || !change.Initial || len(change.Added) != 1 || change.Added[0]["id"] != 1 {
		t.Fatalf("initial subscription result: %+v", change)
	}
	sub.Close()
	if err := sub.Err(); err != nil || db.HasChangeWatchers() {
		t.Fatalf("closed subscription: err=%v, watching=%v", err, db.HasChangeWatchers())
	}
}
