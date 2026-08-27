package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// openReadOnlyPagedStreamDB creates a serving artifact deliberately larger
// than a cursor batch, then reopens it read-only. The reopen is important:
// streaming from a mutable paged artifact would not have a stable root across
// batches, so lazy cursor execution is intentionally limited to this mode.
func openReadOnlyPagedStreamDB(t *testing.T, rows int) *storage.DB {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "paged-stream")
	builder, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE TABLE paged_stream_rows (id INT, name TEXT)`,
		`CREATE INDEX idx_paged_stream_rows_id ON paged_stream_rows(id)`,
	} {
		if _, err := Execute(context.Background(), builder, "default", mustParsePagedSQL(t, sql)); err != nil {
			_ = builder.Close()
			t.Fatalf("build %q: %v", sql, err)
		}
	}
	table, err := builder.Get("default", "paged_stream_rows")
	if err != nil {
		_ = builder.Close()
		t.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := range table.Rows {
		table.Rows[i] = []any{i, "row"}
	}
	table.Version++
	if err := table.RebuildSecondaryIndexes(); err != nil {
		_ = builder.Close()
		t.Fatal(err)
	}
	if err := builder.Sync(); err != nil {
		_ = builder.Close()
		t.Fatal(err)
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		ReadOnly:       true,
		MaxMemoryBytes: 32 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func TestExecuteStreamPagedTableFirstRowDoesNotMaterialize(t *testing.T) {
	db := openReadOnlyPagedStreamDB(t, 256)
	stream, err := ExecuteStreamWithOptions(context.Background(), db, "default", mustParsePagedSQL(t, `SELECT id FROM paged_stream_rows`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	if stats := stream.Stats(); stats.Materialized {
		t.Fatalf("paged simple select was materialized: %#v", stats)
	}
	if stats := db.BackendStats(); stats.LoadCount != 0 {
		t.Fatalf("stream setup called LoadTable %d times", stats.LoadCount)
	}
	select {
	case <-stream.Done():
		t.Fatal("paged stream completed before its consumer accepted the first row")
	default:
	}
	if !stream.Next() {
		t.Fatalf("first paged row: %v", stream.Err())
	}
	if got := expectAsInt(t, stream.Row()["id"]); got != 0 {
		t.Fatalf("first paged id = %d, want 0", got)
	}
	if stats := db.BackendStats(); stats.LoadCount != 0 {
		t.Fatalf("first paged row fell back to full LoadTable (%d loads)", stats.LoadCount)
	}
}

func TestExecuteStreamPagedIndexRangeFirstRowDoesNotMaterialize(t *testing.T) {
	db := openReadOnlyPagedStreamDB(t, 256)
	stmt := mustParsePagedSQL(t, `SELECT id FROM paged_stream_rows WHERE id >= 64 AND id < 96`)
	selectStmt, ok := stmt.(*Select)
	if !ok {
		t.Fatalf("parsed %T, want *Select", stmt)
	}

	// Inspect the private plan in addition to the observable no-LoadTable
	// assertion below. This makes a regression from a range cursor to a table
	// cursor visible even when a small test table happens to fit in cache.
	db.LockContentForRead()
	plan, handled, err := buildStreamingSimpleSelectPlan(ExecEnv{
		ctx:           context.Background(),
		tenant:        "default",
		db:            db,
		now:           time.Now(),
		subqueryCache: newSubqueryResultCache(),
	}, selectStmt)
	db.UnlockContentForRead()
	if err != nil {
		t.Fatal(err)
	}
	if !handled || plan == nil || plan.pagedSource == nil || plan.pagedSource.indexName != "idx_paged_stream_rows_id" || plan.scanType != "PAGED INDEX RANGE SCAN" {
		t.Fatalf("range stream plan = %#v", plan)
	}

	stream, err := ExecuteStreamWithOptions(context.Background(), db, "default", stmt, StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Next() {
		t.Fatalf("first paged range row: %v", stream.Err())
	}
	if got := expectAsInt(t, stream.Row()["id"]); got != 64 {
		t.Fatalf("first paged range id = %d, want 64", got)
	}
	if stats := db.BackendStats(); stats.LoadCount != 0 {
		t.Fatalf("paged range fell back to full LoadTable (%d loads)", stats.LoadCount)
	}

	count := 1
	for stream.Next() {
		count++
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 32 {
		t.Fatalf("paged range returned %d rows, want 32", count)
	}
}

func TestExecuteStreamPagedCancellationStopsBlockedConsumer(t *testing.T) {
	db := openReadOnlyPagedStreamDB(t, 256)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := ExecuteStreamWithOptions(ctx, db, "default", mustParsePagedSQL(t, `SELECT id FROM paged_stream_rows`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("cancelled paged stream did not stop promptly")
	}
	if !errors.Is(stream.Err(), context.Canceled) {
		t.Fatalf("stream error = %v, want context.Canceled", stream.Err())
	}
	if stats := db.BackendStats(); stats.PinnedPages != 0 || stats.TransientFrames != 0 {
		t.Fatalf("cancelled paged stream retained pager state: %#v", stats)
	}
}
