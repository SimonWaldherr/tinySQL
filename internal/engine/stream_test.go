package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func streamTestDB(t testing.TB, rows int) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE stream_rows (id INT, name TEXT)`)); err != nil {
		t.Fatal(err)
	}
	table, err := db.Get("default", "stream_rows")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := range table.Rows {
		table.Rows[i] = []any{i, "row"}
	}
	table.Version++
	return db
}

func BenchmarkExecuteStreamFirstRow(b *testing.B) {
	db := streamTestDB(b, 20000)
	stmt := mustParse(`SELECT id FROM stream_rows`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := ExecuteStream(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if !stream.Next() {
			b.Fatalf("Next: %v", stream.Err())
		}
		if err := stream.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecuteStreamAllRows(b *testing.B) {
	db := streamTestDB(b, 20000)
	stmt := mustParse(`SELECT id FROM stream_rows`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := ExecuteStream(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		rows := 0
		for stream.Next() {
			_ = stream.Row()
			rows++
		}
		if err := stream.Err(); err != nil {
			b.Fatal(err)
		}
		if rows != 20000 {
			b.Fatalf("rows = %d, want 20000", rows)
		}
	}
}

func BenchmarkExecuteMaterializedFirstRow(b *testing.B) {
	db := streamTestDB(b, 20000)
	stmt := mustParse(`SELECT id FROM stream_rows`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) == 0 {
			b.Fatal("missing first row")
		}
	}
}

func TestExecuteStreamYieldsBeforeScanCompletes(t *testing.T) {
	db := streamTestDB(t, 1000)
	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(
		`SELECT id FROM stream_rows WHERE id >= 0 LIMIT 100 OFFSET 5`))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if len(stream.Cols) != 1 || stream.Cols[0] != "id" {
		t.Fatalf("columns = %v, want [id]", stream.Cols)
	}

	// No rows have been consumed and the result exceeds the bounded channel,
	// so completion here would prove the producer materialized all 100 rows.
	select {
	case <-stream.done:
		t.Fatal("stream completed before the consumer read its first row")
	default:
	}

	count := 0
	for stream.Next() {
		got := stream.Row()["id"]
		want := 5 + count
		if got != want {
			t.Fatalf("row %d id = %v, want %d", count, got, want)
		}
		count++
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("rows = %d, want 100", count)
	}
}

func TestExecuteStreamCloseStopsProducer(t *testing.T) {
	db := streamTestDB(t, 1000)
	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(`SELECT * FROM stream_rows`))
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Next() {
		t.Fatalf("first Next failed: %v", stream.Err())
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("explicit Close must not surface cancellation: %v", err)
	}

	// Close waits for the producer to release the read lock, so a following
	// writer must be able to complete immediately.
	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO stream_rows VALUES (1001, 'after close')`)); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteStreamContextCancellation(t *testing.T) {
	db := streamTestDB(t, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := ExecuteStream(ctx, db, "default", mustParse(`SELECT * FROM stream_rows`))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for stream.Next() {
	}
	if !errors.Is(stream.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", stream.Err())
	}
}

func TestExecuteStreamReportsErrorAfterEarlierRows(t *testing.T) {
	db := streamTestDB(t, 2)
	table, _ := db.Get("default", "stream_rows")
	table.Rows[1] = []any{} // malformed direct-storage row: projection fails later

	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(`SELECT id FROM stream_rows`))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Next() || stream.Row()["id"] != 0 {
		t.Fatalf("first row = %v, err=%v", stream.Row(), stream.Err())
	}
	if stream.Next() {
		t.Fatal("unexpected second row")
	}
	if stream.Err() == nil {
		t.Fatal("expected the producer error after the first row")
	}
}

func TestExecuteStreamBlockingShapePreservesOrdering(t *testing.T) {
	db := streamTestDB(t, 4)
	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(
		`SELECT id FROM stream_rows ORDER BY id DESC LIMIT 2`))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var ids []any
	for stream.Next() {
		ids = append(ids, stream.Row()["id"])
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 3 || ids[1] != 2 {
		t.Fatalf("ordered ids = %v, want [3 2]", ids)
	}
}

func TestExecuteStreamWithOptionsReportsProgress(t *testing.T) {
	db := streamTestDB(t, 3)
	stream, err := ExecuteStreamWithOptions(context.Background(), db, "default", mustParse(`SELECT id FROM stream_rows`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	if got := stream.Stats(); got.BufferCapacity != 0 || got.Materialized || got.Complete || got.StartedAt.IsZero() {
		t.Fatalf("initial stats = %#v", got)
	}
	for stream.Next() {
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	got := stream.Stats()
	if !got.Complete || got.CompletedAt.IsZero() || got.FirstRowAt.IsZero() {
		t.Fatalf("completed stats = %#v", got)
	}
	if got.RowsScanned != 3 || got.RowsProduced != 3 {
		t.Fatalf("stream progress = scanned %d, produced %d; want 3, 3", got.RowsScanned, got.RowsProduced)
	}
}

func TestExecuteStreamWithOptionsRejectsNegativeBuffer(t *testing.T) {
	db := streamTestDB(t, 1)
	_, err := ExecuteStreamWithOptions(context.Background(), db, "default", mustParse(`SELECT id FROM stream_rows`), StreamOptions{Buffer: -1})
	if err == nil {
		t.Fatal("expected negative buffer to fail")
	}
}

// TestExecuteStreamDistinctDedupes pins the DISTINCT gate on the streaming
// path. simpleSelectEligible admits DISTINCT so that the materialized fast path
// can claim it, but streamSimpleSelectPlan emits rows as it scans and cannot
// dedupe; a shared predicate once let DISTINCT stream every duplicate through
// database/sql.
func TestExecuteStreamDistinctDedupes(t *testing.T) {
	db := streamTestDB(t, 10)
	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(
		`SELECT DISTINCT name FROM stream_rows`))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var names []any
	for stream.Next() {
		names = append(names, stream.Row()["name"])
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "row" {
		t.Fatalf("streamed DISTINCT name = %v, want [row]", names)
	}
}
