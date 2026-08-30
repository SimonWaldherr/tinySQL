package driver

import (
	"context"
	"database/sql"
	stdDriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// openStreamingDriverDB creates a result larger than engine.ResultStream's
// bounded buffer. That makes a query which has returned its sql.Rows but has
// not yet been consumed observably different from the old materialized path.
func openStreamingDriverDB(t testing.TB, rowCount int) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", "mem://?tenant=driver_streaming&pool_readers=1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	if _, err := db.Exec(`CREATE TABLE stream_rows (id INT, label TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	var insert strings.Builder
	insert.WriteString(`INSERT INTO stream_rows (id, label) VALUES `)
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			insert.WriteByte(',')
		}
		fmt.Fprintf(&insert, "(%d, 'row-%d')", i, i)
	}
	if _, err := db.Exec(insert.String()); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	return db
}

// TestDriverSelectTimeToFirstRowDoesNotMaterialize proves the driver exposes
// sql.Rows before the full result is consumed. The first query holds the sole
// server reader reservation while its producer is blocked behind the bounded
// stream buffer; an old materialized query would have released it before
// QueryContext returned.
func TestDriverSelectTimeToFirstRowDoesNotMaterialize(t *testing.T) {
	db := openStreamingDriverDB(t, 256)
	// The placeholder also exercises QueryContext's ad-hoc prepared-AST cache:
	// its execution must remain borrowed while the first stream is live.
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM stream_rows WHERE id >= ?`, 0)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("first row: %v", rows.Err())
	}
	var first int
	if err := rows.Scan(&first); err != nil {
		t.Fatalf("scan first row: %v", err)
	}
	if first != 0 {
		t.Fatalf("first id = %d, want 0", first)
	}

	blockedCtx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	blockedRows, err := db.QueryContext(blockedCtx, `SELECT id FROM stream_rows WHERE id >= ?`, 1)
	if blockedRows != nil {
		_ = blockedRows.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second query error = %v, want reader-pool context deadline", err)
	}
}

func TestDriverStreamCloseReleasesProducerAndLocks(t *testing.T) {
	db := openStreamingDriverDB(t, 256)
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM stream_rows`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("first row: %v", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	writeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := db.ExecContext(writeCtx, `INSERT INTO stream_rows VALUES (256, 'after-close')`); err != nil {
		t.Fatalf("writer after Rows.Close: %v", err)
	}
}

func TestDriverSlowStreamAllowsConcurrentWriteAndKeepsSnapshot(t *testing.T) {
	db := openStreamingDriverDB(t, 256)
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM stream_rows`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("first row: %v", rows.Err())
	}
	var first int
	if err := rows.Scan(&first); err != nil {
		t.Fatalf("scan first row: %v", err)
	}
	if first != 0 {
		t.Fatalf("first id = %d, want 0", first)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(context.Background(), `INSERT INTO stream_rows VALUES (256, 'concurrent')`)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("concurrent write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow database/sql stream blocked a concurrent writer")
	}

	count := 1
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan row %d: %v", count, err)
		}
		if id != count {
			t.Fatalf("snapshot row %d has id %d", count, id)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("finish snapshot: %v", err)
	}
	if count != 256 {
		t.Fatalf("stream saw %d rows, want original snapshot of 256", count)
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM stream_rows`).Scan(&total); err != nil {
		t.Fatalf("post-write count: %v", err)
	}
	if total != 257 {
		t.Fatalf("post-write count = %d, want 257", total)
	}
}

func TestDriverStreamContextCancellationReleasesProducerAndLocks(t *testing.T) {
	db := openStreamingDriverDB(t, 256)
	ctx, cancel := context.WithCancel(context.Background())
	rows, err := db.QueryContext(ctx, `SELECT id FROM stream_rows`)
	if err != nil {
		cancel()
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		cancel()
		t.Fatalf("first row: %v", rows.Err())
	}

	cancel()
	for rows.Next() {
	}
	if !errors.Is(rows.Err(), context.Canceled) {
		t.Fatalf("Rows.Err = %v, want context.Canceled", rows.Err())
	}

	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	defer writeCancel()
	if _, err := db.ExecContext(writeCtx, `INSERT INTO stream_rows VALUES (256, 'after-cancel')`); err != nil {
		t.Fatalf("writer after cancellation: %v", err)
	}
}

func TestDriverStreamNormalOutput(t *testing.T) {
	db := openStreamingDriverDB(t, 128)
	rows, err := db.QueryContext(context.Background(), `SELECT id, label FROM stream_rows`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if got, want := strings.Join(columns, ","), "id,label"; got != want {
		t.Fatalf("columns = %q, want %q", got, want)
	}

	count := 0
	for rows.Next() {
		var id int
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			t.Fatalf("scan row %d: %v", count, err)
		}
		if id != count || label != fmt.Sprintf("row-%d", count) {
			t.Fatalf("row %d = (%d, %q)", count, id, label)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if count != 128 {
		t.Fatalf("row count = %d, want 128", count)
	}
}

func TestDriverBlockingSelectSkipsResultStream(t *testing.T) {
	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	c := &conn{srv: srv, tenant: "default"}
	if _, err := c.execSQL(context.Background(), `CREATE TABLE blocking_rows (id INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.execSQL(context.Background(), `INSERT INTO blocking_rows VALUES (2), (1), (3)`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	stmt, err := parseSQLCached(`SELECT id FROM blocking_rows ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rawRows, err := c.queryStatement(context.Background(), stmt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rawRows.Close()
	result, ok := rawRows.(*rows)
	if !ok {
		t.Fatalf("rows type = %T, want *rows", rawRows)
	}
	if result.stream != nil {
		t.Fatal("blocking ORDER BY query used a redundant ResultStream")
	}

	dest := make([]stdDriver.Value, 1)
	for i, want := range []int64{1, 2} {
		if err := rawRows.Next(dest); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if got := dest[0]; got != want {
			t.Fatalf("row %d = %v, want %d", i, got, want)
		}
	}
	if err := rawRows.Next(dest); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next = %v, want EOF", err)
	}
}

// The prepared query pool contains mutable bound literals. Start a second
// stream before draining the first one; if the first execution were returned
// to the pool at QueryContext return, the second binding could turn the first
// stream's remaining rows from even to odd IDs.
func TestPreparedSelectStreamRetainsBoundExecution(t *testing.T) {
	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	c := &conn{srv: srv, tenant: "default"}
	if _, err := c.execSQL(context.Background(), `CREATE TABLE prepared_stream (id INT, bucket TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	table, err := srv.db.Get("default", "prepared_stream")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	table.Rows = make([][]any, 512)
	for i := range table.Rows {
		bucket := "right"
		if i%2 == 0 {
			bucket = "left"
		}
		table.Rows[i] = []any{i, bucket}
	}
	table.Version++

	rawStmt, err := c.Prepare(`SELECT id FROM prepared_stream WHERE bucket = ?`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	prepared := rawStmt.(*stmt)
	first, err := prepared.QueryContext(context.Background(), []stdDriver.NamedValue{{Ordinal: 1, Value: "left"}})
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	defer first.Close()
	if streamed, ok := first.(*rows); !ok || streamed.stream == nil {
		t.Fatalf("prepared SELECT did not return streamed rows: %T", first)
	}
	second, err := prepared.QueryContext(context.Background(), []stdDriver.NamedValue{{Ordinal: 1, Value: "right"}})
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	defer second.Close()

	dest := make([]stdDriver.Value, 1)
	count := 0
	for {
		err := first.Next(dest)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("first Next: %v", err)
		}
		id, ok := dest[0].(int64)
		if !ok {
			t.Fatalf("id type = %T, want int64", dest[0])
		}
		if id%2 != 0 {
			t.Fatalf("first stream changed binding at row %d: id = %d, want even", count, id)
		}
		count++
	}
	if count != 256 {
		t.Fatalf("first stream rows = %d, want 256", count)
	}
}

func TestPreparedReturningRemainsMaterialized(t *testing.T) {
	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	c := &conn{srv: srv, tenant: "default"}
	if _, err := c.execSQL(context.Background(), `CREATE TABLE prepared_returning (id INT, label TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	rawStmt, err := c.Prepare(`INSERT INTO prepared_returning VALUES (?, ?) RETURNING id, label`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	prepared := rawStmt.(*stmt)
	rawRows, err := prepared.QueryContext(context.Background(), []stdDriver.NamedValue{
		{Ordinal: 1, Value: int64(1)},
		{Ordinal: 2, Value: "created"},
	})
	if err != nil {
		t.Fatalf("query returning: %v", err)
	}
	defer rawRows.Close()
	result, ok := rawRows.(*rows)
	if !ok {
		t.Fatalf("rows type = %T, want *rows", rawRows)
	}
	if result.stream != nil {
		t.Fatal("DML RETURNING must finish atomically before exposing rows")
	}
	dest := make([]stdDriver.Value, 2)
	if err := rawRows.Next(dest); err != nil {
		t.Fatalf("RETURNING row: %v", err)
	}
	if dest[0] != int64(1) || dest[1] != "created" {
		t.Fatalf("RETURNING values = %v, want [1 created]", dest)
	}
}

// BenchmarkDriverStreamFirstRow measures the public database/sql path up to
// the first scanned value. Closing immediately models pagination and clients
// that only need an initial result while ensuring the held reader slot is
// released on every iteration.
func BenchmarkDriverStreamFirstRow(b *testing.B) {
	db := openStreamingDriverDB(b, 256)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, `SELECT id FROM stream_rows`)
		if err != nil {
			b.Fatal(err)
		}
		if !rows.Next() {
			err := rows.Err()
			_ = rows.Close()
			b.Fatalf("first row: %v", err)
		}
		var id int
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
