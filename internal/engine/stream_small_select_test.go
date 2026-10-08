package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func smallStreamDB(t testing.TB, rows int) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	for _, q := range []string{
		`CREATE TABLE s (id INT PRIMARY KEY, grp INT, name TEXT)`,
		`CREATE INDEX s_grp ON s (grp)`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < rows; i++ {
		q := fmt.Sprintf(`INSERT INTO s VALUES (%d, %d, 'n%d')`, i, i%7, i)
		if _, err := Execute(context.Background(), db, "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func drainStream(t *testing.T, stream *ResultStream) []Row {
	t.Helper()
	var out []Row
	for stream.Next() {
		row := Row{}
		for k, v := range stream.Row() {
			row[k] = v
		}
		out = append(out, row)
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	return out
}

func streamDone(s *ResultStream) bool {
	select {
	case <-s.Done():
		return true
	default:
		return false
	}
}

// A request that can only touch a few rows is answered before ExecuteStream
// returns: no producer goroutine is left running, and the stream is complete.
func TestExecuteStreamSmallSelectCompletesSynchronously(t *testing.T) {
	db := smallStreamDB(t, 500)
	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(`SELECT name FROM s WHERE id = 42`))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !streamDone(stream) {
		t.Fatal("point lookup left a producer running")
	}
	stats := stream.Stats()
	if !stats.Complete || stats.RowsProduced != 1 || stats.Materialized {
		t.Fatalf("stats = %+v", stats)
	}
	if got := drainStream(t, stream); len(got) != 1 || got[0]["name"] != "n42" {
		t.Fatalf("rows = %v", got)
	}
	if cols := stream.Columns(); len(cols) != 1 || cols[0] != "name" {
		t.Fatalf("columns = %v", cols)
	}
}

// Results that are not small, or whose caller asked for backpressure, keep the
// producer.
func TestExecuteStreamLargeOrBoundedStaysAsynchronous(t *testing.T) {
	db := smallStreamDB(t, 500)
	large, err := ExecuteStream(context.Background(), db, "default", mustParse(`SELECT id FROM s`))
	if err != nil {
		t.Fatal(err)
	}
	defer large.Close()
	if streamDone(large) {
		t.Fatal("a 500-row scan finished before any row was consumed")
	}

	strict, err := ExecuteStreamWithOptions(context.Background(), db, "default",
		mustParse(`SELECT name FROM s WHERE id = 1`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer strict.Close()
	if got := strict.Stats().BufferCapacity; got != 0 {
		t.Fatalf("strict stream reports buffer capacity %d", got)
	}
	if rows := drainStream(t, strict); len(rows) != 1 {
		t.Fatalf("strict rows = %v", rows)
	}

	sized, err := ExecuteStreamWithOptions(context.Background(), db, "default",
		mustParse(`SELECT name FROM s WHERE id = 1`), StreamOptions{Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer sized.Close()
	if got := sized.Stats().BufferCapacity; got != 8 {
		t.Fatalf("buffer capacity = %d, want the requested 8", got)
	}
}

// The synchronous path must return exactly what the producer and the
// materializing executor return.
func TestExecuteStreamSmallSelectMatchesOtherExecutors(t *testing.T) {
	db := smallStreamDB(t, 300)
	queries := []string{
		`SELECT id, name FROM s WHERE id = 7`,
		`SELECT id, name FROM s WHERE id = 100000`,
		`SELECT id FROM s WHERE id IN (1, 2, 3, 400)`,
		`SELECT id, grp FROM s WHERE id = 7 AND grp = 0`,
		`SELECT id, grp FROM s WHERE id = 7 AND grp = 1`,
		`SELECT id FROM s WHERE grp = 3 LIMIT 5`,
		`SELECT id FROM s WHERE grp = 3 LIMIT 5 OFFSET 2`,
		`SELECT id FROM s LIMIT 0`,
		`SELECT id FROM s LIMIT 3`,
		`SELECT id FROM s WHERE name = 'n250'`,
		`SELECT id AS ident, UPPER(name) AS up FROM s WHERE id = 9`,
		`SELECT * FROM s WHERE id = 12`,
	}
	ctx := context.Background()
	for _, q := range queries {
		stmt := mustParse(q)
		want, err := Execute(ctx, db, "default", stmt)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		for name, opts := range map[string]StreamOptions{
			"default": {Buffer: DefaultResultStreamBuffer},
			"strict":  {Buffer: 0},
		} {
			stream, err := ExecuteStreamWithOptions(ctx, db, "default", stmt, opts)
			if err != nil {
				t.Fatalf("%s [%s]: %v", q, name, err)
			}
			got := drainStream(t, stream)
			_ = stream.Close()
			if !reflect.DeepEqual(stream.Columns(), want.Cols) {
				t.Fatalf("%s [%s]: columns %v, want %v", q, name, stream.Columns(), want.Cols)
			}
			// Compare as multisets: a streamed scan and the executor agree on
			// content; limit-without-order queries agree on which rows too.
			if len(got) != len(want.Rows) {
				t.Fatalf("%s [%s]: %d rows, want %d", q, name, len(got), len(want.Rows))
			}
			for i := range got {
				if !reflect.DeepEqual(got[i], want.Rows[i]) {
					t.Fatalf("%s [%s]: row %d = %v, want %v", q, name, i, got[i], want.Rows[i])
				}
			}
		}
	}
}

func TestExecuteStreamSmallSelectReportsPlanningAndContextErrors(t *testing.T) {
	db := smallStreamDB(t, 10)
	if _, err := ExecuteStream(context.Background(), db, "default", mustParse(`SELECT id FROM missing_table WHERE id = 1`)); err == nil {
		t.Fatal("expected an error for a missing table")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ExecuteStream(ctx, db, "default", mustParse(`SELECT id FROM s WHERE id = 1`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// ReleaseRow recycles rows through the pool; recycled rows must not leak values
// between queries.
func TestExecuteStreamSmallSelectRowReuseDoesNotLeak(t *testing.T) {
	db := smallStreamDB(t, 50)
	for i := 0; i < 200; i++ {
		id := i % 50
		stream, err := ExecuteStream(context.Background(), db, "default",
			mustParse(fmt.Sprintf(`SELECT name FROM s WHERE id = %d`, id)))
		if err != nil {
			t.Fatal(err)
		}
		if !stream.Next() {
			t.Fatalf("no row for id %d", id)
		}
		row := stream.Row()
		if len(row) != 1 || row["name"] != fmt.Sprintf("n%d", id) {
			t.Fatalf("id %d: row = %v", id, row)
		}
		stream.ReleaseRow(row)
		if stream.Next() {
			t.Fatal("unexpected second row")
		}
		_ = stream.Close()
	}
}

// A scan that outgrows the synchronous first phase is continued by the
// producer from where the first phase stopped: nothing lost, repeated or
// reordered, wherever the boundary falls.
func TestExecuteStreamHandoffAfterSynchronousPhase(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{10, 63, 64, 65, 66, 200, 2047, 2048, 2049, 5000} {
		db := storage.NewDB()
		if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE h (id INT, grp INT, name TEXT)`)); err != nil {
			t.Fatal(err)
		}
		table, _ := db.Get("default", "h")
		table.Rows = make([][]any, n)
		for i := range table.Rows {
			table.Rows[i] = []any{i, i % 5, fmt.Sprintf("n%d", i)}
		}
		table.Version++
		queries := []string{
			`SELECT id FROM h`,
			`SELECT id, name FROM h WHERE grp = 2`,
			`SELECT id FROM h WHERE id >= 2040 AND id < 2060`, // matches only past the candidate budget
			fmt.Sprintf(`SELECT id FROM h WHERE id = %d`, n-1),
			`SELECT id FROM h LIMIT 100`,
			`SELECT id FROM h LIMIT 100 OFFSET 30`,
			`SELECT id FROM h WHERE grp = 1 LIMIT 70 OFFSET 20`,
			`SELECT id FROM h WHERE grp = 9`,
		}
		for _, q := range queries {
			stmt := mustParse(q)
			want, err := Execute(ctx, db, "default", stmt)
			if err != nil {
				t.Fatalf("n=%d %s: %v", n, q, err)
			}
			for _, buf := range []int{DefaultResultStreamBuffer, 5, 0, 1000} {
				stream, err := ExecuteStreamWithOptions(ctx, db, "default", stmt, StreamOptions{Buffer: buf})
				if err != nil {
					t.Fatalf("n=%d %s buf=%d: %v", n, q, buf, err)
				}
				got := drainStream(t, stream)
				stats := stream.Stats()
				_ = stream.Close()
				if len(got) != len(want.Rows) {
					t.Fatalf("n=%d %s buf=%d: %d rows, want %d", n, q, buf, len(got), len(want.Rows))
				}
				for i := range got {
					if !reflect.DeepEqual(got[i], want.Rows[i]) {
						t.Fatalf("n=%d %s buf=%d: row %d = %v, want %v", n, q, buf, i, got[i], want.Rows[i])
					}
				}
				if int(stats.RowsProduced) != len(want.Rows) || !stats.Complete {
					t.Fatalf("n=%d %s buf=%d: stats %+v for %d rows", n, q, buf, stats, len(want.Rows))
				}
			}
		}
	}
}

// Abandoning a stream that is still being produced after the hand-off must
// release the table pin and the producer, like any other stream.
func TestExecuteStreamHandoffCloseReleasesProducer(t *testing.T) {
	db := smallStreamDB(t, 3000)
	stream, err := ExecuteStream(context.Background(), db, "default", mustParse(`SELECT id FROM s`))
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Next() {
		t.Fatalf("no first row: %v", stream.Err())
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-stream.Done():
	default:
		t.Fatal("producer still running after Close")
	}
	// A writer must now proceed: neither the read lock nor a table pin is held.
	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO s VALUES (100000, 0, 'late')`)); err != nil {
		t.Fatal(err)
	}
}

// The stream keeps reading the table as it was when the query started, even if
// a writer changes it after the hand-off.
func TestExecuteStreamHandoffKeepsSnapshot(t *testing.T) {
	db := smallStreamDB(t, 300)
	ctx := context.Background()
	stream, err := ExecuteStream(ctx, db, "default", mustParse(`SELECT id FROM s`))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Next() {
		t.Fatal("no first row")
	}
	for _, q := range []string{`DELETE FROM s WHERE grp = 3`, `INSERT INTO s VALUES (9999, 1, 'new')`, `UPDATE s SET name = 'changed'`} {
		if _, err := Execute(ctx, db, "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	rows := 1
	for stream.Next() {
		rows++
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 300 {
		t.Fatalf("stream returned %d rows, want the 300 that existed when it started", rows)
	}
}
