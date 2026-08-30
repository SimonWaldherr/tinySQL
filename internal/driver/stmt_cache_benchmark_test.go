package driver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// BenchmarkRepeatedSelectViaDriver measures the database/sql round-trip cost
// of re-issuing the same SELECT text — the shape the parsed-statement cache
// (parseSQLCached) accelerates. Run with the cache disabled by raising the
// statement above parsedStmtCacheMaxSQLLen if a no-cache baseline is needed.
func BenchmarkRepeatedSelectViaDriver(b *testing.B) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE bench_docs (id INT, score FLOAT, title TEXT)`); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO bench_docs VALUES (%d, %d.5, 'document title %d')`, i, i%10, i)); err != nil {
			b.Fatal(err)
		}
	}

	const q = `SELECT id, title FROM bench_docs WHERE score > 3.0 AND title LIKE 'document%' ORDER BY id DESC LIMIT 10`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		if n != 10 {
			b.Fatalf("expected 10 rows, got %d", n)
		}
	}
}

// BenchmarkRepeatedSelectViaDriverParallel measures concurrent ad-hoc
// executions of one cached SELECT AST. Unlike prepared statements, every
// connection intentionally shares the parsed statement and its immutable
// simple-plan template, so this catches accidental serialization of cache
// hits under an exclusive plan-cache lock.
func BenchmarkRepeatedSelectViaDriverParallel(b *testing.B) {
	db, err := sql.Open("tinysql", "mem://?tenant=parallel_plan_cache&pool_readers=0")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE parallel_bench_docs (id INT, score FLOAT, title TEXT)`); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO parallel_bench_docs VALUES (%d, %d.5, 'document title %d')`, i, i%10, i)); err != nil {
			b.Fatal(err)
		}
	}

	const q = `SELECT id, title FROM parallel_bench_docs WHERE score > 3.0 AND title LIKE 'document%' ORDER BY id DESC LIMIT 10`
	b.ReportAllocs()
	b.SetParallelism(2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rows, err := db.QueryContext(ctx, q)
			if err != nil {
				b.Error(err)
				return
			}
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Close(); err != nil {
				b.Error(err)
				return
			}
			if n != 10 {
				b.Errorf("expected 10 rows, got %d", n)
				return
			}
		}
	})
}

// BenchmarkCachedPointSelectViaDriverParallel isolates coordination overhead
// for the short indexed lookups common in tile stores, offline navigation and
// embedded tools. All workers reuse one ad-hoc SQL text and therefore one
// parsed AST/plan cache entry.
func BenchmarkCachedPointSelectViaDriverParallel(b *testing.B) {
	db, err := sql.Open("tinysql", "mem://?tenant=parallel_point_cache&pool_readers=0")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE parallel_points (id INT, label TEXT)`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX parallel_points_id ON parallel_points(id)`); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO parallel_points VALUES (%d, 'point-%d')`, i, i)); err != nil {
			b.Fatal(err)
		}
	}

	const q = `SELECT label FROM parallel_points WHERE id = 42`
	b.ReportAllocs()
	b.SetParallelism(2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var label string
			if err := db.QueryRowContext(ctx, q).Scan(&label); err != nil {
				b.Error(err)
				return
			}
			if label != "point-42" {
				b.Errorf("label = %q, want point-42", label)
				return
			}
		}
	})
}
