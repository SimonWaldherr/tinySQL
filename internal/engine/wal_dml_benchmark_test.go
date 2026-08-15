// Benchmarks for INSERT/UPDATE with a write-ahead log attached.
//
// The DML benchmarks elsewhere in this package all run against a pure
// in-memory database, where the WAL costs nothing because there is none. These
// measure what a durable deployment actually pays per statement: the
// pre-execution metadata image, the whole-database change diff, the record
// serialization, and the fsync.
//
// The indexed variants matter most. A WAL delta record describes only the rows
// a statement touched, but the table it belongs to may carry a secondary index
// of any size, and how much of that index ends up in the record decides
// whether a single-row write costs O(1) or O(index).
package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// walBenchDB opens a ModeWAL database seeded with `rows` rows, optionally
// carrying a secondary index over a low-cardinality column.
func walBenchDB(b *testing.B, rows int, indexed bool) *storage.DB {
	b.Helper()
	cfg := storage.DefaultStorageConfig(storage.ModeWAL)
	cfg.Path = filepath.Join(b.TempDir(), "wal")
	db, err := storage.OpenDB(cfg)
	if err != nil {
		b.Fatalf("open ModeWAL database: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE t (id INT PRIMARY KEY, bucket INT, val TEXT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i++ {
		table.Rows[i] = []any{i, i % 64, fmt.Sprintf("val-%d", i)}
	}
	table.Version++
	if indexed {
		if _, err := Execute(ctx, db, "default", mustParse(
			`CREATE INDEX idx_bucket ON t(bucket)`)); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

func benchmarkWALInsert(b *testing.B, indexed bool) {
	const seed = 10000
	db := walBenchDB(b, seed, indexed)
	ctx := context.Background()
	stmts := make([]Statement, b.N)
	for i := range stmts {
		stmts[i] = mustParse(fmt.Sprintf(`INSERT INTO t VALUES (%d, %d, 'x')`, seed+i, i%64))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmts[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALInsertUnindexed is the baseline: one durable single-row INSERT.
func BenchmarkWALInsertUnindexed(b *testing.B) { benchmarkWALInsert(b, false) }

// BenchmarkWALInsertIndexed is the same statement on a table that also has a
// secondary index. The gap to the unindexed variant is what index maintenance
// plus whatever the WAL record carries about the index costs per row; it
// should not grow with the number of rows already in the table.
func BenchmarkWALInsertIndexed(b *testing.B) { benchmarkWALInsert(b, true) }

func benchmarkWALPointUpdate(b *testing.B, indexed bool) {
	db := walBenchDB(b, 10000, indexed)
	ctx := context.Background()
	stmt := mustParse(`UPDATE t SET val = 'y' WHERE id = 4321`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALUpdateUnindexed / BenchmarkWALUpdateIndexed measure a durable
// point UPDATE, which the WAL writes as a single-row delta record.
func BenchmarkWALUpdateUnindexed(b *testing.B) { benchmarkWALPointUpdate(b, false) }
func BenchmarkWALUpdateIndexed(b *testing.B)   { benchmarkWALPointUpdate(b, true) }

// BenchmarkWALUpdateMatchingNoRows is the statement that should be nearly
// free: a durable UPDATE whose WHERE clause matches nothing has changed no
// table, so it has nothing to log and nothing to fsync.
func BenchmarkWALUpdateMatchingNoRows(b *testing.B) {
	db := walBenchDB(b, 10000, false)
	ctx := context.Background()
	stmt := mustParse(`UPDATE t SET val = 'y' WHERE id = 999999`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALInsertManyTables measures how a durable single-row INSERT scales
// with the number of tables in the database — the per-statement pre-image and
// change diff both walk the whole catalog, so this is where that shows up.
func BenchmarkWALInsertManyTables(b *testing.B) {
	cfg := storage.DefaultStorageConfig(storage.ModeWAL)
	cfg.Path = filepath.Join(b.TempDir(), "wal")
	db, err := storage.OpenDB(cfg)
	if err != nil {
		b.Fatalf("open ModeWAL database: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	const tables = 32
	for i := 0; i < tables; i++ {
		if _, err := Execute(ctx, db, "default", mustParse(
			fmt.Sprintf(`CREATE TABLE t%d (id INT PRIMARY KEY, val TEXT)`, i))); err != nil {
			b.Fatal(err)
		}
	}
	stmts := make([]Statement, b.N)
	for i := range stmts {
		stmts[i] = mustParse(fmt.Sprintf(`INSERT INTO t0 VALUES (%d, 'x')`, i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmts[i]); err != nil {
			b.Fatal(err)
		}
	}
}
