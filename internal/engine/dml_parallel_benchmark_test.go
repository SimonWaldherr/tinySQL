// Benchmarks for concurrent write throughput.
//
// The single-threaded INSERT/UPDATE benchmarks in perf_benchmark_test.go say
// how expensive one statement is; these say how much of that cost can run at
// the same time as another statement's. They deliberately write to *disjoint*
// tables (and, in the tenant variants, disjoint tenants), so a perfectly
// parallel engine would keep ns/op flat as GOMAXPROCS goes up and only a
// shared bottleneck — a process-wide lock or a process-wide cache — can make
// it climb.
//
// Read them as a ratio, never as an absolute: divide the parallel ns/op by the
// matching serial benchmark's. 1.0 means the writers never met; N means they
// serialized completely.
package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// parallelDMLMinTables keeps small machines from measuring an accidentally
// tiny table catalog. parallelDMLTableCount additionally grows with
// GOMAXPROCS: RunParallel starts one worker per logical processor by default,
// and every worker must own a distinct table for this benchmark to measure
// only shared engine contention.
const parallelDMLMinTables = 32

func parallelDMLTableCount() int {
	if workers := runtime.GOMAXPROCS(0); workers > parallelDMLMinTables {
		return workers
	}
	return parallelDMLMinTables
}

func TestParallelDMLTableCountCoversRunParallelWorkers(t *testing.T) {
	const workers = 64
	previous := runtime.GOMAXPROCS(workers)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })

	if got := parallelDMLTableCount(); got < workers {
		t.Fatalf("parallel DML tables = %d, want at least %d RunParallel workers", got, workers)
	}
}

// setupParallelDMLTables creates one table per RunParallel worker (with a
// minimum of parallelDMLMinTables), each seeded with
// seedRows rows, and returns the database. Tables are named t0..tN-1 and share
// the schema (id INT PRIMARY KEY, bucket INT, val TEXT).
func setupParallelDMLTables(b *testing.B, tenants bool, seedRows int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	for i := 0; i < parallelDMLTableCount(); i++ {
		tenant, name := parallelDMLTarget(tenants, i)
		if _, err := Execute(ctx, db, tenant, mustParse(
			fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, bucket INT, val TEXT)`, name))); err != nil {
			b.Fatal(err)
		}
		if seedRows == 0 {
			continue
		}
		table, err := db.Get(tenant, name)
		if err != nil {
			b.Fatal(err)
		}
		table.Rows = make([][]any, seedRows)
		for r := 0; r < seedRows; r++ {
			table.Rows[r] = []any{float64(r), float64(r % 64), fmt.Sprintf("val-%d", r)}
		}
		table.Version++
	}
	return db
}

// parallelDMLTarget maps a worker index onto its (tenant, table) pair: either
// one tenant with many tables, or many tenants each with one table.
func parallelDMLTarget(tenants bool, i int) (string, string) {
	if tenants {
		return fmt.Sprintf("tenant%d", i), "t"
	}
	return "default", fmt.Sprintf("t%d", i)
}

func benchmarkParallelInsert(b *testing.B, tenants bool) {
	db := setupParallelDMLTables(b, tenants, 0)
	ctx := context.Background()
	tableCount := parallelDMLTableCount()
	var worker atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(worker.Add(1)-1) % tableCount
		tenant, name := parallelDMLTarget(tenants, id)
		// Each worker owns its table, so it also owns the primary keys it
		// inserts: no two workers can collide on the PK index.
		row := 0
		for pb.Next() {
			stmt := mustParse(fmt.Sprintf(
				`INSERT INTO %s (id, bucket, val) VALUES (%d, %d, 'x')`, name, row, row%64))
			if _, err := Execute(ctx, db, tenant, stmt); err != nil {
				b.Fatal(err)
			}
			row++
		}
	})
}

// BenchmarkParallelInsertDistinctTables writes into one table per available
// core (at least 32) in one tenant. Compare against
// BenchmarkSerialInsertOneTable.
func BenchmarkParallelInsertDistinctTables(b *testing.B) { benchmarkParallelInsert(b, false) }

// BenchmarkParallelInsertDistinctTenants is the same workload with full tenant
// isolation, which is the strongest case for independent writes: nothing about
// two tenants' tables is logically shared.
func BenchmarkParallelInsertDistinctTenants(b *testing.B) { benchmarkParallelInsert(b, true) }

// BenchmarkSerialInsertOneTable is the single-goroutine reference point for the
// two benchmarks above: the same statement shape, one writer, no contention.
func BenchmarkSerialInsertOneTable(b *testing.B) {
	db := setupParallelDMLTables(b, false, 0)
	ctx := context.Background()
	stmts := make([]Statement, b.N)
	for i := range stmts {
		stmts[i] = mustParse(fmt.Sprintf(`INSERT INTO t0 (id, bucket, val) VALUES (%d, %d, 'x')`, i, i%64))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmts[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkParallelPointUpdate(b *testing.B, tenants bool) {
	const seedRows = 5000
	db := setupParallelDMLTables(b, tenants, seedRows)
	ctx := context.Background()
	tableCount := parallelDMLTableCount()
	var worker atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(worker.Add(1)-1) % tableCount
		tenant, name := parallelDMLTarget(tenants, id)
		stmt := mustParse(fmt.Sprintf(`UPDATE %s SET val = 'y' WHERE id = 1234`, name))
		for pb.Next() {
			if _, err := Execute(ctx, db, tenant, stmt); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkParallelUpdateDistinctTables drives the point-UPDATE fast path from
// every core, each worker on its own table. Compare against
// BenchmarkSerialUpdateOneTable.
func BenchmarkParallelUpdateDistinctTables(b *testing.B) { benchmarkParallelPointUpdate(b, false) }

// BenchmarkParallelUpdateDistinctTenants is the tenant-isolated variant.
func BenchmarkParallelUpdateDistinctTenants(b *testing.B) { benchmarkParallelPointUpdate(b, true) }

// BenchmarkSerialUpdateOneTable is the single-goroutine reference point for the
// two benchmarks above.
func BenchmarkSerialUpdateOneTable(b *testing.B) {
	const seedRows = 5000
	db := setupParallelDMLTables(b, false, seedRows)
	ctx := context.Background()
	stmt := mustParse(`UPDATE t0 SET val = 'y' WHERE id = 1234`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParallelReadWriteMix runs one writer per table alongside readers of
// other tables — the shape a serving process actually has. A writer that holds
// a process-wide exclusive lock blocks every reader in the process, so this
// benchmark is where that shows up as latency rather than as throughput.
func BenchmarkParallelReadWriteMix(b *testing.B) {
	const seedRows = 5000
	db := setupParallelDMLTables(b, false, seedRows)
	ctx := context.Background()
	tableCount := parallelDMLTableCount()
	var worker atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(worker.Add(1) - 1)
		table := fmt.Sprintf("t%d", id%tableCount)
		// Every fourth worker writes; the rest read a different table.
		write := id%4 == 0
		var stmt Statement
		if write {
			stmt = mustParse(fmt.Sprintf(`UPDATE %s SET val = 'y' WHERE id = 1234`, table))
		} else {
			stmt = mustParse(fmt.Sprintf(`SELECT val FROM %s WHERE id = 4321`, table))
		}
		for pb.Next() {
			if _, err := Execute(ctx, db, "default", stmt); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMultiRowInsertBatch measures one INSERT statement carrying 100 rows,
// i.e. the per-row cost once the per-statement overhead is amortized. The gap
// between this and BenchmarkSerialInsertOneTable is exactly what a client gains
// by batching, and shrinking that gap is what makes batching less necessary.
func BenchmarkMultiRowInsertBatch(b *testing.B) {
	const rowsPerStatement = 100
	db := setupParallelDMLTables(b, false, 0)
	ctx := context.Background()
	stmts := make([]Statement, b.N)
	for i := range stmts {
		sql := `INSERT INTO t0 (id, bucket, val) VALUES `
		for r := 0; r < rowsPerStatement; r++ {
			if r > 0 {
				sql += ","
			}
			id := i*rowsPerStatement + r
			sql += fmt.Sprintf("(%d,%d,'x')", id, id%64)
		}
		stmts[i] = mustParse(sql)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmts[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rowsPerStatement), "ns/row")
}
