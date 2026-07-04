package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"

	_ "modernc.org/sqlite"
)

// ═══════════════════════════════════════════════════════════════════════════
// Query-shape benchmarks: JOIN and GROUP BY aggregate, tinySQL vs SQLite.
//
// Unlike the storage benchmarks in storage_benchmark_test.go (which mostly
// stress the storage backend), these stress the query engine itself: join
// evaluation and grouped aggregation. Data is loaded once per sub-benchmark;
// only the query itself is timed.
// ═══════════════════════════════════════════════════════════════════════════

type queryOps struct {
	populate func(nCustomers, nOrdersPerCustomer int)
	query    func() int // returns result row count, for correctness checks
	close    func()
}

// ── tinySQL ────────────────────────────────────────────────────────────────

func openTinyQuery(mode storage.StorageMode, sql_ string) func(b *testing.B) queryOps {
	return func(b *testing.B) queryOps {
		b.Helper()
		dir := tmpDir(b)
		cfg := storage.DefaultStorageConfig(mode)
		cfg.Path = filepath.Join(dir, "bench.db")
		db, err := tinysql.OpenDB(cfg)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		exec := func(q string) {
			stmt, err := tinysql.ParseSQL(q)
			if err != nil {
				b.Fatalf("parse %q: %v", q, err)
			}
			if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
				b.Fatalf("exec %q: %v", q, err)
			}
		}
		return queryOps{
			populate: func(nCustomers, nOrdersPerCustomer int) {
				exec("CREATE TABLE customers (id INT, name STRING)")
				exec("CREATE TABLE orders (id INT, customer_id INT, amount FLOAT64)")
				for c := 0; c < nCustomers; c++ {
					exec(fmt.Sprintf("INSERT INTO customers VALUES (%d, 'cust_%d')", c, c))
				}
				oid := 0
				for c := 0; c < nCustomers; c++ {
					for o := 0; o < nOrdersPerCustomer; o++ {
						exec(fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %f)", oid, c, float64(oid)*3.14))
						oid++
					}
				}
			},
			query: func() int {
				stmt, err := tinysql.ParseSQL(sql_)
				if err != nil {
					b.Fatalf("parse query: %v", err)
				}
				rs, err := tinysql.Execute(ctx, db, "default", stmt)
				if err != nil {
					b.Fatalf("exec query: %v", err)
				}
				if rs == nil {
					return 0
				}
				return len(rs.Rows)
			},
			close: func() { db.Close() },
		}
	}
}

// ── SQLite via modernc (pure Go) ────────────────────────────────────────────

func openSQLiteQuery(sql_ string) func(b *testing.B) queryOps {
	return func(b *testing.B) queryOps {
		b.Helper()
		dir := tmpDir(b)
		dbPath := filepath.Join(dir, "bench.sqlite3")
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			b.Fatal(err)
		}
		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("PRAGMA synchronous=NORMAL")

		return queryOps{
			populate: func(nCustomers, nOrdersPerCustomer int) {
				db.Exec("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
				db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")

				tx, _ := db.Begin()
				cstmt, _ := tx.Prepare("INSERT INTO customers VALUES (?,?)")
				for c := 0; c < nCustomers; c++ {
					cstmt.Exec(c, fmt.Sprintf("cust_%d", c))
				}
				cstmt.Close()

				ostmt, _ := tx.Prepare("INSERT INTO orders VALUES (?,?,?)")
				oid := 0
				for c := 0; c < nCustomers; c++ {
					for o := 0; o < nOrdersPerCustomer; o++ {
						ostmt.Exec(oid, c, float64(oid)*3.14)
						oid++
					}
				}
				ostmt.Close()
				tx.Commit()
			},
			query: func() int {
				rows, err := db.Query(sql_)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				defer rows.Close()
				n := 0
				cols, _ := rows.Columns()
				dest := make([]any, len(cols))
				scanArgs := make([]any, len(cols))
				for i := range dest {
					scanArgs[i] = &dest[i]
				}
				for rows.Next() {
					rows.Scan(scanArgs...)
					n++
				}
				return n
			},
			close: func() { db.Close() },
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: Join — INNER JOIN customers/orders, project a few columns.
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkJoin(b *testing.B) {
	const joinSQLTiny = "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id"
	const joinSQLSQLite = "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id"

	sizes := []struct{ customers, perCustomer int }{
		{10, 5},   // 50 orders
		{50, 10},  // 500 orders
		{100, 20}, // 2000 orders
	}

	for _, sz := range sizes {
		label := fmt.Sprintf("customers=%d,orders=%d", sz.customers, sz.customers*sz.perCustomer)
		for _, be := range []struct {
			name string
			open func(b *testing.B) queryOps
		}{
			{"tinySQL-Memory", openTinyQuery(storage.ModeMemory, joinSQLTiny)},
			{"tinySQL-Disk", openTinyQuery(storage.ModeDisk, joinSQLTiny)},
			{"SQLite-modernc", openSQLiteQuery(joinSQLSQLite)},
		} {
			b.Run(fmt.Sprintf("%s/%s", be.name, label), func(b *testing.B) {
				ops := be.open(b)
				defer ops.close()
				ops.populate(sz.customers, sz.perCustomer)

				want := sz.customers * sz.perCustomer

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					n := ops.query()
					if n != want {
						b.Fatalf("expected %d joined rows, got %d", want, n)
					}
				}
			})
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: Aggregate — GROUP BY customer_id with COUNT/SUM.
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkAggregate(b *testing.B) {
	const aggSQL = "SELECT customer_id, COUNT(*), SUM(amount) FROM orders GROUP BY customer_id"

	sizes := []struct{ customers, perCustomer int }{
		{10, 5},
		{50, 10},
		{100, 20},
	}

	for _, sz := range sizes {
		label := fmt.Sprintf("customers=%d,orders=%d", sz.customers, sz.customers*sz.perCustomer)
		for _, be := range []struct {
			name string
			open func(b *testing.B) queryOps
		}{
			{"tinySQL-Memory", openTinyQuery(storage.ModeMemory, aggSQL)},
			{"tinySQL-Disk", openTinyQuery(storage.ModeDisk, aggSQL)},
			{"SQLite-modernc", openSQLiteQuery(aggSQL)},
		} {
			b.Run(fmt.Sprintf("%s/%s", be.name, label), func(b *testing.B) {
				ops := be.open(b)
				defer ops.close()
				ops.populate(sz.customers, sz.perCustomer)

				want := sz.customers

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					n := ops.query()
					if n != want {
						b.Fatalf("expected %d grouped rows, got %d", want, n)
					}
				}
			})
		}
	}
}
