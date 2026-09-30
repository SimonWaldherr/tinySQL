package driver

import (
	"database/sql"
	"fmt"
	"testing"
)

// BenchmarkTxSmallWrite measures the fixed cost of one small read-write
// transaction (BEGIN, one write, COMMIT or ROLLBACK) against a fixed-size table.
//
// A transaction runs on a private shadow of the database (see SnapshotForTx),
// so this is the benchmark that exposes any per-BEGIN cost proportional to the
// table: the row clone, the cloned PRIMARY KEY index, and the first append
// reallocating the row slice. The table size is fixed per sub-benchmark and
// the UPDATE/INSERT sub-benchmarks never grow it, so ns/op does not drift
// with b.N the way a growing-table benchmark does.
func BenchmarkTxSmallWrite(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("Update/rows=%d", n), func(b *testing.B) {
			db := openTxBenchDB(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := tx.Exec(`UPDATE bench SET score = ? WHERE id = ?`, float64(i), i%n); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("InsertRollback/rows=%d", n), func(b *testing.B) {
			db := openTxBenchDB(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				// Roll back the inserted row so every iteration snapshots exactly n rows.
				if _, err := tx.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, 'x', 1.0, 1)`, n+i); err != nil {
					b.Fatal(err)
				}
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("InsertDelete/rows=%d", n), func(b *testing.B) {
			db := openTxBenchDB(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				id := n + i
				if _, err := tx.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, 'x', 1.0, 1)`, id); err != nil {
					b.Fatal(err)
				}
				if _, err := tx.Exec(`DELETE FROM bench WHERE id = ?`, id); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("ReadOnlyTx/rows=%d", n), func(b *testing.B) {
			db := openTxBenchDB(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.BeginTx(b.Context(), &sql.TxOptions{ReadOnly: true})
				if err != nil {
					b.Fatal(err)
				}
				var name string
				if err := tx.QueryRow(`SELECT name FROM bench WHERE id = ?`, i%n).Scan(&name); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func openTxBenchDB(b *testing.B, rows int) *sql.DB {
	b.Helper()
	db, err := sql.Open("tinysql", fmt.Sprintf("mem://?tenant=tx_bench_%d_%s", rows, b.Name()))
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, score FLOAT, bucket INT)`); err != nil {
		b.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := tx.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, 'row', ?, ?)`, i, float64(i), i%64); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db
}
