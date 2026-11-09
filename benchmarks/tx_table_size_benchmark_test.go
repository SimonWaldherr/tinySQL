package benchmarks

// Transaction cost against table size.
//
// A short BEGIN … COMMIT that touches one row is the most common write shape of
// an application: the cost of that transaction must not depend on how many
// rows the table already holds. The other parity benchmarks seed a fixed
// 10 000-row table, so they cannot show a BEGIN that copies the database — only
// a size sweep can. ns/op and B/op should stay flat as rows grows; if they grow
// linearly, transaction setup is copying something proportional to the data.
//
// Run with:
//
//	go test ./benchmarks/ -run='^$' -bench='ParityTx' -benchmem

import (
	"fmt"
	"strings"
	"testing"
)

// parityTxSizes are the table sizes swept by the transaction benchmarks.
var parityTxSizes = []int{1_000, 10_000, 100_000}

// memParityEngines returns the in-memory tinySQL and SQLite engines: the
// transaction-setup cost under test is CPU and allocation, so the durability
// tiers would only add fsync noise.
func memParityEngines() []parityEngine {
	var out []parityEngine
	for _, e := range parityEngines() {
		if strings.HasSuffix(e.name, "/mem") {
			out = append(out, e)
		}
	}
	return out
}

// BenchmarkParityTxSmallWrite is one transaction that updates a single row by
// primary key.
func BenchmarkParityTxSmallWrite(b *testing.B) {
	for _, rows := range parityTxSizes {
		for _, e := range memParityEngines() {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, e.name), func(b *testing.B) {
				db := e.open(b)
				seedRows(b, db, e, rows)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx, err := db.Begin()
					if err != nil {
						b.Fatalf("begin: %v", err)
					}
					if _, err := tx.Exec(`UPDATE bench SET score = ? WHERE id = ?`, float64(i), i%rows); err != nil {
						_ = tx.Rollback()
						b.Fatalf("update: %v", err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatalf("commit: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkParityTxSmallInsert is one transaction that appends a single row.
func BenchmarkParityTxSmallInsert(b *testing.B) {
	for _, rows := range parityTxSizes {
		for _, e := range memParityEngines() {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, e.name), func(b *testing.B) {
				db := e.open(b)
				seedRows(b, db, e, rows)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx, err := db.Begin()
					if err != nil {
						b.Fatalf("begin: %v", err)
					}
					id := rows + i
					if _, err := tx.Exec(`INSERT INTO bench (id, name, score, bucket) VALUES (?, ?, ?, ?)`,
						id, "row", float64(id), id%64); err != nil {
						_ = tx.Rollback()
						b.Fatalf("insert: %v", err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatalf("commit: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkParityTxReadOnly is one transaction that only looks a row up. A
// transaction that never writes should cost the same as the lookup alone.
func BenchmarkParityTxReadOnly(b *testing.B) {
	for _, rows := range parityTxSizes {
		for _, e := range memParityEngines() {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, e.name), func(b *testing.B) {
				db := e.open(b)
				seedRows(b, db, e, rows)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx, err := db.Begin()
					if err != nil {
						b.Fatalf("begin: %v", err)
					}
					var name string
					var score float64
					if err := tx.QueryRow(`SELECT name, score FROM bench WHERE id = ?`, i%rows).Scan(&name, &score); err != nil {
						_ = tx.Rollback()
						b.Fatalf("lookup: %v", err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatalf("commit: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkParityTxManyTables is the other axis of BEGIN cost: the number of
// tables in the database rather than the number of rows. A transaction that
// reads one row of one table should not pay for the other tables.
func BenchmarkParityTxManyTables(b *testing.B) {
	for _, tables := range []int{1, 50, 500} {
		for _, e := range memParityEngines() {
			b.Run(fmt.Sprintf("tables=%d/%s", tables, e.name), func(b *testing.B) {
				db := e.open(b)
				seedRows(b, db, e, 100)
				for i := 1; i < tables; i++ {
					mustExec(b, db, fmt.Sprintf(`CREATE TABLE extra_%d (id INT PRIMARY KEY, v TEXT)`, i))
					mustExec(b, db, fmt.Sprintf(`INSERT INTO extra_%d VALUES (1, 'x')`, i))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx, err := db.Begin()
					if err != nil {
						b.Fatalf("begin: %v", err)
					}
					var name string
					var score float64
					if err := tx.QueryRow(`SELECT name, score FROM bench WHERE id = ?`, i%100).Scan(&name, &score); err != nil {
						_ = tx.Rollback()
						b.Fatalf("lookup: %v", err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatalf("commit: %v", err)
					}
				}
			})
		}
	}
}
