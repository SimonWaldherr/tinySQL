package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// setupOrderKeyTable builds a table whose INT and TEXT columns hold the Go
// types the INSERT path stores (int and string), in insertion order. The
// float-typed fixtures in perf_benchmark_test.go take the unboxed float ORDER BY
// path; these exercise the ordering of ordinary INT and TEXT keys.
func setupOrderKeyTable(b *testing.B, rows int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	if _, err := Execute(b.Context(), db, "default", mustParse(
		`CREATE TABLE u (id INT, name TEXT, age INT, score FLOAT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "u")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i++ {
		table.Rows[i] = []any{
			i,
			fmt.Sprintf("name-%06d", (i*7919)%rows),
			18 + (i*31)%60,
			float64(i%1000) * 0.5,
		}
	}
	table.Version++
	return db
}

// BenchmarkOrderIntKeyLatestLimit is the "newest N rows" shape: ids ascend with
// insertion, so every candidate displaces the bounded heap's current worst item.
func BenchmarkOrderIntKeyLatestLimit(b *testing.B) {
	db := setupOrderKeyTable(b, 20000)
	runBench(b, db, `SELECT id, name FROM u ORDER BY id DESC LIMIT 20`)
}

// BenchmarkOrderIntKeyMultiLimit orders by an INT key with duplicates, then by
// a second INT key, so ties fall through to the composite comparator.
func BenchmarkOrderIntKeyMultiLimit(b *testing.B) {
	db := setupOrderKeyTable(b, 20000)
	runBench(b, db, `SELECT id, name FROM u ORDER BY age, id DESC LIMIT 20`)
}

// BenchmarkOrderIntKeyFullSort sorts every row by a duplicate-heavy INT key.
func BenchmarkOrderIntKeyFullSort(b *testing.B) {
	db := setupOrderKeyTable(b, 20000)
	runBench(b, db, `SELECT id, name FROM u ORDER BY age`)
}

// BenchmarkOrderTextKeyLimit orders by a TEXT key with a bounded heap.
func BenchmarkOrderTextKeyLimit(b *testing.B) {
	db := setupOrderKeyTable(b, 20000)
	runBench(b, db, `SELECT id, name FROM u ORDER BY name LIMIT 20`)
}

// BenchmarkOrderTextKeyFullSort sorts every row by a TEXT key.
func BenchmarkOrderTextKeyFullSort(b *testing.B) {
	db := setupOrderKeyTable(b, 20000)
	runBench(b, db, `SELECT id, name FROM u ORDER BY name`)
}

// BenchmarkOrderMultiKeyFullSort sorts every row by an INT key and then a TEXT
// key in descending order, so each candidate takes the encoded multi-column path.
func BenchmarkOrderMultiKeyFullSort(b *testing.B) {
	db := setupOrderKeyTable(b, 20000)
	runBench(b, db, `SELECT id, name FROM u ORDER BY age, name DESC`)
}
