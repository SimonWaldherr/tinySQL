package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Integer arithmetic and SUM over INT columns: the exact integer paths must
// not be slower than the former float64 conversions.
func BenchmarkIntegerArithmetic(b *testing.B) {
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("m", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "a", Type: storage.IntType}, {Name: "f", Type: storage.FloatType}}, false)
	for i := range 20000 {
		table.Rows = append(table.Rows, []any{i, i % 1000, float64(i) / 4})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, bench := range []struct{ name, sql string }{
		{"projection", "SELECT a * 3 + id - 7 AS v FROM m"},
		{"filter", "SELECT id FROM m WHERE a % 7 = 3"},
		{"sum-int-batch", "SELECT SUM(a) AS s, AVG(a) AS avg FROM m"},
		{"sum-float-batch", "SELECT SUM(f) AS s FROM m"},
		{"sum-expr", "SELECT SUM(a * 2) AS s FROM m"},
		{"group-sum", "SELECT a % 10 AS g, SUM(id) AS s FROM m GROUP BY a % 10"},
	} {
		stmt := mustParse(bench.sql)
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
