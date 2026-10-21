package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkAggregateBinary(b *testing.B) {
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("t", []storage.Column{
		{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.IntType},
		{Name: "f", Type: storage.FloatType}, {Name: "g", Type: storage.IntType},
	}, false)
	for i := range 20000 {
		table.Rows = append(table.Rows, []any{i, i % 13, float64(i) / 4, i % 20})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, query string }{
		{"IntegerLiteral", "SELECT SUM(a * 2) FROM t"},
		{"IntegerColumns", "SELECT SUM(a * b), AVG(a + b) FROM t"},
		{"FloatColumns", "SELECT SUM(a * f), AVG(f - b) FROM t"},
		{"Grouped", "SELECT g, SUM(a * b), AVG(a + b) FROM t GROUP BY g"},
		{"DirectControl", "SELECT SUM(a), AVG(f) FROM t"},
		{"NestedControl", "SELECT SUM((a + b) * 2) FROM t"},
	} {
		stmt := mustParse(tc.query)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
