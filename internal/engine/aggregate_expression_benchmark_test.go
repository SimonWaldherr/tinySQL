package engine

import (
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"testing"
)

func BenchmarkAggregateExpressions(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("facts", []storage.Column{{Name: "day", Type: storage.TextType}, {Name: "category", Type: storage.IntType}, {Name: "amount", Type: storage.IntType}}, false)
	for i := 0; i < 10000; i++ {
		table.Rows = append(table.Rows, []any{[]string{"2024-01-01", "2024-02-01", "2024-03-01"}[i%3], i % 16, i%100 + 1})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"GroupExpression", `SELECT YEAR(day) AS year, COUNT(*) AS n FROM facts GROUP BY YEAR(day)`},
		{"AggregateRatio", `SELECT category, SUM(amount) / COUNT(*) AS mean FROM facts GROUP BY category`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
