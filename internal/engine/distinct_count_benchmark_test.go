package engine

import (
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"testing"
)

func BenchmarkDistinctCount(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { db.Close() })
	table := storage.NewTable("dc", []storage.Column{{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.IntType}}, false)
	for i := 0; i < 50000; i++ {
		var v any = i % 1000
		if i%10 == 0 {
			v = nil
		}
		table.Rows = append(table.Rows, []any{i % 100, v})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"distinct", `SELECT DISTINCT v FROM dc`},
		{"count", `SELECT COUNT(DISTINCT v) FROM dc`},
		{"grouped", `SELECT g, COUNT(DISTINCT v) AS n FROM dc GROUP BY g HAVING COUNT(DISTINCT v)>5`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
