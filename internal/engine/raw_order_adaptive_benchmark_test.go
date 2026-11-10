package engine

import (
	"fmt"
	"testing"
)

func BenchmarkRawOrderPages(b *testing.B) {
	for _, order := range []struct{ name, sql string }{
		{"Float", "val DESC"},
		{"Integer", "id DESC"},
		{"Multi", "grp, sub, val DESC"},
	} {
		for _, page := range []struct {
			name          string
			limit, offset int
		}{
			{"Small", 20, 0},
			{"Large", 19000, 0},
			{"Deep", 20, 19980},
		} {
			b.Run(order.name+"/"+page.name, func(b *testing.B) {
				db := setupPerfTable(b, 20000)
				b.Cleanup(func() { _ = db.Close() })
				table, err := db.Get("default", "t")
				if err != nil {
					b.Fatal(err)
				}
				for i, row := range table.Rows {
					key := (i * 7919) % len(table.Rows)
					row[0], row[3] = key, float64(key)
				}
				table.Version++
				statement := mustParse(fmt.Sprintf("SELECT id, val FROM t ORDER BY %s LIMIT %d OFFSET %d", order.sql, page.limit, page.offset))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, err := Execute(b.Context(), db, "default", statement)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != page.limit {
						b.Fatal("unexpected page length", len(result.Rows))
					}
				}
			})
		}
	}
}
