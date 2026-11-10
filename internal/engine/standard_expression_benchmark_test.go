package engine

import "testing"

func BenchmarkStandardExpressions(b *testing.B) {
	for _, query := range []struct {
		name, sql string
		rows      int
	}{
		{"Projection", "SELECT id + val * 2 AS computed FROM t", 20000},
		{"Comparison", "SELECT val * 2 + id > 20000 AS matched FROM t", 20000},
		{"WhereCount", "SELECT COUNT(*) AS n FROM t WHERE val * 2 + id > 20000", 1},
	} {
		b.Run(query.name, func(b *testing.B) {
			db := setupPerfTable(b, 20000)
			b.Cleanup(func() { _ = db.Close() })
			statement := mustParse(query.sql)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := Execute(b.Context(), db, "default", statement)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != query.rows {
					b.Fatal("unexpected result length", len(result.Rows))
				}
				if query.name == "WhereCount" && result.Rows[0]["n"] != 14999 {
					b.Fatal("unexpected count", result.Rows)
				}
			}
		})
	}
}
