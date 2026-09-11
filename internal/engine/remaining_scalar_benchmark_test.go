package engine

import "testing"

func BenchmarkRemainingScalarPaths(b *testing.B) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"doc": 0, "name": 1, "city": 2}}
	raw := []any{map[string]any{"customer": map[string]any{"name": "Anna"}}, "Anna", "Berlin"}
	for _, sql := range []string{`JSON_GET(doc,'customer.name')`, `CONCAT_WS(';',name,city,'42')`, `TRIM(name)`} {
		b.Run(sql, func(b *testing.B) {
			expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
			b.ReportAllocs()
			for b.Loop() {
				if _, err := evalRawExpr(plan, raw, expr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
