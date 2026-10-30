package engine

import "testing"

func BenchmarkRawNotColumnFilter(b *testing.B) {
	rows := make([][]any, 10_000)
	values := []any{nil, true, false}
	for i := range rows {
		rows[i] = []any{values[i%len(values)]}
	}
	expr := mustParse("SELECT active FROM t WHERE NOT active").(*Select).Where
	columns := map[string]int{"active": 0}
	for _, compiled := range []bool{false, true} {
		name := "Expression"
		filter := buildRawExprFilter(columns, expr)
		if compiled {
			name = "Compiled"
			filter = buildRawFilter(columns, expr)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				matches := 0
				for _, row := range rows {
					match, err := filter(row)
					if err != nil {
						b.Fatal(err)
					}
					if match {
						matches++
					}
				}
				if matches != len(rows)/3 {
					b.Fatalf("got %d matches, want %d", matches, len(rows)/3)
				}
			}
		})
	}
}
