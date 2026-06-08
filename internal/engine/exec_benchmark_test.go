package engine

import (
	"testing"
)

func BenchmarkEvalVarRefLookup(b *testing.B) {
	row := Row{
		"id":     123,
		"name":   "Alice",
		"score":  42.5,
		"active": true,
	}
	ex := newVarRef("name")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := evalVarRef(ex, row)
		if err != nil {
			b.Fatal(err)
		}
		if v == nil {
			b.Fatal("expected value")
		}
	}
}

func BenchmarkSortRowsOrderBy(b *testing.B) {
	rows := make([]Row, 1024)
	for i := range rows {
		rows[i] = Row{
			"id":    1023 - i,
			"name":  "user",
			"score": i % 17,
		}
	}
	orderBy := []OrderItem{{Col: "id", Desc: false}, {Col: "score", Desc: true}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := sortRows(rows, orderBy)
		if len(sorted) != len(rows) {
			b.Fatal("unexpected sort result size")
		}
	}
}
