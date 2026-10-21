package engine

import (
	"context"
	"testing"
)

var rawBetweenFilterSink int

func BenchmarkRawBetweenFilter(b *testing.B) {
	rows := make([][]any, 100_000)
	for i := range rows {
		rows[i] = []any{float64(i)}
	}
	columns := map[string]int{"value": 0}
	for _, predicate := range []string{"value BETWEEN 45000 AND 55000", "value NOT BETWEEN 45000 AND 55000"} {
		expr := mustParse("SELECT value FROM t WHERE " + predicate).(*Select).Where
		for _, compiled := range []bool{false, true} {
			name := "SeparateComparisons"
			binary := expr.(*Binary)
			left := buildRawFilterSpecialized(columns, binary.Left)
			right := buildRawFilterSpecialized(columns, binary.Right)
			filter := separateRangeComparisons(left, right, binary.Op == "OR")
			if compiled {
				name = "Compiled"
				filter = buildRawFilter(columns, expr)
			}
			b.Run(predicate+"/"+name, func(b *testing.B) {
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
					rawBetweenFilterSink = matches
				}
			})
		}
	}
}

func separateRangeComparisons(left, right func([]any) (bool, error), outside bool) func([]any) (bool, error) {
	if outside {
		return func(raw []any) (bool, error) {
			match, err := left(raw)
			if err != nil || match {
				return match, err
			}
			return right(raw)
		}
	}
	return func(raw []any) (bool, error) {
		match, err := left(raw)
		if err != nil || !match {
			return false, err
		}
		return right(raw)
	}
}

func BenchmarkBetweenCountScan(b *testing.B) {
	db := setupPerfTable(b, 100_000)
	for _, bound := range []bool{false, true} {
		name := "Literal"
		stmt := mustParse("SELECT COUNT(*) AS n FROM t WHERE id BETWEEN 45000 AND 55000").(*Select)
		if bound {
			name = "Parameter"
			between := stmt.Where.(*Binary)
			between.Left.(*Binary).Right.(*Literal).Parameter = true
			between.Right.(*Binary).Right.(*Literal).Parameter = true
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				result, err := Execute(context.Background(), db, "default", stmt)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0]["n"] != 10001 {
					b.Fatalf("unexpected count: %v", result.Rows)
				}
			}
		})
	}
}
