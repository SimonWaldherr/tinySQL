package engine

import (
	"testing"
	"time"
)

var rawClockFilterSink int

func BenchmarkRawClockFilter(b *testing.B) {
	rows := make([][]any, 10_000)
	for i := range rows {
		rows[i] = []any{i, "2024-03-05 06:07:08"}
	}
	for _, predicate := range []string{
		"id >= 9990 AND DATE(value) >= '2024-01-01'",
		"DATE(value) >= '2024-01-01' AND id >= 9990",
	} {
		where := mustParse("SELECT id FROM clocks WHERE " + predicate).(*Select).Where
		for _, clock := range []bool{false, true} {
			name := "CachedFilter"
			if clock {
				name = "BoundClock"
			}
			b.Run(predicate+"/"+name, func(b *testing.B) {
				plan := &simpleSelectPlan{colIndex: map[string]int{"id": 0, "value": 1}, where: where}
				plan.filter = buildRawFilter(plan.colIndex, where)
				if clock {
					bindSimplePlanClock(plan, ExecEnv{now: time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)})
				}
				b.ReportAllocs()
				for b.Loop() {
					matched := 0
					for _, row := range rows {
						match, err := plan.filter(row)
						if err != nil {
							b.Fatal(err)
						}
						if match {
							matched++
						}
					}
					rawClockFilterSink = matched
				}
			})
		}
	}
}
