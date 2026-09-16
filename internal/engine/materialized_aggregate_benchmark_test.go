package engine

import (
	"context"
	"fmt"
	"testing"
)

// Isolate aggregation of rows already produced by a subquery or join. Fixture
// construction and SQL parsing are excluded from the timed section.
func BenchmarkMaterializedAggregate(b *testing.B) {
	for _, tc := range []struct {
		name, sql string
		groups    int
	}{
		{"FewGroups", `SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp`, 50},
		{"UniqueGroups", `SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp`, 20000},
		{"Ungrouped", `SELECT COUNT(*) AS n, SUM(val) AS total FROM t`, 1},
		{"UngroupedHaving", `SELECT COUNT(*) AS n, SUM(val) AS total FROM t HAVING COUNT(*) > 0`, 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rows := make([]Row, 20000)
			for i := range rows {
				rows[i] = Row{"grp": fmt.Sprintf("group-%d", i%tc.groups), "val": float64(i)}
			}
			stmt := mustParse(tc.sql).(*Select)
			env := ExecEnv{ctx: context.Background()}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, _, err := processAggregateQuery(env, stmt, rows)
				if err != nil {
					b.Fatal(err)
				}
				if len(result) != tc.groups {
					b.Fatalf("got %d groups, want %d", len(result), tc.groups)
				}
			}
		})
	}
}

func BenchmarkSubqueryAggregate(b *testing.B) {
	for _, tc := range []struct{ name, sql string }{
		{"Grouped", `SELECT grp, COUNT(*) AS n, SUM(val) AS total FROM (SELECT grp, val FROM t) q GROUP BY grp`},
		{"UngroupedHaving", `SELECT COUNT(*) AS n, SUM(val) AS total FROM (SELECT val FROM t) q HAVING COUNT(*) > 0`},
	} {
		b.Run(tc.name, func(b *testing.B) { runBench(b, setupPerfTable(b, 20000), tc.sql) })
	}
}
