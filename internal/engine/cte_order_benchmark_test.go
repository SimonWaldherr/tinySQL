package engine

import (
	"fmt"
	"testing"
)

// Includes materializing the producer; statements and source data are reused.
func BenchmarkCTEOrderedPage(b *testing.B) {
	for _, tail := range []struct{ name, sql string }{
		{"single", "ORDER BY q.val DESC LIMIT 20"},
		{"multiple", "ORDER BY q.grp, q.val DESC LIMIT 20 OFFSET 10"},
		{"filtered", "WHERE q.id >= 10000 ORDER BY q.grp, q.val DESC LIMIT 20"},
		{"large-page", "ORDER BY q.val DESC LIMIT 19000"},
		{"full", "ORDER BY q.val DESC"},
	} {
		b.Run(tail.name, func(b *testing.B) {
			db := setupPerfTable(b, 20000)
			defer db.Close()
			runBench(b, db, "WITH c AS (SELECT id,grp,val FROM t) SELECT q.id AS id,q.grp AS grp FROM c q "+tail.sql)
		})
	}
}

// Isolates consumer selection from producer materialization, with tied keys.
func BenchmarkCTEOrderedConsumer(b *testing.B) {
	rs := &ResultSet{Cols: []string{"id", "grp", "val"}, Rows: make([]Row, 20000)}
	for i := range rs.Rows {
		rs.Rows[i] = Row{"id": i, "grp": i % 31, "val": (i * 7919) % len(rs.Rows)}
	}
	for _, n := range []int{20, 19000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			stmt := mustParse(fmt.Sprintf("SELECT q.id AS id FROM c q ORDER BY q.grp,q.val DESC LIMIT %d", n)).(*Select)
			env := ExecEnv{ctx: b.Context(), ctes: map[string]*ResultSet{"c": rs}}
			b.ReportAllocs()
			for b.Loop() {
				got, err := executeSelect(env, stmt)
				if err != nil || len(got.Rows) != n {
					b.Fatalf("CTE ordered consumer: %v / %v", got, err)
				}
			}
		})
	}
}
