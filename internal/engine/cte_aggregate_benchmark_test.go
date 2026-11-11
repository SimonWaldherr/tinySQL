package engine

import "testing"

func BenchmarkCTEAggregateConsumer(b *testing.B) {
	source := &ResultSet{Cols: []string{"id", "grp", "v"}, Rows: make([]Row, 20000)}
	for i := range source.Rows {
		source.Rows[i] = Row{"id": i, "grp": i % 31, "v": i % 100}
	}
	for _, tc := range []struct{ name, sql string }{
		{"Count", "SELECT COUNT(*) FROM c"},
		{"Numeric", "SELECT SUM(q.v),AVG(q.v),MIN(q.v),MAX(q.v) FROM c q"},
		{"Grouped", "SELECT q.grp,COUNT(*) AS n,SUM(q.v) AS total FROM c q WHERE q.id>=10000 GROUP BY q.grp ORDER BY n DESC"},
		{"ExpressionControl", "SELECT SUM(v+1) FROM c"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql).(*Select)
			env := ExecEnv{ctx: b.Context(), ctes: map[string]*ResultSet{"c": source}}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := executeSelect(env, stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCTEAggregateQuery(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	runBench(b, db, "WITH c AS (SELECT id,grp,val FROM t) SELECT q.grp,COUNT(*),SUM(q.val) FROM c q WHERE q.id>=10000 GROUP BY q.grp")
}
