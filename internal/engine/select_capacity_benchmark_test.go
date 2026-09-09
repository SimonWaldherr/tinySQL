package engine

import "testing"

func BenchmarkCTEQualifiedProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	runBench(b, db, `WITH c AS (SELECT id, grp, val FROM t) SELECT q.id, q.grp FROM c q WHERE id >= 10000 LIMIT 20`)
}

func BenchmarkSelectIndexedSmallResult(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	table, _ := db.Get("default", "t")
	for i := range table.Rows {
		table.Rows[i][0] = i
	}
	if _, err := Execute(b.Context(), db, "default", mustParse("CREATE INDEX idx_select_id ON t(id)")); err != nil {
		b.Fatal(err)
	}
	runBench(b, db, `SELECT id, val FROM t WHERE id = 10000 LIMIT 10000`)
}

func BenchmarkSelectSparseLargeLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	runBench(b, db, `SELECT id, val FROM t WHERE id = 10000 LIMIT 10000`)
}

func BenchmarkSelectDenseLargeLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	runBench(b, db, `SELECT id, val FROM t WHERE id >= 0 LIMIT 10000`)
}
