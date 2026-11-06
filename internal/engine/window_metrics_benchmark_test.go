package engine

import "testing"

func BenchmarkWindowMetricCount(b *testing.B) {
	db := setupPerfTable(b, 5000)
	b.Cleanup(func() { _ = db.Close() })
	runBench(b, db, `SELECT id, COUNT(val) OVER (ORDER BY id ROWS BETWEEN 999 PRECEDING AND CURRENT ROW) AS n FROM t`)
}

func BenchmarkWindowMetricTimeRange(b *testing.B) {
	db := setupPerfTable(b, 5000)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	for i := range table.Rows {
		table.Rows[i][0] = i
	}
	runBench(b, db, `SELECT id, COUNT(*) OVER (ORDER BY id RANGE BETWEEN 999 PRECEDING AND CURRENT ROW) AS n FROM t`)
}
