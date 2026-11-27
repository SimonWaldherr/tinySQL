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

func BenchmarkWindowMetricTotals(b *testing.B) {
	db := setupPerfTable(b, 5000)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	for i, row := range table.Rows {
		row[0] = i
		row[3] = i%1024 - 512
		if i%23 == 0 {
			row[3] = nil
		}
	}
	for _, tc := range []struct{ name, sql string }{
		{"rows", `SELECT SUM(val) OVER (ORDER BY id ROWS 999 PRECEDING), AVG(val) OVER (ORDER BY id ROWS 999 PRECEDING) FROM t`},
		{"range", `SELECT SUM(val) OVER (ORDER BY id RANGE 999 PRECEDING), AVG(val) OVER (ORDER BY id RANGE 999 PRECEDING) FROM t`},
		{"moving", `SELECT MOVING_SUM(1000, val) OVER (ORDER BY id), MOVING_AVG(1000, val) OVER (ORDER BY id) FROM t`},
	} {
		b.Run(tc.name, func(b *testing.B) { runBench(b, db, tc.sql) })
	}
}
