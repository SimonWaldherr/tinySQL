package engine

import "testing"

func BenchmarkWindowRowNumberUnordered(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, ROW_NUMBER() OVER () AS rn FROM t`)
}
func BenchmarkDistinctOrderedLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT DISTINCT grp FROM t ORDER BY grp DESC LIMIT 10 OFFSET 5`)
}
func BenchmarkFilteredDeepOffset(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, val FROM t WHERE val >= 0 LIMIT 10 OFFSET 15000`)
}
