// Benchmarks for windowed queries (OVER (PARTITION BY ... [ORDER BY ...]))
// at a scale that actually exercises evalWindowFunction's partition-build
// cost across many output rows -- mirrors perf_benchmark_test.go's
// setupPerfTable pattern (same table shape: id/grp/sub/val/note, grp splits
// rows into 50 groups) so results are directly comparable to the other
// engine benchmarks' scale and methodology.
package engine

import "testing"

// BenchmarkWindowRankManyPartitions runs RANK() OVER a table split into 50
// roughly-equal partitions (via setupPerfTable's grp column) -- the common
// windowed-query shape (dashboards, leaderboards-per-group, etc.) and the
// one most exposed to a per-row partition-build cost: N output rows across
// P partitions means the unmemoized implementation rebuilds (filter + sort)
// an O(N/P)-sized partition N times instead of P times.
func BenchmarkWindowRankManyPartitions(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, grp, RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS rk FROM t`)
}

// BenchmarkWindowRowNumberManyPartitions is the same shape with ROW_NUMBER,
// which (unlike RANK) needs no per-row tie-scan of its own -- isolating the
// partition-build cost from evalRankFunction's own O(tie-run) work.
func BenchmarkWindowRowNumberManyPartitions(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn FROM t`)
}

// BenchmarkWindowLagLeadManyPartitions covers the two-window-function-calls-
// per-row case (LAG and LEAD in the same SELECT list, same OVER clause
// shape): each is its own *FuncCall cache entry, so this also demonstrates
// that memoizing per call site doesn't multiply the partition-build cost by
// the number of window functions in the query beyond the expected 2x.
func BenchmarkWindowLagLeadManyPartitions(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, grp,
		LAG(val) OVER (PARTITION BY grp ORDER BY val DESC) AS lg,
		LEAD(val) OVER (PARTITION BY grp ORDER BY val DESC) AS ld
		FROM t`)
}

// BenchmarkWindowRankSinglePartition has no PARTITION BY at all, so the
// "partition" is the entire table -- the worst case for the unmemoized
// implementation (an O(N log N) filter+sort repeated N times instead of
// once), and the case where the fix's benefit is largest.
func BenchmarkWindowRankSinglePartition(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, RANK() OVER (ORDER BY val DESC) AS rk FROM t`)
}
