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

// BenchmarkWindowLagLeadManyPartitions covers two window functions in the
// same SELECT list with the same PARTITION BY/ORDER BY shape. They should
// share each partition's filter+sort work, while keeping separate per-row
// LAG/LEAD evaluation.
func BenchmarkWindowLagLeadManyPartitions(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, grp,
		LAG(val) OVER (PARTITION BY grp ORDER BY val DESC) AS lg,
		LEAD(val) OVER (PARTITION BY grp ORDER BY val DESC) AS ld
		FROM t`)
}

// BenchmarkWindowThreeSharedOverManyPartitions extends the shared-shape case
// to a positional, a backward-looking, and a forward-looking window function.
// It catches regressions that only become visible once more than two SELECT
// expressions reference the same partition ordering.
func BenchmarkWindowThreeSharedOverManyPartitions(b *testing.B) {
	db := setupPerfTable(b, 5000)
	runBench(b, db, `SELECT id, grp,
		ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn,
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
