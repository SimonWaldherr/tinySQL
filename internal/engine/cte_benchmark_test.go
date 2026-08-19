// Benchmarks for CTE (WITH ...) execution: whether a non-recursive CTE
// referenced multiple times in one query is materialized once (see
// processCTEs/rowsFromCTEResult in exec_cte.go/exec_from.go) rather than
// re-evaluated per reference, and the cost of a recursive CTE's per-iteration
// frontier evaluation at a depth big enough to be measurable.
package engine

import "testing"

// BenchmarkCTESingleReference is the baseline: a CTE aggregating the 20000-row
// perf table, referenced once.
func BenchmarkCTESingleReference(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `
		WITH grouped AS (SELECT grp, COUNT(*) AS n, AVG(val) AS a FROM t GROUP BY grp)
		SELECT grp, n, a FROM grouped ORDER BY grp`)
}

// BenchmarkCTEReferencedTwice self-joins the same CTE against itself. Most
// of its cost is the join itself, not the CTE — but if a non-recursive CTE
// were re-evaluated per reference instead of materialized once, this would
// additionally pay for the 20000-row GROUP BY a second time; comparing this
// benchmark's allocation count against BenchmarkCTESingleReference's over
// time is the useful regression signal, not a specific expected ratio.
func BenchmarkCTEReferencedTwice(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `
		WITH grouped AS (SELECT grp, COUNT(*) AS n, AVG(val) AS a FROM t GROUP BY grp)
		SELECT a.grp, a.n, b.a FROM grouped a JOIN grouped b ON a.grp = b.grp ORDER BY a.grp`)
}

// BenchmarkRecursiveCTEChain measures a recursive CTE's per-iteration cost:
// evalRecursiveCTE (exec_cte.go) tracks only the previous iteration's
// frontier rather than rescanning the whole accumulated result set, so this
// should scale close to linearly with the chain length rather than
// quadratically.
func BenchmarkRecursiveCTEChain(b *testing.B) {
	db := setupPerfTable(b, 0)
	runBench(b, db, `
		WITH RECURSIVE chain AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 AS n FROM chain WHERE n < 500
		)
		SELECT n FROM chain ORDER BY n`)
}
