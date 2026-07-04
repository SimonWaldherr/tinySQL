// Benchmarks covering execution-engine hotspots that previously had no
// dedicated coverage: multi-column GROUP BY (the processAggregateQuery path,
// distinct from the single-column aggregate fast path), ORDER BY at scale,
// JOIN at both the nested-loop and hash-join thresholds, plain table scans
// (Row map allocation cost), row-wide LIKE/REGEXP scans, and FTS_SEARCH
// repeated-query behavior (which exercises the document cache).
package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupPerfTable(b *testing.B, rows int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE t (id INT, grp TEXT, sub TEXT, val FLOAT, note TEXT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i++ {
		table.Rows[i] = []any{
			float64(i),
			fmt.Sprintf("group-%d", i%50),
			fmt.Sprintf("sub-%d", i%7),
			float64(i) * 1.5,
			fmt.Sprintf("note number %d lorem ipsum", i),
		}
	}
	table.Version++
	return db
}

func runBench(b *testing.B, db *storage.DB, sql string) {
	b.Helper()
	stmt := mustParse(sql)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(ctx, db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if rs == nil {
			b.Fatal("nil result set")
		}
	}
}

// ─────────────────────────── GROUP BY ──────────────────────────────────────

// BenchmarkGroupByTwoColumns exercises processAggregateQuery directly: the
// single-column aggregate fast path (buildSimpleAggregatePlan) requires
// exactly one GROUP BY column, so two columns forces the general path.
func BenchmarkGroupByTwoColumns(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT grp, sub, COUNT(*) as n, AVG(val) as a FROM t GROUP BY grp, sub`)
}

// BenchmarkGroupBySingleColumnFastPath is the sibling fast-path benchmark,
// kept alongside the general-path one above for direct comparison.
func BenchmarkGroupBySingleColumnFastPath(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT grp, COUNT(*) as n, AVG(val) as a FROM t GROUP BY grp`)
}

// BenchmarkGroupByWithHaving also forces processAggregateQuery (HAVING
// disqualifies the fast path) and adds the HAVING filter cost on top.
func BenchmarkGroupByWithHaving(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT grp, COUNT(*) as n FROM t GROUP BY grp HAVING COUNT(*) > 100`)
}

// ─────────────────────────── ORDER BY ──────────────────────────────────────

func BenchmarkOrderBySingleColumnNoLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, val FROM t ORDER BY val DESC`)
}

func BenchmarkOrderByMultiColumnNoLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, grp, sub, val FROM t ORDER BY grp, sub, val DESC`)
}

func BenchmarkOrderByWithLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, val FROM t ORDER BY val DESC LIMIT 20`)
}

// ─────────────────────────── JOIN ──────────────────────────────────────────

func setupJoinTables(b *testing.B, leftRows, rightRows int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE l (id INT, val TEXT)`)); err != nil {
		b.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE r (id INT, extra TEXT)`)); err != nil {
		b.Fatal(err)
	}
	lt, _ := db.Get("default", "l")
	lt.Rows = make([][]any, leftRows)
	for i := 0; i < leftRows; i++ {
		lt.Rows[i] = []any{float64(i % rightRows), fmt.Sprintf("val-%d", i)}
	}
	lt.Version++
	rt, _ := db.Get("default", "r")
	rt.Rows = make([][]any, rightRows)
	for i := 0; i < rightRows; i++ {
		rt.Rows[i] = []any{float64(i), fmt.Sprintf("extra-%d", i)}
	}
	rt.Version++
	return db
}

// BenchmarkJoinNestedLoopBelowThreshold sizes both sides just under the
// 500-row hash-join cutover (internal/engine/exec.go processInnerJoin), so
// this benchmark specifically measures the nested-loop + mergeRows path.
func BenchmarkJoinNestedLoopBelowThreshold(b *testing.B) {
	db := setupJoinTables(b, 400, 400)
	runBench(b, db, `SELECT l.id, l.val, r.extra FROM l JOIN r ON l.id = r.id`)
}

// BenchmarkJoinHashJoinAboveThreshold exceeds the cutover on both sides,
// exercising HashJoinOptimizer.processHashJoin instead of the nested loop.
func BenchmarkJoinHashJoinAboveThreshold(b *testing.B) {
	db := setupJoinTables(b, 5000, 5000)
	runBench(b, db, `SELECT l.id, l.val, r.extra FROM l JOIN r ON l.id = r.id`)
}

// ─────────────────────────── Row scan / allocation ─────────────────────────

// BenchmarkSelectStarFullScan measures the cost of materializing every row
// of a table into Row maps (rowsFromTable) with no filtering or projection
// pruning — the baseline allocation cost paid by nearly every query.
func BenchmarkSelectStarFullScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT * FROM t`)
}

func BenchmarkSelectProjectedFullScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, val FROM t`)
}

// ─────────────────────────── LIKE / REGEXP scans ───────────────────────────

func BenchmarkLikeRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE note LIKE '%number 123%'`)
}

func BenchmarkRegexpRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE note REGEXP 'number [0-9]{3}0'`)
}

// BenchmarkRowToTextRowScan measures the ROW_TO_TEXT() ad-hoc whole-row
// search predicate, combined with a plain column condition (the shape that
// previously tripped the buildRawFilter AND-fallback bug).
func BenchmarkRowToTextRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE ROW_TO_TEXT() LIKE '%number 123%' AND grp = 'group-23'`)
}

// ─────────────────────────── FTS_SEARCH ────────────────────────────────────

func setupFTSPerfTable(b *testing.B, rows int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE docs (id INT, title TEXT, body TEXT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "docs")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i++ {
		table.Rows[i] = []any{
			float64(i),
			fmt.Sprintf("Document %d", i),
			fmt.Sprintf("the quick brown fox jumps over lazy dog number %d database systems programming", i),
		}
	}
	table.Version++
	return db
}

// BenchmarkFTSSearchRepeated issues the same FTS_SEARCH query repeatedly
// against an unchanged table — the scenario the document cache targets
// (e.g. a live search box re-querying per keystroke). Most iterations should
// hit the cache built on iteration 1.
func BenchmarkFTSSearchRepeated(b *testing.B) {
	db := setupFTSPerfTable(b, 10000)
	runBench(b, db, `SELECT * FROM FTS_SEARCH('docs', 'database programming', 10)`)
}

// ─────────────────────────── Constraint checking ───────────────────────────

// BenchmarkInsertIntoLargePKTable measures single-row INSERT throughput into
// a table with an existing large PRIMARY KEY-constrained dataset — the
// scenario getConstraintIndex targets. Before it, every INSERT paid an O(n)
// scan of the whole table per constraint check; each iteration here should
// now be ~O(1) instead of O(existing row count).
func BenchmarkInsertIntoLargePKTable(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT PRIMARY KEY, val TEXT)`)); err != nil {
		b.Fatal(err)
	}
	const seedRows = 50000
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, seedRows)
	for i := 0; i < seedRows; i++ {
		table.Rows[i] = []any{float64(i), fmt.Sprintf("val-%d", i)}
	}
	table.Version++

	stmts := make([]Statement, b.N)
	for i := 0; i < b.N; i++ {
		stmts[i] = mustParse(fmt.Sprintf(`INSERT INTO t VALUES (%d, 'new-%d')`, seedRows+i, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmts[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFTSSearchColdEachTime forces a cache rebuild every iteration by
// bumping the table version, isolating the tokenization cost the cache
// otherwise amortizes.
func BenchmarkFTSSearchColdEachTime(b *testing.B) {
	db := setupFTSPerfTable(b, 10000)
	table, _ := db.Get("default", "docs")
	stmt := mustParse(`SELECT * FROM FTS_SEARCH('docs', 'database programming', 10)`)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Version++
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}
