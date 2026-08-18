// Benchmarks covering execution-engine hotspots: single- and multi-column
// GROUP BY raw paths, ORDER BY at scale, JOIN at both the nested-loop and
// hash-join thresholds, plain table scans (Row map allocation cost), row-wide
// LIKE/REGEXP scans, and FTS_SEARCH repeated-query behavior (which exercises
// the document cache).
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

// BenchmarkGroupByTwoColumns measures the raw aggregate fast path's composite
// group key handling.
func BenchmarkGroupByTwoColumns(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT grp, sub, COUNT(*) as n, AVG(val) as a FROM t GROUP BY grp, sub`)
}

// BenchmarkGroupBySingleColumnFastPath measures the specialized one-column
// variant that can use the grouped value itself as a map key.
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

// BenchmarkGroupByOrderByLimit combines all three clauses in one query. GROUP
// BY plus ORDER BY always takes processAggregateQuery followed by
// applySortOrderWithLimit's top-N heap — the aggregate fast path
// (executeSimpleAggregateFastPath) excludes any query with an ORDER BY, so
// this exact shape never gets the raw-row heap used by BenchmarkOrderByWithLimit.
func BenchmarkGroupByOrderByLimit(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT grp, COUNT(*) as n, AVG(val) as a FROM t GROUP BY grp ORDER BY a DESC LIMIT 10`)
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

// BenchmarkJoinGroupBySingleColumnFastPath is sized identically to
// BenchmarkJoinHashJoinAboveThreshold (so, before this fast path existed, the
// join itself would have used the hash-join path) but adds a single-column
// GROUP BY on the join key. Before executeSimpleJoinAggregateFastPath
// (exec_fastpath_join_aggregate.go), this exact shape always fell through to
// the generic Row-map join (exec_join.go) followed by processAggregateQuery
// (exec_group.go), which buffers every joined row per group and re-scans
// each group's buffer once per aggregate expression -- the redundant re-scan
// the fast path exists to avoid. B/op is the primary signal that re-scan is
// gone: it should be far below BenchmarkJoinHashJoinAboveThreshold's, since
// no per-row Row map or per-group row buffer is ever materialized.
func BenchmarkJoinGroupBySingleColumnFastPath(b *testing.B) {
	db := setupJoinTables(b, 5000, 5000)
	runBench(b, db, `SELECT l.id, COUNT(*) as n FROM l JOIN r ON l.id = r.id GROUP BY l.id`)
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

// BenchmarkSelectProjectedLimitOffset measures unfiltered pagination far into
// a table. The raw scan can slice source rows before materializing maps, so
// only the visible page should be projected.
func BenchmarkSelectProjectedLimitOffset(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, val FROM t LIMIT 20 OFFSET 10000`)
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

// BenchmarkFTSMatchRowScan measures FTS_MATCH used as a per-row WHERE
// predicate with a constant multi-term boolean query — before
// parseCachedFTSQuery (fts_query_cache.go), every row reparsed the identical
// query string from scratch.
func BenchmarkFTSMatchRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE FTS_MATCH(note, 'number OR lorem')`)
}

// BenchmarkFTSRankRowScan measures FTS_RANK used as a per-row ORDER BY
// expression over every row — the same reparse-per-row concern as
// BenchmarkFTSMatchRowScan, on the scoring path instead of the match path.
func BenchmarkFTSRankRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id, FTS_RANK(note, 'number OR lorem') as score FROM t ORDER BY score DESC LIMIT 20`)
}

// BenchmarkRowToTextRowScan measures the ROW_TO_TEXT() ad-hoc whole-row
// search predicate, combined with a plain column condition (the shape that
// previously tripped the buildRawFilter AND-fallback bug).
func BenchmarkRowToTextRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE ROW_TO_TEXT() LIKE '%number 123%' AND grp = 'group-23'`)
}

// BenchmarkRowToTextMultiTermAndRowScan measures the column-independent,
// order-independent multi-term idiom — several ROW_TO_TEXT() LIKE terms
// ANDed together — that buildRawRowToTextAndFilter compiles into a single
// whole-row-text build per row instead of one rebuild per term.
func BenchmarkRowToTextMultiTermAndRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE ROW_TO_TEXT() LIKE '%number%' AND ROW_TO_TEXT() LIKE '%lorem%' AND ROW_TO_TEXT() LIKE '%123%'`)
}

// BenchmarkContainsAllRowScan measures the CONTAINS_ALL(ROW_TO_TEXT(), ...)
// raw fast-path predicate (buildRawFilterContains): a case-insensitive,
// literal-term whole-row search, compiled into a specialized closure instead
// of falling through to the generic per-row function-dispatch path.
func BenchmarkContainsAllRowScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE CONTAINS_ALL(ROW_TO_TEXT(), 'number', 'lorem', '123')`)
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

// BenchmarkUpdateByPrimaryKey measures the common point-update path, including
// statement atomicity. The constraint seek and row-local rollback snapshot
// should keep work independent of the 20k-row table size.
func BenchmarkUpdateByPrimaryKey(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE updates (id INT PRIMARY KEY, score FLOAT, bucket INT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "updates")
	if err != nil {
		b.Fatal(err)
	}
	const rows = 20000
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i++ {
		table.Rows[i] = []any{float64(i), float64(i), float64(i % 64)}
	}
	table.Version++
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE INDEX idx_updates_bucket ON updates(bucket)`)); err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`UPDATE updates SET score = 42 WHERE id = 12345`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeleteByPrimaryKey measures a point delete from a 20k-row table.
// Use -benchtime=1x when comparing the successful-delete latency; subsequent
// iterations intentionally measure the cold negative-lookup scan.
func BenchmarkDeleteByPrimaryKey(b *testing.B) {
	benchmarkDeleteByPrimaryKey(b, true)
}

func BenchmarkDeleteByPrimaryKeyColdConstraintCache(b *testing.B) {
	benchmarkDeleteByPrimaryKey(b, false)
}

func benchmarkDeleteByPrimaryKey(b *testing.B, warmConstraintCache bool) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE deletes (id INT PRIMARY KEY, score FLOAT, bucket INT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "deletes")
	if err != nil {
		b.Fatal(err)
	}
	const rows = 20000
	table.Rows = make([][]any, rows)
	for i := 0; i < rows; i++ {
		table.Rows[i] = []any{float64(i), float64(i), float64(i % 64)}
	}
	table.Version++
	if warmConstraintCache {
		// Direct row seeding bypasses normal INSERT constraint checks, which
		// build this cache incrementally. Warm it to match a real table.
		_ = getConstraintIndex(table, 0)
	}
	stmt := mustParse(`DELETE FROM deletes WHERE id = 12345`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeleteMatchesNothing measures a DELETE whose WHERE clause matches
// zero rows on a 20,000-row table with a secondary index. The raw-predicate
// fast path used to pay a full row-slice copy and a full secondary-index
// rebuild unconditionally, even when nothing matched and neither had
// actually changed; see the `if del > 0` gate in exec_dml_delete.go.
func BenchmarkDeleteMatchesNothing(b *testing.B) {
	db := setupPerfTable(b, 20000)
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE INDEX idx_grp ON t (grp)`)); err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`DELETE FROM t WHERE id = -1`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(ctx, db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if rs.Rows[0]["deleted"] != 0 {
			b.Fatalf("expected 0 deleted, got %v", rs.Rows[0]["deleted"])
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
