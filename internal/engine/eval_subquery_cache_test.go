package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// mustParseSelect parses sql (expected to be a single SELECT) and returns its
// *Select, for tests that want to hand a subquery body to isSelectCorrelated
// directly without going through a full outer statement.
func mustParseSelect(t *testing.T, sql string) *Select {
	t.Helper()
	stmt := mustParse(sql)
	sel, ok := stmt.(*Select)
	if !ok {
		t.Fatalf("expected *Select, got %T", stmt)
	}
	return sel
}

// --- isSelectCorrelated: direct, DB-free unit tests of the lexical check ---

func TestIsSelectCorrelated_UnqualifiedOwnTableIsSafe(t *testing.T) {
	sel := mustParseSelect(t, "SELECT y FROM small_table")
	if isSelectCorrelated(sel) {
		t.Fatalf("expected uncorrelated: subquery only references its own table")
	}
}

func TestIsSelectCorrelated_QualifiedOwnAliasIsSafe(t *testing.T) {
	sel := mustParseSelect(t, "SELECT 1 FROM t2 AS t2 WHERE t2.flag = 1")
	if isSelectCorrelated(sel) {
		t.Fatalf("expected uncorrelated: t2.flag qualifies to the subquery's own FROM alias")
	}
}

func TestIsSelectCorrelated_QualifiedOuterAliasIsCorrelated(t *testing.T) {
	// Mirrors the task's own motivating example:
	// EXISTS (SELECT 1 FROM t2 WHERE t2.parent_id = outer.id).
	sel := mustParseSelect(t, "SELECT 1 FROM t2 WHERE t2.parent_id = outer.id")
	if !isSelectCorrelated(sel) {
		t.Fatalf("expected correlated: outer.id qualifier is not one of the subquery's own FROM/JOIN aliases")
	}
}

func TestIsSelectCorrelated_JoinOnReferencingOuterIsCorrelated(t *testing.T) {
	sel := mustParseSelect(t, "SELECT 1 FROM a JOIN b ON a.id = b.id AND b.x = outer.y")
	if !isSelectCorrelated(sel) {
		t.Fatalf("expected correlated: JOIN ON references outer.y")
	}
}

func TestIsSelectCorrelated_FromSubqueryAliasIsSafe(t *testing.T) {
	sel := mustParseSelect(t, "SELECT d.v FROM (SELECT val AS v FROM inner_t) AS d WHERE d.v > 0")
	if isSelectCorrelated(sel) {
		t.Fatalf("expected uncorrelated: d.v qualifies to the subquery's own derived-table alias")
	}
}

func TestIsSelectCorrelated_SiblingCTECanReferenceEarlierCTE(t *testing.T) {
	sel := mustParseSelect(t, "WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a) SELECT x FROM b")
	if isSelectCorrelated(sel) {
		t.Fatalf("expected uncorrelated: b's body legitimately references the earlier sibling CTE a")
	}
}

func TestIsSelectCorrelated_CTECannotSeeLaterSiblingOrOwnUsingScope(t *testing.T) {
	// "a" is defined before "later" in the WITH list, so a qualified
	// reference to "later" inside a's own body cannot resolve (standard SQL
	// CTE scoping: only earlier siblings are visible). Note "a"'s own FROM
	// (z) is a separate, unrelated source -- what is being checked here is
	// the qualifier "later." on later.x, not the bare table name "later"
	// appearing in a FROM clause (a bare FROM reference alone, with no
	// qualified column reference to it, is never flagged -- see
	// TestIsSelectCorrelated_UnqualifiedNameNeverFlagged).
	sel := mustParseSelect(t, "WITH a AS (SELECT later.x FROM z), later AS (SELECT 1 AS x) SELECT x FROM a")
	if !isSelectCorrelated(sel) {
		t.Fatalf("expected correlated/unresolved: a's body cannot see the later-defined sibling CTE")
	}
}

func TestIsSelectCorrelated_NestedSubSubqueryReferencingGrandOuterIsCorrelated(t *testing.T) {
	// The innermost reference reaches two lexical levels up (skipping its own
	// immediate parent "b"), which selectHasUnresolvedReference must still
	// catch because ownScope starts empty at the top of the analysis and only
	// ever grows by what is actually inside the subquery tree.
	sel := mustParseSelect(t, "SELECT 1 FROM b WHERE EXISTS (SELECT 1 FROM c WHERE c.x = grandouter.y)")
	if !isSelectCorrelated(sel) {
		t.Fatalf("expected correlated: nested EXISTS references grandouter.y")
	}
}

func TestIsSelectCorrelated_ShadowedAliasNameStillResolvesLocally(t *testing.T) {
	// The subquery happens to reuse alias "t" for its own FROM -- if some
	// enclosing query also has an alias "t", this must still be judged safe:
	// t.x here can only ever resolve to *this* subquery's own "t".
	sel := mustParseSelect(t, "SELECT 1 FROM t WHERE t.x > 5")
	if isSelectCorrelated(sel) {
		t.Fatalf("expected uncorrelated: t.x resolves to the subquery's own FROM alias regardless of any same-named outer alias")
	}
}

func TestIsSelectCorrelated_UnionSiblingSeesCTEsNotFromAliases(t *testing.T) {
	safe := mustParseSelect(t, "WITH a AS (SELECT 1 AS x) SELECT x FROM a UNION SELECT x FROM a")
	if isSelectCorrelated(safe) {
		t.Fatalf("expected uncorrelated: both UNION branches reference the shared CTE a")
	}
	unsafe := mustParseSelect(t, "SELECT 1 AS x FROM t1 UNION SELECT 1 FROM t2 WHERE t1.flag = 1")
	if !isSelectCorrelated(unsafe) {
		t.Fatalf("expected correlated: the second UNION branch cannot see the first branch's FROM alias t1")
	}
}

func TestIsSelectCorrelated_UnqualifiedNameNeverFlagged(t *testing.T) {
	// Documents the deliberate policy: an unqualified name is never treated
	// as potentially correlated, because evalVarRef never resolves one
	// against anything but the subquery's own row. See isSelectCorrelated's
	// doc comment.
	sel := mustParseSelect(t, "SELECT 1 FROM t2 WHERE id = 5")
	if isSelectCorrelated(sel) {
		t.Fatalf("expected uncorrelated: unqualified references are never flagged")
	}
}

// zzUnknownExprForTest is a stand-in for a hypothetical future Expr shape
// exprHasUnresolvedReference does not know how to walk. It deliberately does
// not match any *T case in that function's switch (all cases there are
// pointer types), so it must fall through to the default branch.
type zzUnknownExprForTest struct{}

func TestIsSelectCorrelated_UnknownExprShapeFailsSafe(t *testing.T) {
	sel := mustParseSelect(t, "SELECT 1 FROM t2")
	sel.Where = zzUnknownExprForTest{}
	if !isSelectCorrelated(sel) {
		t.Fatalf("expected an unrecognized Expr shape to conservatively count as correlated")
	}
}

// --- evalCachedSubquery: the actual caching mechanism, exec-count proofs ---

func newSubqueryCacheTestEnv(db *storage.DB) ExecEnv {
	return ExecEnv{
		ctx:           context.Background(),
		tenant:        "default",
		db:            db,
		subqueryCache: newSubqueryResultCache(),
	}
}

// TestEvalCachedSubqueryUncorrelatedExecutesOnce proves -- via an explicit
// execution-count assertion, not merely a correct result -- that a provably
// uncorrelated subquery node is executed exactly once no matter how many
// times evalCachedSubquery is called for it within one statement execution.
// A cache that did nothing would still return the right answer on every
// call, just slowly; execCount is what catches that.
func TestEvalCachedSubqueryUncorrelatedExecutesOnce(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE s (y INT)")
	mustExec(t, db, ctx, "INSERT INTO s VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO s VALUES (2)")

	sel := mustParseSelect(t, "SELECT y FROM s WHERE y > 0")
	ex := &SubqueryExpr{Select: sel}
	env := newSubqueryCacheTestEnv(db)

	const outerRows = 25
	for i := 0; i < outerRows; i++ {
		if _, err := evalCachedSubquery(env, ex, ex.Select); err != nil {
			t.Fatalf("evalCachedSubquery call %d failed: %v", i, err)
		}
	}

	entry := env.subqueryCache.entries[ex]
	if entry == nil {
		t.Fatalf("expected a cache entry for the subquery node")
	}
	if entry.correlated {
		t.Fatalf("expected the subquery to be classified as uncorrelated")
	}
	if entry.execCount != 1 {
		t.Fatalf("expected exactly 1 executeSelect call across %d evaluations, got %d", outerRows, entry.execCount)
	}
}

// TestEvalCachedSubqueryCorrelatedReexecutesEveryCall proves the converse:
// a node the lexical check marks correlated bypasses the cache entirely and
// re-executes on every single call.
func TestEvalCachedSubqueryCorrelatedReexecutesEveryCall(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t2 (parent_id INT)")
	mustExec(t, db, ctx, "INSERT INTO t2 VALUES (1)")

	// References outer.id, which is not one of this subquery's own FROM/JOIN
	// aliases -- lexically correlated (and, per this engine's actual column
	// resolution, would error every time it runs -- but that is exactly the
	// point: run it, don't cache it, and confirm it really is re-run).
	sel := mustParseSelect(t, "SELECT 1 FROM t2 WHERE t2.parent_id = outer.id")
	ex := &ExistsExpr{Select: sel}
	env := newSubqueryCacheTestEnv(db)

	const outerRows = 5
	for i := 0; i < outerRows; i++ {
		// Each call is expected to error (unknown column "outer.id") -- that
		// error itself is not what this test is about; only execCount is.
		_, _ = evalCachedSubquery(env, ex, ex.Select)
	}

	entry := env.subqueryCache.entries[ex]
	if entry == nil {
		t.Fatalf("expected a cache entry for the subquery node")
	}
	if !entry.correlated {
		t.Fatalf("expected the subquery to be classified as correlated")
	}
	if entry.execCount != outerRows {
		t.Fatalf("expected %d executeSelect calls (no caching for a correlated node), got %d", outerRows, entry.execCount)
	}
}

// TestSubqueryCacheIntegration_UncorrelatedInSubqueryExecutesOnce is the
// end-to-end version of the exec-count proof above: it drives the real
// executeSelect/applyWhereClause path (exec_join.go) with many outer rows and
// an uncorrelated "x IN (SELECT y FROM small_table)" predicate, and checks
// both that the result is correct AND that the inner SELECT ran exactly
// once -- not once per outer row.
func TestSubqueryCacheIntegration_UncorrelatedInSubqueryExecutesOnce(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE outer_t (x INT)")
	mustExec(t, db, ctx, "CREATE TABLE small_table (y INT)")
	// Exactly one row: evalSubqueryExpr (which the IN (SELECT ...) form
	// reaches through evalExpr's generic *SubqueryExpr dispatch -- see its
	// doc comment) requires a scalar (0- or 1-row) result independently of
	// this cache, a pre-existing constraint this test does not exercise.
	mustExec(t, db, ctx, "INSERT INTO small_table VALUES (4)")

	const outerRows = 40
	for i := 1; i <= outerRows; i++ {
		mustExec(t, db, ctx, fmt.Sprintf("INSERT INTO outer_t VALUES (%d)", i))
	}

	sel := mustParseSelect(t, "SELECT x FROM outer_t WHERE x IN (SELECT y FROM small_table) ORDER BY x")
	env := newSubqueryCacheTestEnv(db)
	rs, err := executeSelect(env, sel)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["x"] != 4 {
		t.Fatalf("unexpected result rows: %#v", rs.Rows)
	}

	if len(env.subqueryCache.entries) != 1 {
		t.Fatalf("expected exactly 1 distinct subquery AST node cached, got %d", len(env.subqueryCache.entries))
	}
	for _, entry := range env.subqueryCache.entries {
		if entry.correlated {
			t.Fatalf("expected the IN (SELECT ...) subquery to be classified as uncorrelated")
		}
		if entry.execCount != 1 {
			t.Fatalf("expected the inner SELECT to run exactly once across %d outer rows, got %d executions", outerRows, entry.execCount)
		}
	}
}

// --- Trigger channel: the one real per-invocation-varying ambient state ---

// TestSubqueryCacheTriggerRowVariesPerFiring proves that a subquery reached
// from inside a trigger's WHEN clause -- and depending on env.triggerRow's
// NEW binding through an *unqualified* bare-column reference, which
// isSelectCorrelated's lexical check alone would judge safe to cache -- is
// still re-evaluated correctly for every firing within a single multi-row
// INSERT (one Execute call, N trigger firings sharing the same
// ExecEnv/subqueryCache). If the EXISTS result were wrongly cached from the
// first firing, every row would get that first row's verdict; ordering the
// non-matching row first makes that failure mode maximally visible (an empty
// log instead of the one correct match).
func TestSubqueryCacheTriggerRowVariesPerFiring(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE parent (id INT)")
	mustExec(t, db, ctx, "CREATE TABLE child (parent_id INT)")
	mustExec(t, db, ctx, "CREATE TABLE matched_log (id INT)")
	mustExec(t, db, ctx, "INSERT INTO child VALUES (2)")
	// "id" inside the EXISTS subquery is unqualified and is not a column of
	// child (child only has parent_id), so it resolves through
	// env.triggerRow's bare-defaults-to-NEW fallback -- a channel the purely
	// lexical isSelectCorrelated cannot see. The CASE wrapper is there only
	// to keep this on the general expression evaluator (evalExpr, which
	// implements the triggerRow fallback): a bare comparison this simple
	// would otherwise run through the raw fast path's evalRawExpr, which has
	// no triggerRow fallback at all -- a separate, pre-existing limitation
	// of that path unrelated to this cache.
	mustExec(t, db, ctx, `CREATE TRIGGER log_matched AFTER INSERT ON parent
		FOR EACH ROW WHEN (EXISTS (SELECT 1 FROM child WHERE parent_id = CASE WHEN 1 = 1 THEN id ELSE 0 END))
		BEGIN
			INSERT INTO matched_log VALUES (id);
		END`)
	// Non-matching row first, matching row second, another non-matching row
	// third -- one multi-row INSERT, one Execute() call, three trigger
	// firings sharing the same ExecEnv/subqueryCache.
	mustExec(t, db, ctx, "INSERT INTO parent VALUES (3), (2), (5)")

	rs, err := Execute(ctx, db, "default", mustParse("SELECT id FROM matched_log ORDER BY id"))
	if err != nil {
		t.Fatalf("SELECT from matched_log failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 2 {
		t.Fatalf("expected exactly one matched_log row (id=2), got %#v", rs.Rows)
	}
}
