// Tests for the join+aggregate fast path (executeSimpleJoinAggregateFastPath,
// exec_fastpath_join_aggregate.go): an equi-join combined with a
// single-column GROUP BY on a column from either side.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupJoinAggTables(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE orders (id INT, cust_id INT, amount FLOAT)`)
	execSQL(t, db, `CREATE TABLE custs (id INT, name TEXT)`)
	for _, sql := range []string{
		`INSERT INTO custs VALUES (1, 'alice')`,
		`INSERT INTO custs VALUES (2, 'bob')`,
		`INSERT INTO orders VALUES (100, 1, 10.0)`,
		`INSERT INTO orders VALUES (101, 1, 20.0)`,
		`INSERT INTO orders VALUES (102, 2, 5.0)`,
	} {
		execSQL(t, db, sql)
	}
	return db
}

// rowsByKey indexes rows by a column value for order-independent assertions,
// since GROUP BY result order is not guaranteed by SQL semantics and this
// fast path does not attempt to replicate the generic hash-join's
// size-dependent probe/build row order.
func rowsByKey(rows []Row, key string) map[any]Row {
	out := make(map[any]Row, len(rows))
	for _, r := range rows {
		out[r[key]] = r
	}
	return out
}

func TestJoinAggregateFastPathEligibility(t *testing.T) {
	db := setupJoinAggTables(t)
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}

	cases := []struct {
		name   string
		sql    string
		wantOK bool
	}{
		{"group by left equi-join column with count/sum", `SELECT o.cust_id, COUNT(*) AS n, SUM(o.amount) AS total FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id`, true},
		{"group by right side column", `SELECT c.name, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY c.name`, true},
		{"having disqualifies", `SELECT o.cust_id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id HAVING COUNT(*) > 1`, false},
		{"two column group by disqualifies", `SELECT o.cust_id, c.name, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id, c.name`, false},
		{"non-equi join disqualifies", `SELECT o.cust_id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id > c.id GROUP BY o.cust_id`, false},
		{"extra join predicate disqualifies", `SELECT o.cust_id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id AND c.id > 0 GROUP BY o.cust_id`, false},
		{"order by disqualifies", `SELECT o.cust_id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id ORDER BY n`, false},
		{"limit disqualifies", `SELECT o.cust_id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id LIMIT 1`, false},
		{"distinct disqualifies", `SELECT DISTINCT o.cust_id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id`, false},
		{"bare column not matching group key disqualifies", `SELECT o.id, COUNT(*) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id`, false},
		{"count distinct disqualifies", `SELECT o.cust_id, COUNT(DISTINCT o.id) AS n FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id`, false},
		{"no aggregate disqualifies", `SELECT o.cust_id FROM orders o JOIN custs c ON o.cust_id = c.id GROUP BY o.cust_id`, false},
		{"no group by disqualifies (plain join fast path's shape)", `SELECT o.cust_id, o.amount FROM orders o JOIN custs c ON o.cust_id = c.id`, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := mustParse(tc.sql).(*Select)
			_, ok, err := buildSimpleJoinAggregatePlan(env, stmt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("eligibility = %v, want %v for %s", ok, tc.wantOK, tc.sql)
			}
		})
	}
}

func TestJoinAggregateFastPathCountSumAvgMinMax(t *testing.T) {
	db := setupJoinAggTables(t)
	rs := execSQL(t, db, `
		SELECT o.cust_id AS cust_id, COUNT(*) AS n, SUM(o.amount) AS total, AVG(o.amount) AS avg_amt,
		       MIN(o.amount) AS min_amt, MAX(o.amount) AS max_amt
		FROM orders o JOIN custs c ON o.cust_id = c.id
		GROUP BY o.cust_id
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2 groups", rs.Rows)
	}
	byCust := rowsByKey(rs.Rows, "cust_id")

	row1, ok := byCust[1]
	if !ok {
		t.Fatalf("missing group cust_id=1 in %#v", rs.Rows)
	}
	expectInt(t, row1["n"], 2, "cust 1 count")
	expectFloat(t, row1["total"], 30.0, 1e-9, "cust 1 total")
	expectFloat(t, row1["avg_amt"], 15.0, 1e-9, "cust 1 avg")
	expectFloat(t, row1["min_amt"], 10.0, 1e-9, "cust 1 min")
	expectFloat(t, row1["max_amt"], 20.0, 1e-9, "cust 1 max")

	row2, ok := byCust[2]
	if !ok {
		t.Fatalf("missing group cust_id=2 in %#v", rs.Rows)
	}
	expectInt(t, row2["n"], 1, "cust 2 count")
	expectFloat(t, row2["total"], 5.0, 1e-9, "cust 2 total")
}

// TestJoinAggregateFastPathGroupByRightColumn exercises the "group key is a
// column from the right table" half of the narrow shape (left column is
// covered by TestJoinAggregateFastPathCountSumAvgMinMax).
func TestJoinAggregateFastPathGroupByRightColumn(t *testing.T) {
	db := setupJoinAggTables(t)
	rs := execSQL(t, db, `
		SELECT c.name AS name, COUNT(*) AS n, SUM(o.amount) AS total
		FROM orders o JOIN custs c ON o.cust_id = c.id
		GROUP BY c.name
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2 groups", rs.Rows)
	}
	byName := rowsByKey(rs.Rows, "name")
	alice, ok := byName["alice"]
	if !ok {
		t.Fatalf("missing alice group in %#v", rs.Rows)
	}
	expectInt(t, alice["n"], 2, "alice count")
	expectFloat(t, alice["total"], 30.0, 1e-9, "alice total")

	bob, ok := byName["bob"]
	if !ok {
		t.Fatalf("missing bob group in %#v", rs.Rows)
	}
	expectInt(t, bob["n"], 1, "bob count")
	expectFloat(t, bob["total"], 5.0, 1e-9, "bob total")
}

// TestJoinAggregateFastPathWherePushdown confirms the WHERE clause continues
// to filter joined rows before grouping, exactly as the generic path would.
func TestJoinAggregateFastPathWherePushdown(t *testing.T) {
	db := setupJoinAggTables(t)
	rs := execSQL(t, db, `
		SELECT o.cust_id AS cust_id, COUNT(*) AS n
		FROM orders o JOIN custs c ON o.cust_id = c.id
		WHERE o.amount >= 10
		GROUP BY o.cust_id
	`)
	byCust := rowsByKey(rs.Rows, "cust_id")
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %#v, want exactly cust_id=1 (amount>=10 excludes cust 2's only order)", rs.Rows)
	}
	expectInt(t, byCust[1]["n"], 2, "cust 1 count after filter")
}

// TestJoinAggregateFastPathNullJoinKeyNeverMatches is a regression test for a
// pre-change discrepancy this new fast path must NOT reproduce: the
// generic hash-join (HashJoinOptimizer.processHashJoin / getJoinKey in
// optimizations.go) never matches a NULL join key against another NULL join
// key ("if key != nil" skips both build and probe for NULL), so
// "SELECT l.id, COUNT(*) FROM l JOIN r ON l.id = r.id GROUP BY l.id" with
// NULL ids on both sides -- which, before this fast path existed, always ran
// through that generic path since GROUP BY disqualified the plain join fast
// path -- produces no group for the NULL rows. This fast path must match
// that, not the plain join fast path's own (separate, pre-existing, and out
// of scope here) comparableKeyPart map lookup, which treats two NULL keys as
// equal to each other.
func TestJoinAggregateFastPathNullJoinKeyNeverMatches(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE l (id INT, val TEXT)`)
	execSQL(t, db, `CREATE TABLE r (id INT, extra TEXT)`)
	execSQL(t, db, `INSERT INTO l VALUES (1, 'a')`)
	execSQL(t, db, `INSERT INTO l VALUES (NULL, 'b')`)
	execSQL(t, db, `INSERT INTO r VALUES (1, 'x')`)
	execSQL(t, db, `INSERT INTO r VALUES (NULL, 'y')`)

	rs := execSQL(t, db, `SELECT l.id AS id, COUNT(*) AS n FROM l JOIN r ON l.id = r.id GROUP BY l.id`)
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %#v, want exactly one group (NULL id must never match NULL id)", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 1, "only surviving group's id")
	expectInt(t, rs.Rows[0]["n"], 1, "only surviving group's count")
}

// TestJoinAggregateFastPathGroupsNullValuesTogether confirms that once a
// NULL *group* value (as opposed to a NULL *join key*, covered above) is
// produced by a real join match, every row sharing that NULL group value
// still collapses into a single group -- mirroring the plain aggregate fast
// path's writeSingleGroupKey NULL handling.
func TestJoinAggregateFastPathGroupsNullValuesTogether(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE l (id INT, tag TEXT)`)
	execSQL(t, db, `CREATE TABLE r (id INT)`)
	execSQL(t, db, `INSERT INTO l VALUES (1, NULL)`)
	execSQL(t, db, `INSERT INTO l VALUES (1, NULL)`)
	execSQL(t, db, `INSERT INTO l VALUES (1, 'x')`)
	execSQL(t, db, `INSERT INTO r VALUES (1)`)

	rs := execSQL(t, db, `SELECT l.tag AS tag, COUNT(*) AS n FROM l JOIN r ON l.id = r.id GROUP BY l.tag`)
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2 groups (NULL tag and 'x' tag)", rs.Rows)
	}
	byTag := rowsByKey(rs.Rows, "tag")
	expectInt(t, byTag["x"]["n"], 1, "'x' group count")
	nullRow, ok := byTag[nil]
	if !ok {
		t.Fatalf("missing NULL tag group in %#v", rs.Rows)
	}
	expectInt(t, nullRow["n"], 2, "NULL group count")
}

// TestJoinAggregateShapesStillFallThroughToGenericPath exercises query
// shapes that are similar to, but outside of, the narrow fast-path shape
// (multi-column GROUP BY with a join being the case the stage description
// specifically calls out) and confirms they still produce correct results
// via the generic path, unaffected by this change.
func TestJoinAggregateShapesStillFallThroughToGenericPath(t *testing.T) {
	db := setupJoinAggTables(t)
	execSQL(t, db, `INSERT INTO orders VALUES (103, 2, 7.0)`)

	twoColGroupBySQL := `
		SELECT o.cust_id AS cust_id, c.name AS name, COUNT(*) AS n, SUM(o.amount) AS total
		FROM orders o JOIN custs c ON o.cust_id = c.id
		GROUP BY o.cust_id, c.name
	`
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}
	stmt := mustParse(twoColGroupBySQL).(*Select)
	if _, ok, err := buildSimpleJoinAggregatePlan(env, stmt); err != nil || ok {
		t.Fatalf("expected two-column GROUP BY to stay ineligible for the new fast path, ok=%v err=%v", ok, err)
	}

	rs := execSQL(t, db, twoColGroupBySQL)
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2 groups", rs.Rows)
	}
	byCust := rowsByKey(rs.Rows, "cust_id")
	expectInt(t, byCust[1]["n"], 2, "cust 1 count")
	expectFloat(t, byCust[1]["total"], 30.0, 1e-9, "cust 1 total")
	expectInt(t, byCust[2]["n"], 2, "cust 2 count")
	expectFloat(t, byCust[2]["total"], 12.0, 1e-9, "cust 2 total")

	// HAVING with a join also stays on the generic path; confirm it still
	// filters correctly.
	rs2 := execSQL(t, db, `
		SELECT o.cust_id AS cust_id, COUNT(*) AS n
		FROM orders o JOIN custs c ON o.cust_id = c.id
		GROUP BY o.cust_id
		HAVING COUNT(*) > 1
	`)
	if len(rs2.Rows) != 2 {
		t.Fatalf("rows = %#v, want both groups (each now has >1 order after the extra INSERT above)", rs2.Rows)
	}
	byCustHaving := rowsByKey(rs2.Rows, "cust_id")
	expectInt(t, byCustHaving[1]["n"], 2, "having-filtered cust 1 count")
	expectInt(t, byCustHaving[2]["n"], 2, "having-filtered cust 2 count")
}

// TestJoinAggregateFastPathOutputColumnOrder confirms output column order
// follows the SELECT list, matching the generic path's behavior.
func TestJoinAggregateFastPathOutputColumnOrder(t *testing.T) {
	db := setupJoinAggTables(t)
	rs := execSQL(t, db, `
		SELECT SUM(o.amount) AS total, o.cust_id, COUNT(*) AS n
		FROM orders o JOIN custs c ON o.cust_id = c.id
		GROUP BY o.cust_id
	`)
	want := []string{"total", "o.cust_id", "n"}
	if len(rs.Cols) != len(want) {
		t.Fatalf("cols = %#v, want %#v", rs.Cols, want)
	}
	for i, c := range want {
		if rs.Cols[i] != c {
			t.Fatalf("cols[%d] = %q, want %q (cols=%#v)", i, rs.Cols[i], c, rs.Cols)
		}
	}
}
