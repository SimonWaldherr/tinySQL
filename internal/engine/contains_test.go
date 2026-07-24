// Tests for CONTAINS_ALL / CONTAINS_ANY / CONTAINS_SCORE (see fts.go): plain
// case-insensitive substring search over N literal terms, and the raw-path
// WHERE-clause fast filter that recognizes them (see buildRawFilterContains
// in exec.go).
package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestContainsAllRequiresEveryTerm(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'the quick brown fox jumps')`))

	rs := execSQL(t, db, `SELECT CONTAINS_ALL(body, 'quick', 'fox') AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != true {
		t.Errorf("CONTAINS_ALL with every term present = %v, want true", rs.Rows[0]["r"])
	}

	rs = execSQL(t, db, `SELECT CONTAINS_ALL(body, 'quick', 'zebra') AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != false {
		t.Errorf("CONTAINS_ALL with one term missing = %v, want false", rs.Rows[0]["r"])
	}
}

func TestContainsAnyRequiresAtLeastOneTerm(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'the quick brown fox jumps')`))

	rs := execSQL(t, db, `SELECT CONTAINS_ANY(body, 'zebra', 'fox') AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != true {
		t.Errorf("CONTAINS_ANY with one term present = %v, want true", rs.Rows[0]["r"])
	}

	rs = execSQL(t, db, `SELECT CONTAINS_ANY(body, 'zebra', 'giraffe') AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != false {
		t.Errorf("CONTAINS_ANY with no terms present = %v, want false", rs.Rows[0]["r"])
	}
}

// TestContainsCaseInsensitiveMatch guards CONTAINS_ALL/ANY's deliberate
// case-insensitive substring semantics -- both the stored text and the terms
// may be any mix of case, unlike LIKE (case-sensitive by default in this
// engine).
func TestContainsCaseInsensitiveMatch(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'Hello World')`))

	rs := execSQL(t, db, `SELECT CONTAINS_ALL(body, 'hello', 'WORLD') AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != true {
		t.Errorf("mixed-case CONTAINS_ALL('Hello World', 'hello', 'WORLD') = %v, want true", rs.Rows[0]["r"])
	}

	rs = execSQL(t, db, `SELECT CONTAINS_ANY(body, 'HELLO', 'zzz-nomatch') AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != true {
		t.Errorf("mixed-case CONTAINS_ANY('Hello World', 'HELLO', ...) = %v, want true", rs.Rows[0]["r"])
	}
}

// TestContainsNullText guards the documented NULL-input convention: a NULL
// text argument makes CONTAINS_ALL/CONTAINS_ANY return false and
// CONTAINS_SCORE return 0, without erroring, whether the NULL comes from a
// nullable column or a literal NULL.
func TestContainsNullText(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, NULL)`))

	rs := execSQL(t, db, `SELECT CONTAINS_ALL(body, 'a') AS a, CONTAINS_ANY(body, 'a') AS b, CONTAINS_SCORE(body, 'a') AS c FROM t WHERE id = 1`)
	if rs.Rows[0]["a"] != false {
		t.Errorf("CONTAINS_ALL with NULL text (column) = %v, want false", rs.Rows[0]["a"])
	}
	if rs.Rows[0]["b"] != false {
		t.Errorf("CONTAINS_ANY with NULL text (column) = %v, want false", rs.Rows[0]["b"])
	}
	expectInt(t, rs.Rows[0]["c"], 0, "CONTAINS_SCORE with NULL text (column)")

	rs = execSQL(t, db, `SELECT CONTAINS_ALL(NULL, 'a') AS a, CONTAINS_ANY(NULL, 'a') AS b, CONTAINS_SCORE(NULL, 'a') AS c FROM t WHERE id = 1`)
	if rs.Rows[0]["a"] != false {
		t.Errorf("CONTAINS_ALL with literal NULL text = %v, want false", rs.Rows[0]["a"])
	}
	if rs.Rows[0]["b"] != false {
		t.Errorf("CONTAINS_ANY with literal NULL text = %v, want false", rs.Rows[0]["b"])
	}
	expectInt(t, rs.Rows[0]["c"], 0, "CONTAINS_SCORE with literal NULL text")
}

// TestContainsNullTermIsSkippedNotMatched guards evalContainsTerms' handling
// of a NULL term argument (see evalContainsTerms in fts.go): it must never
// error and must never itself count as matched. CONTAINS_SCORE and
// CONTAINS_ANY make that directly observable. CONTAINS_ALL is also exercised:
// per the actual formula (matched == len(ex.Args)-1), a NULL term can never
// be satisfied, so CONTAINS_ALL is false whenever any term argument is NULL
// -- even if every non-NULL term matches -- which is worth pinning down
// explicitly rather than assuming.
func TestContainsNullTermIsSkippedNotMatched(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'hello world')`))

	// A NULL term must not error and must not count toward the score: only
	// the two real terms ('hello', 'world') are found.
	rs := execSQL(t, db, `SELECT CONTAINS_SCORE(body, 'hello', NULL, 'world') AS s FROM t WHERE id = 1`)
	expectInt(t, rs.Rows[0]["s"], 2, "CONTAINS_SCORE with a NULL term mixed in")

	// CONTAINS_ANY: the NULL term itself never matches, but another real term
	// still can.
	rs = execSQL(t, db, `SELECT CONTAINS_ANY(body, NULL, 'zzz-nomatch') AS any1, CONTAINS_ANY(body, NULL, 'world') AS any2 FROM t WHERE id = 1`)
	if rs.Rows[0]["any1"] != false {
		t.Errorf("CONTAINS_ANY(body, NULL, 'zzz-nomatch') = %v, want false", rs.Rows[0]["any1"])
	}
	if rs.Rows[0]["any2"] != true {
		t.Errorf("CONTAINS_ANY(body, NULL, 'world') = %v, want true", rs.Rows[0]["any2"])
	}

	// CONTAINS_ALL: a NULL term can never be "found", so it behaves as an
	// always-failing requirement even though the other term matches.
	rs = execSQL(t, db, `SELECT CONTAINS_ALL(body, 'hello', NULL) AS r FROM t WHERE id = 1`)
	if rs.Rows[0]["r"] != false {
		t.Errorf("CONTAINS_ALL(body, 'hello', NULL) = %v, want false (NULL term can never match)", rs.Rows[0]["r"])
	}
}

// TestContainsScoreOrdersRows guards CONTAINS_SCORE's intended
// "ORDER BY ... DESC" ranking usage, combined with ROW_TO_TEXT() as the
// searched text.
func TestContainsScoreOrdersRows(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, note TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'a b c')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2, 'a b')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (3, 'a')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (4, 'zzz')`))

	rs := execSQL(t, db, `SELECT id, CONTAINS_SCORE(ROW_TO_TEXT(), 'a', 'b', 'c') AS score FROM t ORDER BY score DESC`)
	if len(rs.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	wantIDs := []int{1, 2, 3, 4}
	wantScores := []int{3, 2, 1, 0}
	for i, row := range rs.Rows {
		expectInt(t, row["id"], wantIDs[i], fmt.Sprintf("rank %d id", i+1))
		expectInt(t, row["score"], wantScores[i], fmt.Sprintf("rank %d score", i+1))
	}
}

// TestContainsAllWithRowToTextIsColumnAndOrderIndependent guards the
// ROW_TO_TEXT() + CONTAINS_ALL/ANY idiom this feature is meant to simplify:
// unlike a single LIKE pattern (order- and column-dependent), CONTAINS_ALL
// over ROW_TO_TEXT() finds every term regardless of which column it lands in
// or what order the terms appear in relative to how the query lists them.
func TestContainsAllWithRowToTextIsColumnAndOrderIndependent(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE orders (id INT, sku TEXT, note TEXT)`))
	// Row 1: "urgent" and "widget" both present, but in reversed column/word
	// order relative to how the query lists them.
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (1, 'widget-1', 'urgent request')`))
	// Row 2: only "urgent", missing "widget" anywhere in the row.
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (2, 'gadget-2', 'urgent request')`))
	// Row 3: both terms present in the same column, reversed word order.
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (3, 'n/a', 'widget urgent order')`))

	rs := execSQL(t, db, `SELECT id FROM orders WHERE CONTAINS_ALL(ROW_TO_TEXT(), 'urgent', 'widget')`)
	gotIDs := map[int]bool{}
	for _, row := range rs.Rows {
		n, _ := toInt(row["id"])
		gotIDs[n] = true
	}
	if len(gotIDs) != 2 || !gotIDs[1] || !gotIDs[3] {
		t.Fatalf("CONTAINS_ALL: expected rows 1 and 3 only, got %+v", rs.Rows)
	}

	rs = execSQL(t, db, `SELECT id FROM orders WHERE CONTAINS_ANY(ROW_TO_TEXT(), 'widget', 'zzz-nomatch')`)
	gotIDs = map[int]bool{}
	for _, row := range rs.Rows {
		n, _ := toInt(row["id"])
		gotIDs[n] = true
	}
	if len(gotIDs) != 2 || !gotIDs[1] || !gotIDs[3] {
		t.Fatalf("CONTAINS_ANY: expected rows 1 and 3 only, got %+v", rs.Rows)
	}
}

// TestContainsAllTreatsLikeWildcardsAsLiteral guards an important behavioral
// difference from LIKE: '%' and '_' inside a CONTAINS_ALL/ANY term must be
// matched as literal characters, never as LIKE's any-sequence / single-char
// wildcards.
func TestContainsAllTreatsLikeWildcardsAsLiteral(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'Special offer: 50% off today')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2, 'Special offer: 50 percent off today')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (3, 'value is a_b exactly')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (4, 'value is axb not underscore')`))

	// '%' must be matched as a literal character, not a LIKE any-sequence wildcard.
	rs := execSQL(t, db, `SELECT id FROM t WHERE CONTAINS_ALL(body, '50%') ORDER BY id`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected only row 1 (literal '50%%'), got %+v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 1, "id")

	// '_' must be matched as a literal character, not a LIKE single-char
	// wildcard (which would also match row 4's 'axb').
	rs = execSQL(t, db, `SELECT id FROM t WHERE CONTAINS_ALL(body, 'a_b') ORDER BY id`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected only row 3 (literal 'a_b'), got %+v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 3, "id")
}

// TestContainsAllUsesRawFastPath guards buildRawFilterContains: a WHERE
// clause of the form CONTAINS_ALL(ROW_TO_TEXT(), 'lit1', 'lit2') with literal
// string terms must compile via buildSimpleSelectPlan into a non-nil raw
// filter closure, and that closure -- run directly against raw table rows --
// must agree with running the same query end-to-end via Execute. Mirrors
// TestRowToTextUsesRawFastPathCorrectly in fts_row_search_test.go.
func TestContainsAllUsesRawFastPath(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE orders (id INT, sku TEXT, note TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (1, 'widget-1', 'urgent request')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (2, 'gadget-2', 'urgent request')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (3, 'n/a', 'widget urgent order')`))

	stmt := mustParse(`SELECT id FROM orders WHERE CONTAINS_ALL(ROW_TO_TEXT(), 'urgent', 'widget')`).(*Select)
	plan, ok, err := buildSimpleSelectPlan(ExecEnv{ctx: ctx, tenant: "default", db: db}, stmt)
	if err != nil || !ok {
		t.Fatalf("CONTAINS_ALL raw plan = %#v, ok=%v, err=%v", plan, ok, err)
	}
	if plan.filter == nil {
		t.Fatal("expected a compiled raw filter for CONTAINS_ALL(ROW_TO_TEXT(), ...); fell back to the generic path")
	}

	rs := execSQL(t, db, `SELECT id FROM orders WHERE CONTAINS_ALL(ROW_TO_TEXT(), 'urgent', 'widget')`)
	gotIDs := map[int]bool{}
	for _, row := range rs.Rows {
		n, _ := toInt(row["id"])
		gotIDs[n] = true
	}
	if len(gotIDs) != 2 || !gotIDs[1] || !gotIDs[3] {
		t.Fatalf("Execute: expected rows 1 and 3 only, got %+v", rs.Rows)
	}

	table, err := db.Get("default", "orders")
	if err != nil {
		t.Fatal(err)
	}
	// Row insertion order: (1, ...), (2, ...), (3, ...) -> raw indexes 0,1,2.
	wantMatch := []bool{true, false, true}
	for idx, raw := range table.Rows {
		got, err := plan.filter(raw)
		if err != nil {
			t.Fatalf("plan.filter row %d (raw=%v): %v", idx, raw, err)
		}
		if got != wantMatch[idx] {
			t.Errorf("plan.filter row %d (raw=%v) = %v, want %v", idx, raw, got, wantMatch[idx])
		}
	}
}

// TestContainsAllRawAndGeneralPathsAgree cross-checks buildRawFilterContains
// (the raw fast path, exercised via buildSimpleSelectPlan + plan.filter)
// against evalContainsAll (the general per-row evaluator) on the same set of
// (text, terms) cases, including a literal '%' term and mixed-case text/terms
// -- they must always produce the same boolean result.
func TestContainsAllRawAndGeneralPathsAgree(t *testing.T) {
	cases := []struct {
		text  string
		terms []string
		want  bool
	}{
		{"Hello World", []string{"hello", "world"}, true},
		{"Hello World", []string{"hello", "zzz-nomatch"}, false},
		{"Discount 50% today", []string{"50%"}, true},
		{"Discount 50 today", []string{"50%"}, false},
		{"MiXeD CaSe TeXt", []string{"mixed", "case"}, true},
		{"MiXeD CaSe TeXt", []string{"mixed", "nomatch"}, false},
	}

	for _, c := range cases {
		db := storage.NewDB()
		ctx := context.Background()
		Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, body TEXT)`))
		insertSQL := fmt.Sprintf(`INSERT INTO t VALUES (1, '%s')`, c.text)
		if _, err := Execute(ctx, db, "default", mustParse(insertSQL)); err != nil {
			t.Fatalf("case %+v: insert failed: %v", c, err)
		}

		termLits := make([]string, len(c.terms))
		for i, term := range c.terms {
			termLits[i] = "'" + term + "'"
		}
		sql := fmt.Sprintf(`SELECT id FROM t WHERE CONTAINS_ALL(body, %s)`, strings.Join(termLits, ", "))
		stmt := mustParse(sql).(*Select)
		plan, ok, err := buildSimpleSelectPlan(ExecEnv{ctx: ctx, tenant: "default", db: db}, stmt)
		if err != nil || !ok {
			t.Fatalf("case %+v: raw plan build failed ok=%v err=%v", c, ok, err)
		}
		if plan.filter == nil {
			t.Fatalf("case %+v: expected a compiled raw filter", c)
		}

		table, err := db.Get("default", "t")
		if err != nil {
			t.Fatal(err)
		}
		rawGot, err := plan.filter(table.Rows[0])
		if err != nil {
			t.Fatalf("case %+v: plan.filter error: %v", c, err)
		}

		// General (row-map) evaluator path: call evalContainsAll directly with
		// hand-built literal arguments, bypassing the raw fast path entirely.
		args := []Expr{&Literal{Val: c.text}}
		for _, term := range c.terms {
			args = append(args, &Literal{Val: term})
		}
		fc := &FuncCall{Name: "CONTAINS_ALL", Args: args}
		generalVal, err := evalContainsAll(ExecEnv{}, fc, Row{})
		if err != nil {
			t.Fatalf("case %+v: evalContainsAll error: %v", c, err)
		}
		generalGot, _ := generalVal.(bool)

		if rawGot != c.want {
			t.Errorf("case text=%q terms=%v: raw path = %v, want %v", c.text, c.terms, rawGot, c.want)
		}
		if generalGot != c.want {
			t.Errorf("case text=%q terms=%v: general path = %v, want %v", c.text, c.terms, generalGot, c.want)
		}
	}
}
