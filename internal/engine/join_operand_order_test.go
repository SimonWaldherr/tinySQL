package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The hash-join optimizer took JoinCondition.LeftColumn straight from the
// syntactic left operand of `=`, without checking which relation that column
// belongs to. `ON b.id = a.id` therefore probed the LEFT rows for a RIGHT
// column, matched nothing, and returned zero rows for an inner join — silently,
// and only above the row count that engages the optimizer.
//
// These tests assert joined VALUES rather than only row counts. A row-count
// assertion is not enough to catch this on a LEFT JOIN: the broken lookup still
// emits every left row, just padded with NULLs, so the count looks right while
// the data is wrong.

func joinOrderSetup(t *testing.T, n int) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE a (id INT, av TEXT)`)
	execSQL(t, db, `CREATE TABLE b (id INT, bv TEXT)`)
	for i := 0; i < n; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO a VALUES (%d, 'a%d')`, i, i))
		execSQL(t, db, fmt.Sprintf(`INSERT INTO b VALUES (%d, 'b%d')`, i, i))
	}
	return db
}

func mustQuery(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return rs
}

// checkJoinPairs asserts every row pairs av='a<i>' with bv='b<i>' for the same
// i, which is what the equi-join must produce regardless of operand order.
func checkJoinPairs(t *testing.T, rs *ResultSet, want int, label string) {
	t.Helper()
	if len(rs.Rows) != want {
		t.Errorf("%s: got %d rows, want %d", label, len(rs.Rows), want)
		return
	}
	seen := make(map[string]bool, want)
	for _, r := range rs.Rows {
		av, okA := ragValue(r, "av")
		bv, okB := ragValue(r, "bv")
		if !okA || !okB {
			t.Errorf("%s: row missing av/bv: %v", label, r)
			return
		}
		if av == nil || bv == nil {
			t.Errorf("%s: NULL join column, right side did not match: av=%v bv=%v", label, av, bv)
			return
		}
		as, bs := fmt.Sprintf("%v", av), fmt.Sprintf("%v", bv)
		if len(as) < 2 || len(bs) < 2 || as[1:] != bs[1:] {
			t.Errorf("%s: mismatched pair av=%q bv=%q", label, as, bs)
			return
		}
		if seen[as] {
			t.Errorf("%s: duplicate row for %q", label, as)
			return
		}
		seen[as] = true
	}
}

// TestJoinReversedOperandsMatchesForwardOrder covers both sides of the
// hash-join threshold, since the bug is invisible below it.
func TestJoinReversedOperandsMatchesForwardOrder(t *testing.T) {
	for _, n := range []int{10, 500, 501, 900} {
		db := joinOrderSetup(t, n)
		forward := mustQuery(t, db, `SELECT * FROM a JOIN b ON a.id = b.id`)
		checkJoinPairs(t, forward, n, fmt.Sprintf("n=%d forward", n))
		reversed := mustQuery(t, db, `SELECT * FROM a JOIN b ON b.id = a.id`)
		checkJoinPairs(t, reversed, n, fmt.Sprintf("n=%d reversed", n))
	}
}

// TestLeftJoinReversedOperandsKeepsRightValues is the case a row-count-only
// assertion cannot catch: a LEFT JOIN whose right side never matches still
// returns the full left row count, with NULLs where the right columns belong.
func TestLeftJoinReversedOperandsKeepsRightValues(t *testing.T) {
	for _, n := range []int{10, 501, 900} {
		db := joinOrderSetup(t, n)
		rs := mustQuery(t, db, `SELECT * FROM a LEFT JOIN b ON b.id = a.id`)
		checkJoinPairs(t, rs, n, fmt.Sprintf("n=%d left-reversed", n))
	}
}

// TestJoinReversedOperandsPartialOverlap checks the orientation fix does not
// turn a genuine non-match into a match: only the overlapping ids may join.
func TestJoinReversedOperandsPartialOverlap(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE a (id INT, av TEXT)`)
	execSQL(t, db, `CREATE TABLE b (id INT, bv TEXT)`)
	// a: 0..599, b: 300..899 -> 300 overlapping ids, above the threshold.
	for i := 0; i < 600; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO a VALUES (%d, 'a%d')`, i, i))
	}
	for i := 300; i < 900; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO b VALUES (%d, 'b%d')`, i, i))
	}
	forward := mustQuery(t, db, `SELECT * FROM a JOIN b ON a.id = b.id`)
	reversed := mustQuery(t, db, `SELECT * FROM a JOIN b ON b.id = a.id`)
	if len(forward.Rows) != 300 {
		t.Errorf("forward: got %d rows, want 300", len(forward.Rows))
	}
	if len(reversed.Rows) != len(forward.Rows) {
		t.Errorf("reversed returned %d rows, forward returned %d",
			len(reversed.Rows), len(forward.Rows))
	}
	checkJoinPairs(t, reversed, 300, "partial-overlap reversed")
}

// TestOrientJoinConditionUnit pins the orientation helper directly, including
// the indeterminate case that must decline the hash path.
func TestOrientJoinConditionUnit(t *testing.T) {
	left := []Row{{"id": 1, "a.id": 1, "av": "x", "a.av": "x"}}
	right := []Row{{"id": 1, "b.id": 1, "bv": "y", "b.bv": "y"}}

	forward := &JoinCondition{LeftColumn: "a.id", RightColumn: "b.id"}
	if !orientJoinCondition(forward, left, right) {
		t.Fatal("forward orientation must be accepted")
	}
	if forward.LeftColumn != "a.id" || forward.RightColumn != "b.id" {
		t.Errorf("forward orientation must not swap: %+v", forward)
	}

	reversed := &JoinCondition{LeftColumn: "b.id", RightColumn: "a.id"}
	if !orientJoinCondition(reversed, left, right) {
		t.Fatal("reversed orientation must be accepted")
	}
	if reversed.LeftColumn != "a.id" || reversed.RightColumn != "b.id" {
		t.Errorf("reversed orientation must swap to (a.id, b.id): %+v", reversed)
	}

	// Neither name resolves on either side: must decline so the caller uses
	// the nested loop rather than dropping every row.
	unknown := &JoinCondition{LeftColumn: "zz.nope", RightColumn: "qq.nope"}
	if orientJoinCondition(unknown, left, right) {
		t.Error("unresolvable columns must decline the hash path")
	}

	// Empty relation: nothing to orient against, accepted by contract.
	if !orientJoinCondition(&JoinCondition{LeftColumn: "a.id", RightColumn: "b.id"}, nil, right) {
		t.Error("empty left relation must be accepted")
	}
}
