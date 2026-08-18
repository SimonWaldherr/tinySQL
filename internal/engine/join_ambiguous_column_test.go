// Regression tests for three related JOIN correctness bugs found while
// auditing the engine for further optimization opportunities, all sharing
// one root cause: an "unmatched side gets NULL" or "bare-name output" step
// that overwrote a real, already-correct column value instead of only
// filling in genuinely missing ones.
//
//  1. addRightNulls (row_helpers.go), called by processLeftJoin's >500-row
//     hash-join branch on every result row (not just unmatched ones),
//     unconditionally nulled the right table's qualified columns
//     ("b.col") — silently discarding a real match's values.
//  2. processRightJoin/processFullOuterJoin's inline left-column nulling
//     for an unmatched right row unconditionally nulled every key from a
//     sample left row, including the unqualified name the right row
//     already carries its own real value under when the two tables share
//     a column name.
//  3. processNonAggregateQuery and processAggregateQuery's `SELECT *`
//     expansion derived a joined row's *unqualified* column value (e.g.
//     "id" for both "a.id" and "b.id") by ranging over the row's map —
//     whose iteration order Go deliberately randomizes — so two tables
//     sharing a column name produced a different, non-deterministic
//     answer for that bare column on every run of the identical query.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupAmbiguousColumnTables(t *testing.T, aRows, bRows int, bStep int) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE a (id INT, val TEXT)`)
	execSQL(t, db, `CREATE TABLE b (id INT, name TEXT)`)
	ctx := context.Background()
	for i := 0; i < aRows; i++ {
		insertRow(t, db, ctx, "a", i, "a"+ambigItoa(i))
	}
	for i := 0; i < bRows; i += bStep {
		if i >= aRows {
			break
		}
		insertRow(t, db, ctx, "b", i, "b"+ambigItoa(i))
	}
	return db
}

func insertRow(t *testing.T, db *storage.DB, ctx context.Context, table string, id int, text string) {
	t.Helper()
	col := "val"
	if table == "b" {
		col = "name"
	}
	_, err := Execute(ctx, db, "default", mustParse(
		"INSERT INTO "+table+" VALUES ("+ambigItoa(id)+", '"+text+"')"))
	_ = col
	if err != nil {
		t.Fatalf("insert into %s: %v", table, err)
	}
}

func ambigItoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestLeftJoinLargeDoesNotNullMatchedRightColumns is bug #1: a matched row,
// produced by the >500-row HashJoinOptimizer path, must keep its real
// right-side values under their qualified names, not have them overwritten
// to NULL by the subsequent addRightNulls pass.
func TestLeftJoinLargeDoesNotNullMatchedRightColumns(t *testing.T) {
	db := setupAmbiguousColumnTables(t, 600, 600, 2) // b has every even id, forcing >500 on both sides
	rs := execSQL(t, db, `SELECT a.id, b.id AS bid, b.name AS bname
		FROM a LEFT JOIN b ON a.id = b.id
		WHERE a.id = 4`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row for a.id=4, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	row := rs.Rows[0]
	if row["bid"] != 4 {
		t.Errorf("matched row: expected bid=4, got %v (addRightNulls clobbered a real match)", row["bid"])
	}
	if row["bname"] != "b4" {
		t.Errorf("matched row: expected bname=\"b4\", got %v (addRightNulls clobbered a real match)", row["bname"])
	}
}

// TestLeftJoinLargeUnmatchedStillGetsNullRight guards the other half of the
// same fix: a genuinely unmatched row must still see NULL for the right
// side, not its own stale non-nil placeholder.
func TestLeftJoinLargeUnmatchedStillGetsNullRight(t *testing.T) {
	db := setupAmbiguousColumnTables(t, 600, 600, 2) // odd ids have no match in b
	rs := execSQL(t, db, `SELECT a.id, b.id AS bid, b.name AS bname
		FROM a LEFT JOIN b ON a.id = b.id
		WHERE a.id = 5`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row for a.id=5, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	row := rs.Rows[0]
	if row["bid"] != nil {
		t.Errorf("unmatched row: expected bid=NULL, got %v", row["bid"])
	}
	if row["bname"] != nil {
		t.Errorf("unmatched row: expected bname=NULL, got %v", row["bname"])
	}
}

// TestSelectStarLeftJoinAmbiguousColumnIsDeterministic is bug #3 on the
// non-aggregate path: a and b both have an "id" column. Every one of many
// repeated, identical queries must resolve the bare "id" the same way.
// Before the fix this depended on Go's randomized map iteration order and
// could return a different, wrong answer on any given run.
func TestSelectStarLeftJoinAmbiguousColumnIsDeterministic(t *testing.T) {
	db := setupAmbiguousColumnTables(t, 4, 4, 2) // a: 0,1,2,3; b: 0,2 (small -- nested-loop fallback)
	for i := 0; i < 200; i++ {
		rs := execSQL(t, db, `SELECT * FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.val`)
		for _, row := range rs.Rows {
			wantMatch := row["val"] == "a0" || row["val"] == "a2"
			if wantMatch {
				if row["id"] == nil {
					t.Fatalf("run %d: matched row %+v has nil unqualified id; want a's real id", i, row)
				}
				if row["name"] == nil {
					t.Fatalf("run %d: matched row %+v has nil unqualified name; want b's real name", i, row)
				}
			} else {
				if row["id"] == nil {
					t.Fatalf("run %d: unmatched row %+v has nil unqualified id; want a's own real id to survive", i, row)
				}
				if row["name"] != nil {
					t.Fatalf("run %d: unmatched row %+v has non-nil name; want nil (no b match)", i, row)
				}
			}
		}
	}
}

// TestSelectStarGroupByJoinAmbiguousColumnIsDeterministic is the same bug
// #3, on the GROUP BY / aggregate path (processAggregateQuery), which had
// its own, separate copy of the same buggy loop.
func TestSelectStarGroupByJoinAmbiguousColumnIsDeterministic(t *testing.T) {
	db := setupAmbiguousColumnTables(t, 4, 4, 2)
	for i := 0; i < 200; i++ {
		rs := execSQL(t, db, `SELECT * FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.id, a.val, b.id, b.name ORDER BY a.val`)
		for _, row := range rs.Rows {
			wantMatch := row["val"] == "a0" || row["val"] == "a2"
			if wantMatch && row["id"] == nil {
				t.Fatalf("run %d: matched grouped row %+v has nil unqualified id", i, row)
			}
			if !wantMatch && row["id"] == nil {
				t.Fatalf("run %d: unmatched grouped row %+v has nil unqualified id; want a's own id to survive", i, row)
			}
		}
	}
}

// TestRightJoinUnmatchedKeepsOwnUnqualifiedColumn is bug #2: an unmatched
// right row must keep its own real value under the shared unqualified name,
// not have it blanked by the left-side null-fill (which nulls every key
// from a sample left row, and "id" is one of those keys purely because the
// left table happens to share that column name).
func TestRightJoinUnmatchedKeepsOwnUnqualifiedColumn(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE a (id INT, val TEXT)`)
	execSQL(t, db, `CREATE TABLE b (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO a VALUES (1, 'a1')`)
	execSQL(t, db, `INSERT INTO b VALUES (1, 'b1')`)
	execSQL(t, db, `INSERT INTO b VALUES (2, 'b2')`)

	rs := execSQL(t, db, `SELECT * FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.name`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	unmatched := rs.Rows[1]
	if unmatched["name"] != "b2" {
		t.Fatalf("expected second row to be b2, got %+v", unmatched)
	}
	if unmatched["id"] == nil {
		t.Errorf("unmatched right row: expected unqualified id to keep b's own value (2), got nil")
	}
	if unmatched["val"] != nil {
		t.Errorf("unmatched right row: expected val=NULL (no left match), got %v", unmatched["val"])
	}
}

// TestFullOuterJoinUnmatchedRightKeepsOwnUnqualifiedColumn is the same bug
// #2 via processFullOuterJoin's right-only pass.
func TestFullOuterJoinUnmatchedRightKeepsOwnUnqualifiedColumn(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE a (id INT, val TEXT)`)
	execSQL(t, db, `CREATE TABLE b (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO a VALUES (1, 'a1')`)
	execSQL(t, db, `INSERT INTO b VALUES (1, 'b1')`)
	execSQL(t, db, `INSERT INTO b VALUES (2, 'b2')`)

	rs := execSQL(t, db, `SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id ORDER BY b.name`)
	var unmatchedRight Row
	found := false
	for _, r := range rs.Rows {
		if r["name"] == "b2" {
			unmatchedRight = r
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an unmatched b2 row, got: %+v", rs.Rows)
	}
	if unmatchedRight["id"] == nil {
		t.Errorf("unmatched right row (FULL OUTER): expected unqualified id to keep b's own value (2), got nil")
	}
}
