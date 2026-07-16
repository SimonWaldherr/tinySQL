// Regression tests for three-valued (NULL-aware) SQL logic under NOT,
// NOT LIKE, NOT IN, and NOT REGEXP. A prior bug collapsed a NULL-involving
// comparison to a definite false in several fast-path and general-evaluator
// code paths; negating that already-collapsed false made rows with a NULL
// operand wrongly satisfy the negated predicate (NOT(unknown) must stay
// unknown/excluded, not become true). Each test below exercises both a
// query shape that is eligible for the raw fast path and one that is forced
// through the general Row-map evaluator (via DISTINCT, which disqualifies
// simpleSelectEligible), so both code paths are held to the same standard.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupThreeValuedLogicTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, v INT, s TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 5, 'abc')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2, NULL, NULL)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (3, 10, 'xyz')`))
	return db
}

func idSet(rows []Row) map[int]bool {
	out := map[int]bool{}
	for _, r := range rows {
		switch v := r["id"].(type) {
		case int:
			out[v] = true
		case int64:
			out[int(v)] = true
		case float64:
			out[int(v)] = true
		}
	}
	return out
}

func TestNotEqualsExcludesNullRowFastPath(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT id FROM t WHERE NOT (v = 5)`)
	got := idSet(rs.Rows)
	if want := (map[int]bool{3: true}); len(got) != len(want) || !got[3] {
		t.Fatalf("fast path: WHERE NOT (v = 5) = %v, want only id=3 (NULL row must stay excluded)", got)
	}
}

func TestNotEqualsExcludesNullRowGeneralPath(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	// DISTINCT disqualifies the raw fast path, forcing applyWhereClause's
	// general Row-map evaluator — this must agree with the fast path.
	rs := execSQL(t, db, `SELECT DISTINCT id FROM t WHERE NOT (v = 5)`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("general path: WHERE NOT (v = 5) = %v, want only id=3", got)
	}
}

func TestNotEqualsInAggregateFastPathExcludesNullRow(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT COUNT(*) AS c FROM t WHERE NOT (v = 5)`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["c"], 1, "COUNT(*) WHERE NOT (v = 5)")
}

func TestNotLikeExcludesNullRowFastPath(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT id FROM t WHERE NOT (s LIKE 'a%')`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("fast path: WHERE NOT (s LIKE 'a%%') = %v, want only id=3 (NULL row must stay excluded)", got)
	}
}

func TestNotLikeExcludesNullRowGeneralPath(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT DISTINCT id FROM t WHERE NOT (s LIKE 'a%')`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("general path: WHERE NOT (s LIKE 'a%%') = %v, want only id=3", got)
	}
}

func TestDirectNotLikeOperatorExcludesNullRow(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	// The "x NOT LIKE 'pattern'" spelling (LikeExpr.Negate) hits the same
	// early NULL short-circuit as the Unary-NOT-wrapped spelling above.
	rs := execSQL(t, db, `SELECT id FROM t WHERE s NOT LIKE 'a%'`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("WHERE s NOT LIKE 'a%%' = %v, want only id=3", got)
	}
}

func TestNotInExcludesNullRowFastPath(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT id FROM t WHERE NOT (v IN (5, 20))`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("fast path: WHERE NOT (v IN (5, 20)) = %v, want only id=3 (NULL row must stay excluded)", got)
	}
}

func TestNotInExcludesNullRowGeneralPath(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT DISTINCT id FROM t WHERE NOT (v IN (5, 20))`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("general path: WHERE NOT (v IN (5, 20)) = %v, want only id=3", got)
	}
}

func TestDirectNotInOperatorExcludesNullRow(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT id FROM t WHERE v NOT IN (5, 20)`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("WHERE v NOT IN (5, 20) = %v, want only id=3", got)
	}
}

func TestNotRegexpExcludesNullRow(t *testing.T) {
	db := setupThreeValuedLogicTable(t)
	rs := execSQL(t, db, `SELECT id FROM t WHERE NOT (s REGEXP '^a')`)
	got := idSet(rs.Rows)
	if len(got) != 1 || !got[3] {
		t.Fatalf("WHERE NOT (s REGEXP '^a') = %v, want only id=3 (NULL row must stay excluded)", got)
	}
}

func TestPlainInAndLikeStillExcludeNullRow(t *testing.T) {
	// Non-negated forms must keep excluding the NULL row too (both false
	// and unknown are non-matches for a direct WHERE predicate) — guards
	// against the fix accidentally flipping the non-negated case.
	db := setupThreeValuedLogicTable(t)
	if rs := execSQL(t, db, `SELECT id FROM t WHERE v IN (5, 20)`); len(idSet(rs.Rows)) != 1 || !idSet(rs.Rows)[1] {
		t.Fatalf("WHERE v IN (5, 20) = %v, want only id=1", idSet(rs.Rows))
	}
	if rs := execSQL(t, db, `SELECT id FROM t WHERE s LIKE 'a%'`); len(idSet(rs.Rows)) != 1 || !idSet(rs.Rows)[1] {
		t.Fatalf("WHERE s LIKE 'a%%' = %v, want only id=1", idSet(rs.Rows))
	}
}
