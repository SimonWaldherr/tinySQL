package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A whole-table aggregate with no GROUP BY at all ("SELECT COUNT(*) FROM t",
// "SELECT SUM(x), AVG(y) FROM t WHERE ...") used to be excluded from the raw
// aggregate fast path (simpleAggregateEligibleSelect required len(s.GroupBy) >
// 0), so it fell all the way back to the general path: resolveFromClause
// materializes a dual-key Row map for every row via rowsFromTable before
// aggregation even starts. That is arguably the single most common SQL shape
// there is.
//
// The accumulator machinery already treats zero group-by columns as one
// implicit group (executeSimpleMultiGroupAggregate); the new piece is
// synthesizing that group's output row when literally no input row matched —
// SQL requires a whole-table aggregate to still return exactly one row over
// zero matching rows (COUNT(*) = 0, SUM/AVG/MIN/MAX = NULL), unlike a real
// GROUP BY, which correctly returns zero rows in that case.

func ungroupedAggDB(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE u (id INT, val FLOAT, grp TEXT)`)
	for i := 0; i < 37; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO u VALUES (%d, %d.5, 'g%d')`, i, i, i%4))
	}
	// A NULL value: SUM/AVG/MIN/MAX must skip it, COUNT(val) must not count it.
	execSQL(t, db, `INSERT INTO u (id, grp) VALUES (900, 'g0')`)
	return db
}

func firstRow(t *testing.T, rs *ResultSet) Row {
	t.Helper()
	if len(rs.Rows) != 1 {
		t.Fatalf("expected exactly 1 row, got %d: %v", len(rs.Rows), rs.Rows)
	}
	return rs.Rows[0]
}

func TestUngroupedAggregateBasic(t *testing.T) {
	db := ungroupedAggDB(t)
	rs := execSQL(t, db, `SELECT COUNT(*) AS n, COUNT(val) AS nv, SUM(val) AS s, AVG(val) AS a, MIN(val) AS mn, MAX(val) AS mx FROM u`)
	r := firstRow(t, rs)

	n, _ := ragValue(r, "n")
	if got, _ := toInt(n); got != 38 {
		t.Errorf("COUNT(*) = %v, want 38", n)
	}
	nv, _ := ragValue(r, "nv")
	if got, _ := toInt(nv); got != 37 {
		t.Errorf("COUNT(val) = %v, want 37 (the NULL row must not count)", nv)
	}
	mn, _ := ragValue(r, "mn")
	if fmt.Sprintf("%v", mn) != "0.5" {
		t.Errorf("MIN(val) = %v, want 0.5", mn)
	}
	mx, _ := ragValue(r, "mx")
	if fmt.Sprintf("%v", mx) != "36.5" {
		t.Errorf("MAX(val) = %v, want 36.5", mx)
	}
	// sum of 0.5..36.5 step 1 over 37 terms = 37*(0.5+36.5)/2 = 684.5
	s, _ := ragValue(r, "s")
	if sf, ok := s.(float64); !ok || sf != 684.5 {
		t.Errorf("SUM(val) = %v, want 684.5", s)
	}
	a, _ := ragValue(r, "a")
	if af, ok := a.(float64); !ok || af != 684.5/37.0 {
		t.Errorf("AVG(val) = %v, want %v", a, 684.5/37.0)
	}
}

// TestUngroupedAggregateEmptyInput is the contract this change had to get
// right: zero matching rows must still yield exactly one row, with COUNT=0
// and everything else NULL — not zero rows, which is what a real GROUP BY
// would (correctly) produce.
func TestUngroupedAggregateEmptyInput(t *testing.T) {
	for _, sql := range []string{
		`SELECT COUNT(*) AS n, SUM(val) AS s, AVG(val) AS a, MIN(val) AS mn, MAX(val) AS mx FROM u WHERE id > 100000`,
	} {
		db := ungroupedAggDB(t)
		rs := execSQL(t, db, sql)
		r := firstRow(t, rs)
		n, _ := ragValue(r, "n")
		if got, _ := toInt(n); got != 0 {
			t.Errorf("%s: COUNT(*) = %v, want 0", sql, n)
		}
		for _, col := range []string{"s", "a", "mn", "mx"} {
			v, ok := ragValue(r, col)
			if !ok || v != nil {
				t.Errorf("%s: %s = %v, want NULL", sql, col, v)
			}
		}
	}

	// An entirely empty table takes the same path.
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE empty_u (id INT, val FLOAT)`)
	rs := execSQL(t, db, `SELECT COUNT(*) AS n, SUM(val) AS s FROM empty_u`)
	r := firstRow(t, rs)
	if n, _ := ragValue(r, "n"); fmt.Sprintf("%v", n) != "0" {
		t.Errorf("COUNT(*) over an empty table = %v, want 0", n)
	}
	if s, _ := ragValue(r, "s"); s != nil {
		t.Errorf("SUM(val) over an empty table = %v, want NULL", s)
	}
}

// TestUngroupedAggregateHavingFiltersEmptyGroup checks that HAVING is applied
// to the synthesized empty-input group too: if it doesn't match, the query
// must return zero rows, not one row with a value that fails the predicate.
func TestUngroupedAggregateHavingFiltersEmptyGroup(t *testing.T) {
	db := ungroupedAggDB(t)
	rs := execSQL(t, db, `SELECT COUNT(*) AS n FROM u WHERE id > 100000 HAVING COUNT(*) > 0`)
	if len(rs.Rows) != 0 {
		t.Errorf("HAVING COUNT(*) > 0 over zero matching rows returned %d rows, want 0: %v", len(rs.Rows), rs.Rows)
	}

	rs = execSQL(t, db, `SELECT COUNT(*) AS n FROM u HAVING COUNT(*) > 0`)
	r := firstRow(t, rs)
	if n, _ := ragValue(r, "n"); fmt.Sprintf("%v", n) != "38" {
		t.Errorf("COUNT(*) = %v, want 38", n)
	}
}

// TestUngroupedAggregateMatchesGeneralPath compares the fast path's result
// against the general path for the same logical query. A HAVING clause that
// simpleAggregateHavingSupported cannot compile (a function call it does not
// special-case) declines the raw path while remaining semantically identical
// (true for every row here), which is what makes the comparison possible.
func TestUngroupedAggregateMatchesGeneralPath(t *testing.T) {
	db := ungroupedAggDB(t)
	render := func(sql string) Row {
		rs := execSQL(t, db, sql)
		return firstRow(t, rs)
	}
	fast := render(`SELECT COUNT(*) AS n, SUM(val) AS s, AVG(val) AS a, MIN(val) AS mn, MAX(val) AS mx FROM u`)
	general := render(`SELECT COUNT(*) AS n, SUM(val) AS s, AVG(val) AS a, MIN(val) AS mn, MAX(val) AS mx FROM u HAVING LENGTH('x') > 0`)
	for _, col := range []string{"n", "s", "a", "mn", "mx"} {
		fv, _ := ragValue(fast, col)
		gv, _ := ragValue(general, col)
		if fmt.Sprintf("%v", fv) != fmt.Sprintf("%v", gv) {
			t.Errorf("%s: fast=%v general=%v", col, fv, gv)
		}
	}
}

// TestUngroupedAggregateDoesNotClaimPlainSelect guards the cheap pre-filter:
// an ordinary non-aggregate SELECT (no GROUP BY, no aggregate function) must
// still be rejected by this path and handled by the plain SELECT fast path,
// not silently misrouted.
func TestUngroupedAggregateDoesNotClaimPlainSelect(t *testing.T) {
	db := ungroupedAggDB(t)
	rs := execSQL(t, db, `SELECT id, val FROM u WHERE grp = 'g0'`)
	if len(rs.Rows) == 0 {
		t.Fatal("expected matching rows for grp = 'g0'")
	}
	for _, r := range rs.Rows {
		if _, ok := ragValue(r, "id"); !ok {
			t.Errorf("row missing id: %v", r)
		}
	}
}

// BenchmarkUngroupedCountStar and BenchmarkUngroupedSumAvg measure the shape
// this change targets: a single aggregate over the whole table, no GROUP BY.
func BenchmarkUngroupedCountStar(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT COUNT(*) FROM t`)
}

func BenchmarkUngroupedSumAvg(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT SUM(val) AS s, AVG(val) AS a, MIN(val) AS mn, MAX(val) AS mx FROM t WHERE val > 100`)
}
