package engine

// Tests for the range index seek.
//
// An index access path is only worth having if it returns exactly what a scan
// would. The central test here is therefore differential: the same query is run
// against two databases with identical contents, one indexed and one not, and the
// row sets must match. That catches boundary errors (inclusive vs exclusive),
// ordering assumptions, and the encoding traps that make a byte-ordered walk
// unsound for some column types — without depending on how the planner happens to
// be structured.

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func rangeExec(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rs
}

// rangeIDs returns the id column of a result, sorted, for set comparison.
func rangeIDs(t *testing.T, rs *ResultSet) []int {
	t.Helper()
	out := make([]int, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		v, _ := ragValue(row, "id")
		n, err := toInt(v)
		if err != nil {
			t.Fatalf("id %v is not an int: %v", v, err)
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

// buildRangeFixture creates a POI-shaped table, optionally indexed. Coordinates
// are floats, zoom an int, and name text, so each column type's range behaviour
// can be exercised.
func buildRangeFixture(t *testing.T, withIndex bool) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	rangeExec(t, db, `CREATE TABLE poi (id INT, lat FLOAT, lon FLOAT, zoom INT, name TEXT)`)
	for i := 0; i < 400; i++ {
		// Latitudes span negatives to positives so sign handling is covered.
		lat := -20.0 + float64(i)*0.1
		lon := 5.0 + float64(i%40)*0.25
		rangeExec(t, db, fmt.Sprintf(`INSERT INTO poi VALUES (%d, %g, %g, %d, 'p%03d')`,
			i, lat, lon, i%16, i))
	}
	if withIndex {
		rangeExec(t, db, `CREATE INDEX poi_lat ON poi (lat)`)
		rangeExec(t, db, `CREATE INDEX poi_latlon ON poi (lat, lon)`)
		rangeExec(t, db, `CREATE INDEX poi_zoom_lat ON poi (zoom, lat)`)
		rangeExec(t, db, `CREATE INDEX poi_name ON poi (name)`)
	}
	return db
}

// TestRangeIndexMatchesTableScan is the core correctness test.
func TestRangeIndexMatchesTableScan(t *testing.T) {
	indexed := buildRangeFixture(t, true)
	scanned := buildRangeFixture(t, false)

	queries := []string{
		// Two-sided, one-sided, and both strictness variants.
		`SELECT id FROM poi WHERE lat BETWEEN -5.0 AND 5.0`,
		`SELECT id FROM poi WHERE lat >= -5.0 AND lat <= 5.0`,
		`SELECT id FROM poi WHERE lat > -5.0 AND lat < 5.0`,
		`SELECT id FROM poi WHERE lat > 10.5`,
		`SELECT id FROM poi WHERE lat >= 10.5`,
		`SELECT id FROM poi WHERE lat < -19.0`,
		`SELECT id FROM poi WHERE lat <= -19.0`,
		// Exactly on stored boundary values, where inclusivity decides.
		`SELECT id FROM poi WHERE lat >= -20.0`,
		`SELECT id FROM poi WHERE lat > -20.0`,
		`SELECT id FROM poi WHERE lat <= 19.9`,
		// Bounding box: a range on the leading column, residual on the second.
		`SELECT id FROM poi WHERE lat BETWEEN -1.0 AND 1.0 AND lon BETWEEN 6.0 AND 8.0`,
		// Equality prefix plus range, which is the composite access shape.
		`SELECT id FROM poi WHERE zoom = 3 AND lat BETWEEN -10.0 AND 10.0`,
		`SELECT id FROM poi WHERE zoom = 3 AND lat > 0.0`,
		// Integer range.
		`SELECT id FROM poi WHERE zoom BETWEEN 4 AND 6`,
		`SELECT id FROM poi WHERE zoom > 13`,
		// Integer literals against a float column: the bound must widen, not
		// truncate.
		`SELECT id FROM poi WHERE lat BETWEEN -5 AND 5`,
		`SELECT id FROM poi WHERE lat > 10`,
		// Empty and full ranges.
		`SELECT id FROM poi WHERE lat BETWEEN 100.0 AND 200.0`,
		`SELECT id FROM poi WHERE lat BETWEEN -1000.0 AND 1000.0`,
		// Reversed range: no rows.
		`SELECT id FROM poi WHERE lat BETWEEN 5.0 AND -5.0`,
		// Literal on the left, mirroring the operator.
		`SELECT id FROM poi WHERE 0.0 < lat AND 5.0 > lat`,
		// Text range must not use the index, and must still be right.
		`SELECT id FROM poi WHERE name > 'p100' AND name < 'p200'`,
		// Range combined with an unrelated predicate.
		`SELECT id FROM poi WHERE lat BETWEEN -5.0 AND 5.0 AND name > 'p100'`,
		// OR must not be treated as a bound.
		`SELECT id FROM poi WHERE lat < -19.0 OR lat > 19.0`,
		// Contradictory bounds on the same column.
		`SELECT id FROM poi WHERE lat > 5.0 AND lat < 1.0`,
		// Redundant bounds: the tightest must win.
		`SELECT id FROM poi WHERE lat > 0.0 AND lat > 3.0 AND lat < 10.0 AND lat < 6.0`,
	}

	for _, q := range queries {
		want := rangeIDs(t, rangeExec(t, scanned, q))
		got := rangeIDs(t, rangeExec(t, indexed, q))
		if len(want) != len(got) {
			t.Errorf("%s\n  indexed returned %d rows, scan returned %d", q, len(got), len(want))
			continue
		}
		for i := range want {
			if want[i] != got[i] {
				t.Errorf("%s\n  indexed ids %v != scan ids %v", q, got, want)
				break
			}
		}
	}
}

// TestRangeIndexIsActuallyUsed guards against the tests above passing simply
// because the planner never picked the index.
func TestRangeIndexIsActuallyUsed(t *testing.T) {
	db := buildRangeFixture(t, true)
	cases := []struct {
		query    string
		wantScan string
	}{
		{`SELECT id FROM poi WHERE lat BETWEEN -1.0 AND 1.0`, "INDEX RANGE SCAN"},
		{`SELECT id FROM poi WHERE lat > 10.0`, "INDEX RANGE SCAN"},
		{`SELECT id FROM poi WHERE zoom = 3 AND lat > 0.0`, "INDEX RANGE SCAN"},
		{`SELECT id FROM poi WHERE zoom BETWEEN 4 AND 6`, "INDEX RANGE SCAN"},
		// Text ranges are not byte-ordered by value, so the index must be refused.
		{`SELECT id FROM poi WHERE name > 'p100'`, "TABLE SCAN"},
		// A range under OR does not constrain the result.
		{`SELECT id FROM poi WHERE lat > 10.0 OR lat < -10.0`, "TABLE SCAN"},
		// Equality alone still takes an equality seek (prefix, since poi_zoom_lat
		// has a second column that is unconstrained).
		{`SELECT id FROM poi WHERE zoom = 3`, "INDEX PREFIX SEEK"},
	}
	for _, tc := range cases {
		rs := rangeExec(t, db, `EXPLAIN `+tc.query)
		var plan strings.Builder
		for _, row := range rs.Rows {
			for _, col := range rs.Cols {
				if v, ok := ragValue(row, col); ok && v != nil {
					fmt.Fprintf(&plan, "%v ", v)
				}
			}
		}
		if !strings.Contains(plan.String(), tc.wantScan) {
			t.Errorf("%s\n  expected %s, plan was:\n  %s", tc.query, tc.wantScan, plan.String())
		}
	}
}

// TestRangeIndexRefusesMixedNumericColumn covers the encoding trap: integers and
// floats carry different index tags, so a column holding both sorts as two blocks
// rather than by value. A range over such a column must fall back to a scan and
// still return every matching row.
func TestRangeIndexRefusesMixedNumericColumn(t *testing.T) {
	build := func(withIndex bool) *storage.DB {
		db := storage.NewDB()
		rangeExec(t, db, `CREATE TABLE m (id INT, v FLOAT)`)
		table, err := db.Get("default", "m")
		if err != nil {
			t.Fatal(err)
		}
		// Written directly so the column genuinely mixes int and float64 for
		// values that interleave numerically.
		table.Rows = [][]any{
			{0, int(1)}, {1, float64(1.5)}, {2, int(2)},
			{3, float64(2.5)}, {4, int(3)}, {5, float64(3.5)},
		}
		table.Version++
		if withIndex {
			rangeExec(t, db, `CREATE INDEX m_v ON m (v)`)
		}
		return db
	}
	indexed, scanned := build(true), build(false)

	for _, q := range []string{
		`SELECT id FROM m WHERE v BETWEEN 1.0 AND 3.0`,
		`SELECT id FROM m WHERE v > 2.0`,
		`SELECT id FROM m WHERE v <= 2.5`,
	} {
		want := rangeIDs(t, rangeExec(t, scanned, q))
		got := rangeIDs(t, rangeExec(t, indexed, q))
		if fmt.Sprint(want) != fmt.Sprint(got) {
			t.Errorf("%s: indexed %v != scan %v — a mixed int/float column has no byte order to walk",
				q, got, want)
		}
	}
}

// TestRangeIndexFloatBoundOnIntegerColumn covers the bound conversion that must
// not move a boundary. `zoom > 1.5` on an integer column cannot become `zoom > 1`,
// which would wrongly admit 1.
func TestRangeIndexFloatBoundOnIntegerColumn(t *testing.T) {
	build := func(withIndex bool) *storage.DB {
		db := storage.NewDB()
		rangeExec(t, db, `CREATE TABLE z (id INT, zoom INT)`)
		for i := 0; i < 10; i++ {
			rangeExec(t, db, fmt.Sprintf(`INSERT INTO z VALUES (%d, %d)`, i, i))
		}
		if withIndex {
			rangeExec(t, db, `CREATE INDEX z_zoom ON z (zoom)`)
		}
		return db
	}
	indexed, scanned := build(true), build(false)
	for _, q := range []string{
		`SELECT id FROM z WHERE zoom > 1.5`,
		`SELECT id FROM z WHERE zoom < 4.5`,
		`SELECT id FROM z WHERE zoom BETWEEN 1.5 AND 4.5`,
		`SELECT id FROM z WHERE zoom >= 2.0`,
	} {
		want := rangeIDs(t, rangeExec(t, scanned, q))
		got := rangeIDs(t, rangeExec(t, indexed, q))
		if fmt.Sprint(want) != fmt.Sprint(got) {
			t.Errorf("%s: indexed %v != scan %v", q, got, want)
		}
	}
}

// TestRangeIndexWithNullsAndNegativeZero covers values whose ordering is easy to
// get wrong: NULL has its own encoding, and -0.0 compares equal to 0.0 while
// encoding differently.
func TestRangeIndexWithNullsAndNegativeZero(t *testing.T) {
	build := func(withIndex bool) *storage.DB {
		db := storage.NewDB()
		rangeExec(t, db, `CREATE TABLE n (id INT, v FLOAT)`)
		table, err := db.Get("default", "n")
		if err != nil {
			t.Fatal(err)
		}
		table.Rows = [][]any{
			{0, float64(-1)}, {1, math.Copysign(0, -1)}, {2, float64(0)},
			{3, float64(1)}, {4, nil}, {5, float64(2)},
		}
		table.Version++
		if withIndex {
			rangeExec(t, db, `CREATE INDEX n_v ON n (v)`)
		}
		return db
	}
	indexed, scanned := build(true), build(false)
	for _, q := range []string{
		`SELECT id FROM n WHERE v >= 0.0`,
		`SELECT id FROM n WHERE v > 0.0`,
		`SELECT id FROM n WHERE v BETWEEN -1.0 AND 1.0`,
		`SELECT id FROM n WHERE v < 0.0`,
	} {
		want := rangeIDs(t, rangeExec(t, scanned, q))
		got := rangeIDs(t, rangeExec(t, indexed, q))
		if fmt.Sprint(want) != fmt.Sprint(got) {
			t.Errorf("%s: indexed %v != scan %v", q, got, want)
		}
	}
}

// TestRangeIndexSurvivesMutation checks the seek reflects writes, since the index
// is maintained incrementally and the column profile is cached by table version.
func TestRangeIndexSurvivesMutation(t *testing.T) {
	db := storage.NewDB()
	rangeExec(t, db, `CREATE TABLE t (id INT, v FLOAT)`)
	for i := 0; i < 20; i++ {
		rangeExec(t, db, fmt.Sprintf(`INSERT INTO t VALUES (%d, %g)`, i, float64(i)))
	}
	rangeExec(t, db, `CREATE INDEX t_v ON t (v)`)

	count := func(q string) int { return len(rangeExec(t, db, q).Rows) }
	if got := count(`SELECT id FROM t WHERE v BETWEEN 5.0 AND 9.0`); got != 5 {
		t.Fatalf("initial range returned %d rows, want 5", got)
	}
	rangeExec(t, db, `INSERT INTO t VALUES (100, 7.5)`)
	if got := count(`SELECT id FROM t WHERE v BETWEEN 5.0 AND 9.0`); got != 6 {
		t.Errorf("after insert: %d rows, want 6 — the index seek missed a new row", got)
	}
	rangeExec(t, db, `DELETE FROM t WHERE id = 6`)
	if got := count(`SELECT id FROM t WHERE v BETWEEN 5.0 AND 9.0`); got != 5 {
		t.Errorf("after delete: %d rows, want 5", got)
	}
	rangeExec(t, db, `UPDATE t SET v = 100.0 WHERE id = 7`)
	if got := count(`SELECT id FROM t WHERE v BETWEEN 5.0 AND 9.0`); got != 4 {
		t.Errorf("after update: %d rows, want 4 — a moved value is still in the old range", got)
	}
	if got := count(`SELECT id FROM t WHERE v > 50.0`); got != 1 {
		t.Errorf("moved value not findable at its new position: %d rows, want 1", got)
	}
}

// TestRangeIndexStorageLayer exercises LookupSecondaryIndexRange directly,
// including the unsupported cases it must report rather than answer wrongly.
func TestRangeIndexStorageLayer(t *testing.T) {
	table := storage.NewTable("t", []storage.Column{
		{Name: "a", Type: storage.IntType},
		{Name: "b", Type: storage.TextType},
	}, false)
	for i := 0; i < 10; i++ {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("s%d", i)})
	}
	table.Version++
	if err := table.CreateSecondaryIndex("t_a", []string{"a"}, false); err != nil {
		t.Fatal(err)
	}
	if err := table.CreateSecondaryIndex("t_b", []string{"b"}, false); err != nil {
		t.Fatal(err)
	}
	if err := table.RebuildSecondaryIndexes(); err != nil {
		t.Fatal(err)
	}

	inc := func(v any) storage.IndexRangeBound {
		return storage.IndexRangeBound{Value: v, Inclusive: true}
	}
	exc := func(v any) storage.IndexRangeBound {
		return storage.IndexRangeBound{Value: v, Inclusive: false}
	}
	absent := storage.IndexRangeBound{Absent: true}

	idxA := table.Indexes["t_a"]
	rows, err := table.LookupSecondaryIndexRange(idxA, nil, inc(3), inc(6))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rows) != "[3 4 5 6]" {
		t.Errorf("inclusive 3..6 = %v, want [3 4 5 6]", rows)
	}
	if rows, err = table.LookupSecondaryIndexRange(idxA, nil, exc(3), exc(6)); err != nil {
		t.Fatal(err)
	} else if fmt.Sprint(rows) != "[4 5]" {
		t.Errorf("exclusive 3..6 = %v, want [4 5]", rows)
	}
	if rows, err = table.LookupSecondaryIndexRange(idxA, nil, inc(7), absent); err != nil {
		t.Fatal(err)
	} else if fmt.Sprint(rows) != "[7 8 9]" {
		t.Errorf(">= 7 = %v, want [7 8 9]", rows)
	}
	if rows, err = table.LookupSecondaryIndexRange(idxA, nil, absent, exc(2)); err != nil {
		t.Fatal(err)
	} else if fmt.Sprint(rows) != "[0 1]" {
		t.Errorf("< 2 = %v, want [0 1]", rows)
	}

	// A text component cannot be range-walked: its key framing puts a length
	// ahead of the payload, so byte order is not value order.
	if _, err = table.LookupSecondaryIndexRange(table.Indexes["t_b"], nil, inc("s3"), inc("s6")); err == nil {
		t.Error("a text range should be reported unsupported, not answered")
	}
	// Both bounds absent is meaningless.
	if _, err = table.LookupSecondaryIndexRange(idxA, nil, absent, absent); err == nil {
		t.Error("an unbounded range should be rejected")
	}
	// A prefix as long as the index leaves no column to range over.
	if _, err = table.LookupSecondaryIndexRange(idxA, []any{1}, inc(2), absent); err == nil {
		t.Error("a prefix covering every index column should be rejected")
	}
}
