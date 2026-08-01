package engine

// Tests for the numeric secondary-index seek decision.
//
// The fast path added in index_seek_safety.go must not change which rows a query
// returns. The invariant it could break is SQL's numeric equality: 1 and 1.0
// compare equal, so a seek that only visits one of the two index encodings would
// silently drop rows. These tests pin both halves — that an integer lookup on a
// clean column uses the index, and that a column mixing integers and floats
// still returns every numerically equal row.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func seekTestDB(t *testing.T) *storage.DB {
	t.Helper()
	return storage.NewDB()
}

func mustExecSeek(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rs
}

// TestCompositeIndexPointSeekIsPlanned checks EXPLAIN reports a seek for the
// three-column equality lookup a tile server issues. Before the seek-safety fix
// this fell back to a scan, and the fallback was invisible except as latency.
func TestCompositeIndexPointSeekIsPlanned(t *testing.T) {
	db := seekTestDB(t)
	mustExecSeek(t, db, `CREATE TABLE tiles (zoom_level INT, tile_column INT, tile_row INT, label TEXT)`)
	for z := 0; z < 3; z++ {
		for c := 0; c < 8; c++ {
			for r := 0; r < 8; r++ {
				mustExecSeek(t, db, fmt.Sprintf(
					`INSERT INTO tiles VALUES (%d, %d, %d, 'z%dc%dr%d')`, z, c, r, z, c, r))
			}
		}
	}
	mustExecSeek(t, db, `CREATE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)`)

	rs := mustExecSeek(t, db,
		`EXPLAIN SELECT label FROM tiles WHERE zoom_level = 1 AND tile_column = 3 AND tile_row = 5`)
	var plan strings.Builder
	for _, row := range rs.Rows {
		for _, col := range rs.Cols {
			if v, ok := ragValue(row, col); ok && v != nil {
				fmt.Fprintf(&plan, "%v ", v)
			}
		}
		plan.WriteByte('\n')
	}
	text := plan.String()
	if !strings.Contains(text, "SEEK") {
		t.Errorf("plan does not use an index seek:\n%s", text)
	}

	// And it returns the right row.
	rs = mustExecSeek(t, db,
		`SELECT label FROM tiles WHERE zoom_level = 1 AND tile_column = 3 AND tile_row = 5`)
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rs.Rows))
	}
	if v, _ := ragValue(rs.Rows[0], "label"); v != "z1c3r5" {
		t.Errorf("label = %v, want z1c3r5", v)
	}
}

// TestIndexSeekKeepsNumericEquality is the correctness guard. A column holding
// both an integer and a float that compare equal must return both rows for either
// literal form, whether the planner seeks or scans.
func TestIndexSeekKeepsNumericEquality(t *testing.T) {
	db := seekTestDB(t)
	// A FLOAT column accepts both, so the stored representations can differ while
	// comparing equal.
	mustExecSeek(t, db, `CREATE TABLE mixed (k FLOAT, label TEXT)`)
	table, err := db.Get("default", "mixed")
	if err != nil {
		t.Fatal(err)
	}
	// Written directly so one row keeps an int and another a float64 for the
	// same numeric value — the situation the seek-safety check exists for.
	table.Rows = [][]any{
		{int(1), "int-one"},
		{float64(1), "float-one"},
		{int(2), "int-two"},
	}
	table.Version++
	mustExecSeek(t, db, `CREATE INDEX mixed_k ON mixed (k)`)

	for _, literal := range []string{"1", "1.0"} {
		rs := mustExecSeek(t, db, fmt.Sprintf(`SELECT label FROM mixed WHERE k = %s`, literal))
		got := map[string]bool{}
		for _, row := range rs.Rows {
			v, _ := ragValue(row, "label")
			got[fmt.Sprintf("%v", v)] = true
		}
		if !got["int-one"] || !got["float-one"] {
			t.Errorf("k = %s returned %v; both int-one and float-one compare equal to it",
				literal, got)
		}
		if got["int-two"] {
			t.Errorf("k = %s also returned int-two", literal)
		}
	}
}

// TestNumericColumnProfileTracksVersion checks the cached column summary is
// invalidated by a write, so a float arriving later is not missed.
func TestNumericColumnProfileTracksVersion(t *testing.T) {
	db := seekTestDB(t)
	mustExecSeek(t, db, `CREATE TABLE p (k FLOAT, label TEXT)`)
	table, err := db.Get("default", "p")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows = [][]any{{int(1), "a"}, {int(2), "b"}}
	table.Version++

	if numericColumnHasFloat(table, 0) {
		t.Error("column of ints reported as holding floats")
	}

	// Introduce a float64 and bump the version the way DML does.
	table.Rows = append(table.Rows, []any{float64(3.5), "c"})
	table.Version++
	if !numericColumnHasFloat(table, 0) {
		t.Error("float added after the profile was cached went unnoticed; the cache is not version-keyed")
	}

	// An out-of-range column must report conservatively.
	if !numericColumnHasFloat(table, 99) {
		t.Error("unknown column should report that it may hold floats")
	}
}

// TestIndexSeekMatchesScanResults is a differential check across many lookups:
// with the index present and absent, the same query must return the same rows.
func TestIndexSeekMatchesScanResults(t *testing.T) {
	build := func(withIndex bool) *storage.DB {
		db := seekTestDB(t)
		mustExecSeek(t, db, `CREATE TABLE t (a INT, b INT, c TEXT, label TEXT)`)
		for a := 0; a < 5; a++ {
			for b := 0; b < 5; b++ {
				mustExecSeek(t, db, fmt.Sprintf(
					`INSERT INTO t VALUES (%d, %d, 'c%d', 'a%db%d')`, a, b, a+b, a, b))
			}
		}
		if withIndex {
			mustExecSeek(t, db, `CREATE INDEX t_ab ON t (a, b)`)
		}
		return db
	}
	indexed := build(true)
	scanned := build(false)

	queries := []string{
		`SELECT label FROM t WHERE a = 2 AND b = 3`,
		`SELECT label FROM t WHERE a = 0 AND b = 0`,
		`SELECT label FROM t WHERE a = 4`,
		`SELECT label FROM t WHERE a = 9 AND b = 9`,
		`SELECT label FROM t WHERE a = 2 AND b = 3 AND c = 'c5'`,
		`SELECT label FROM t WHERE a = 2 AND b = 3 AND c = 'nope'`,
		`SELECT label FROM t WHERE b = 1`,
		`SELECT label FROM t WHERE a = 1 AND b > 2`,
	}
	for _, q := range queries {
		want := labelSet(t, mustExecSeek(t, scanned, q))
		got := labelSet(t, mustExecSeek(t, indexed, q))
		if len(want) != len(got) {
			t.Errorf("%s: indexed returned %d rows, scan returned %d", q, len(got), len(want))
			continue
		}
		for label := range want {
			if !got[label] {
				t.Errorf("%s: indexed result is missing %q", q, label)
			}
		}
	}
}

func labelSet(t *testing.T, rs *ResultSet) map[string]bool {
	t.Helper()
	out := make(map[string]bool, len(rs.Rows))
	for _, row := range rs.Rows {
		v, _ := ragValue(row, "label")
		out[fmt.Sprintf("%v", v)] = true
	}
	return out
}
