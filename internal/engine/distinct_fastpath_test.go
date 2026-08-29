package engine

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// SELECT DISTINCT without ORDER BY now runs on the raw fast path
// (executeSimpleSelectDistinctFastPath), which dedupes on projected values
// before building a Row map. Previously DISTINCT disqualified the fast path
// entirely and the general path materialized a map per *source* row.
//
// These tests pin the new path against the general one. The comparison is
// possible because DISTINCT combined with ORDER BY deliberately declines the
// fast path, so the identical logical query can be executed both ways and the
// results compared as sets.

func setupDistinctTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE d (id INT, grp TEXT, sub TEXT, val FLOAT, flag BOOL)`)
	// Deliberate shape: many rows, few distinct groups, duplicates interleaved
	// rather than clustered, so first-occurrence ordering is observable.
	for i := 0; i < 60; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO d VALUES (%d, 'g%d', 's%d', %d.5, %v)`,
			i, i%4, i%3, i%5, i%2 == 0))
	}
	// A NULL group and a duplicate of it: NULL must dedupe against NULL.
	execSQL(t, db, `INSERT INTO d (id, sub, val, flag) VALUES (900, 'sx', 1.5, true)`)
	execSQL(t, db, `INSERT INTO d (id, sub, val, flag) VALUES (901, 'sx', 1.5, true)`)
	return db
}

// rowsAsKeys renders a result set as comparable strings, in result order.
func rowsAsKeys(rs *ResultSet) []string {
	out := make([]string, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		parts := make([]string, 0, len(rs.Cols))
		for _, c := range rs.Cols {
			v, _ := ragValue(r, c)
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

// TestDistinctFastPathMatchesGeneralPath runs each query twice — once without
// ORDER BY (raw fast path) and once with (general path) — and requires the two
// to agree as sets.
func TestDistinctFastPathMatchesGeneralPath(t *testing.T) {
	db := setupDistinctTable(t)

	cases := []struct{ fast, general string }{
		{`SELECT DISTINCT grp FROM d`,
			`SELECT DISTINCT grp FROM d ORDER BY grp`},
		{`SELECT DISTINCT grp, sub FROM d`,
			`SELECT DISTINCT grp, sub FROM d ORDER BY grp, sub`},
		{`SELECT DISTINCT sub FROM d WHERE id < 30`,
			`SELECT DISTINCT sub FROM d WHERE id < 30 ORDER BY sub`},
		{`SELECT DISTINCT flag FROM d`,
			`SELECT DISTINCT flag FROM d ORDER BY flag`},
		{`SELECT DISTINCT val FROM d`,
			`SELECT DISTINCT val FROM d ORDER BY val`},
		// Expression projection, not a bare column reference.
		{`SELECT DISTINCT val * 2 AS m FROM d`,
			`SELECT DISTINCT val * 2 AS m FROM d ORDER BY m`},
		// Every column, including the NULL-group rows.
		{`SELECT DISTINCT grp, sub, flag FROM d`,
			`SELECT DISTINCT grp, sub, flag FROM d ORDER BY grp, sub, flag`},
		// A predicate that matches nothing.
		{`SELECT DISTINCT grp FROM d WHERE id > 100000`,
			`SELECT DISTINCT grp FROM d WHERE id > 100000 ORDER BY grp`},
	}

	for _, c := range cases {
		fastKeys := rowsAsKeys(execSQL(t, db, c.fast))
		genKeys := rowsAsKeys(execSQL(t, db, c.general))
		sortedFast := append([]string(nil), fastKeys...)
		sortedGen := append([]string(nil), genKeys...)
		sort.Strings(sortedFast)
		sort.Strings(sortedGen)
		if len(sortedFast) != len(sortedGen) {
			t.Errorf("%s\n  fast path returned %d rows %v\n  general path returned %d rows %v",
				c.fast, len(sortedFast), sortedFast, len(sortedGen), sortedGen)
			continue
		}
		for i := range sortedFast {
			if sortedFast[i] != sortedGen[i] {
				t.Errorf("%s\n  fast    = %v\n  general = %v", c.fast, sortedFast, sortedGen)
				break
			}
		}
		// Whatever the path, DISTINCT must not emit a duplicate.
		seen := map[string]bool{}
		for _, k := range fastKeys {
			if seen[k] {
				t.Errorf("%s: fast path emitted duplicate row %q", c.fast, k)
			}
			seen[k] = true
		}
	}
}

// TestDistinctFastPathPreservesFirstOccurrenceOrder pins the ordering contract
// distinctRows defines: the first row of each distinct key wins, and output
// follows scan order. The expected order here is derived from the known insert
// order rather than from the other path, so a change in either implementation
// is caught.
func TestDistinctFastPathPreservesFirstOccurrenceOrder(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE o (id INT, g TEXT)`)
	// First appearances, in order: b, a, c.
	for _, g := range []string{"b", "a", "b", "c", "a", "c", "b"} {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO o VALUES (0, '%s')`, g))
	}
	got := rowsAsKeys(execSQL(t, db, `SELECT DISTINCT g FROM o`))
	want := []string{"b", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("first-occurrence order not preserved: got %v, want %v", got, want)
		}
	}
}

// TestDistinctFastPathLimitOffset checks that LIMIT/OFFSET count DISTINCT rows
// rather than scanned rows. The fast path returns straight to the caller, so it
// owns these clauses itself — applying them to scanned rows would silently
// return the wrong page.
func TestDistinctFastPathLimitOffset(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE p (id INT, g TEXT)`)
	for i := 0; i < 40; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO p VALUES (%d, 'g%d')`, i, i%5))
	}
	// 5 distinct groups, first-occurrence order g0..g4.
	all := rowsAsKeys(execSQL(t, db, `SELECT DISTINCT g FROM p`))
	if len(all) != 5 {
		t.Fatalf("expected 5 distinct groups, got %v", all)
	}
	cases := []struct {
		sql  string
		want []string
	}{
		{`SELECT DISTINCT g FROM p LIMIT 2`, all[:2]},
		{`SELECT DISTINCT g FROM p LIMIT 2 OFFSET 1`, all[1:3]},
		{`SELECT DISTINCT g FROM p LIMIT 100`, all},
		{`SELECT DISTINCT g FROM p LIMIT 0`, []string{}},
		{`SELECT DISTINCT g FROM p LIMIT 3 OFFSET 4`, all[4:5]},
		{`SELECT DISTINCT g FROM p LIMIT 5 OFFSET 10`, []string{}},
	}
	for _, c := range cases {
		got := rowsAsKeys(execSQL(t, db, c.sql))
		if len(got) != len(c.want) {
			t.Errorf("%s: got %v, want %v", c.sql, got, c.want)
			continue
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("%s: got %v, want %v", c.sql, got, c.want)
				break
			}
		}
	}
}

// TestDistinctDuplicateOutputNamesFallsBack covers the one shape where a
// values-based key would diverge from the map-based one: two projections
// sharing an output name collapse to a single Row entry, so the general path's
// key repeats the surviving value. distinctProjectionsSafe rejects it and the
// query must still return the general path's answer.
func TestDistinctDuplicateOutputNamesFallsBack(t *testing.T) {
	if distinctProjectionsSafe([]simpleProjection{{key: "x"}, {key: "x"}}) {
		t.Error("two projections sharing an output key must be rejected")
	}
	if !distinctProjectionsSafe([]simpleProjection{{key: "a"}, {key: "b"}}) {
		t.Error("distinct output keys must be accepted")
	}

	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE q (a INT, b INT)`)
	execSQL(t, db, `INSERT INTO q VALUES (1, 2), (1, 3), (1, 2)`)
	// Both projections are named x; the Row map holds only b's value, so the
	// distinct keys are 2, 3, 2 -> two rows.
	rs := execSQL(t, db, `SELECT DISTINCT a AS x, b AS x FROM q`)
	if len(rs.Rows) != 2 {
		t.Errorf("expected 2 rows from duplicate-output-name DISTINCT, got %d: %v",
			len(rs.Rows), rowsAsKeys(rs))
	}
}

// TestDistinctStarFastPath covers SELECT DISTINCT *, where every column is
// projected with both an unqualified and a qualified Row key.
func TestDistinctStarFastPath(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE s (a INT, b TEXT)`)
	execSQL(t, db, `INSERT INTO s VALUES (1, 'x'), (1, 'x'), (2, 'y'), (1, 'x')`)
	rs := execSQL(t, db, `SELECT DISTINCT * FROM s`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 distinct rows, got %d: %v", len(rs.Rows), rowsAsKeys(rs))
	}
	// The qualified key must still be present, as it is for a non-DISTINCT
	// star query (see buildSimpleSelectStarProjections).
	if v, ok := ragValue(rs.Rows[0], "s.a"); !ok {
		t.Errorf("qualified key s.a missing from DISTINCT * row: %v", rs.Rows[0])
	} else if v == nil {
		t.Errorf("qualified key s.a present but nil: %v", rs.Rows[0])
	}
}

// BenchmarkSelectDistinctFewGroups is the shape DISTINCT is normally written
// for: many rows, few distinct values. Before the fast path this allocated a
// Row map per scanned row; now it allocates one per surviving row.
func BenchmarkSelectDistinctFewGroups(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT DISTINCT grp FROM t`)
}

// BenchmarkSelectDistinctManyGroups is the adversarial case — nearly every row
// is distinct, so dedup cannot avoid the map and only pays for the extra key
// hashing. It guards against the fast path being a regression there.
func BenchmarkSelectDistinctManyGroups(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT DISTINCT id FROM t`)
}
