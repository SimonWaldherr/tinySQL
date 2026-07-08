package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestRowsFromTableUniqueColumns exercises the fast path added when
// rowsFromTable stopped doing a per-row "does this key already exist" map
// check: for a table with no duplicate column names (the overwhelming common
// case), both the qualified ("alias.col") and unqualified ("col") keys must
// still be set correctly for every row.
func TestRowsFromTableUniqueColumns(t *testing.T) {
	tbl := storage.NewTable("t", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "Name", Type: storage.TextType},
	}, false)
	tbl.Rows = [][]any{
		{1, "Alice"},
		{2, "Bob"},
	}

	rows, cols := rowsFromTable(tbl, "t")

	if want := []string{"t.id", "t.name"}; !equalStrings(cols, want) {
		t.Fatalf("cols = %v, want %v", cols, want)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0]["t.id"] != 1 || rows[0]["id"] != 1 {
		t.Fatalf("row 0 id keys = %#v", rows[0])
	}
	if rows[0]["t.name"] != "Alice" || rows[0]["name"] != "Alice" {
		t.Fatalf("row 0 name keys = %#v", rows[0])
	}
	if rows[1]["t.id"] != 2 || rows[1]["id"] != 2 {
		t.Fatalf("row 1 id keys = %#v", rows[1])
	}
	if rows[1]["t.name"] != "Bob" || rows[1]["name"] != "Bob" {
		t.Fatalf("row 1 name keys = %#v", rows[1])
	}
}

// TestRowsFromTableDuplicateColumnNames exercises the slow-path fallback:
// when a table has two columns whose names are equal case-insensitively
// (reachable e.g. via `CREATE TABLE t (id INT, ID INT)` or a CREATE TABLE AS
// SELECT with two unaliased same-named projections), the unqualified key
// must resolve to the FIRST matching column's value, matching the pre-fast-
// path behavior exactly. Both qualified keys stay independently addressable.
func TestRowsFromTableDuplicateColumnNames(t *testing.T) {
	tbl := storage.NewTable("t", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "ID", Type: storage.IntType},
	}, false)
	tbl.Rows = [][]any{
		{1, 100},
	}

	rows, cols := rowsFromTable(tbl, "t")

	if want := []string{"t.id", "t.id"}; !equalStrings(cols, want) {
		t.Fatalf("cols = %v, want %v", cols, want)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	// Both columns lower-case to the same qualified key "t.id", so the
	// second column's value wins there (plain map overwrite, matches
	// pre-existing behavior for the qualified-key loop).
	if rows[0]["t.id"] != 100 {
		t.Fatalf(`row["t.id"] = %#v, want 100 (second column wins on qualified key)`, rows[0]["t.id"])
	}
	// The unqualified key must resolve to the FIRST column's value (id=1),
	// not the second (ID=100) — this is the exact behavior the removed
	// per-row `if _, exists := row[uq]; !exists` check used to guarantee.
	if rows[0]["id"] != 1 {
		t.Fatalf(`row["id"] = %#v, want 1 (first column wins on unqualified key)`, rows[0]["id"])
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
