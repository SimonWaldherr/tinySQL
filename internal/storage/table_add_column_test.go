package storage

import "testing"

// Table.AddColumn exists because Cols and the unexported colPos index ColIndex
// answers from have to move together. These pin the three properties callers
// depend on: the new column resolves by name, existing rows are widened, and a
// rejected call changes nothing.

func TestAddColumnKeepsNameIndexInStep(t *testing.T) {
	tbl := NewTable("kv", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = [][]any{{1}, {2}}

	if err := tbl.AddColumn(Column{Name: "Label", Type: StringType}); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}

	if len(tbl.Cols) != 2 || tbl.Cols[1].Name != "Label" {
		t.Fatalf("Cols = %#v, want a second column named Label", tbl.Cols)
	}
	// Resolvable in any case, like every column NewTable indexed.
	for _, name := range []string{"Label", "label", "LABEL"} {
		idx, err := tbl.ColIndex(name)
		if err != nil {
			t.Fatalf("ColIndex(%q): %v", name, err)
		}
		if idx != 1 {
			t.Fatalf("ColIndex(%q) = %d, want 1", name, idx)
		}
	}
	for i, row := range tbl.Rows {
		if len(row) != 2 || row[1] != nil {
			t.Fatalf("row %d = %#v, want widened with a NULL", i, row)
		}
	}
}

func TestAddColumnRejectsDuplicateRegardlessOfCase(t *testing.T) {
	tbl := NewTable("kv", []Column{{Name: "id", Type: IntType}, {Name: "name", Type: StringType}}, false)
	tbl.Rows = [][]any{{1, "one"}}

	// A case-different duplicate is the dangerous one: every lookup goes
	// through the lower-cased index, so accepting it would shadow the original
	// column and leave the two indistinguishable.
	if err := tbl.AddColumn(Column{Name: "NAME", Type: StringType}); err == nil {
		t.Fatal("AddColumn accepted a column differing from an existing one only in case")
	}
	if len(tbl.Cols) != 2 {
		t.Fatalf("Cols after a rejected AddColumn = %d, want 2", len(tbl.Cols))
	}
	if len(tbl.Rows[0]) != 2 {
		t.Fatalf("row width after a rejected AddColumn = %d, want 2", len(tbl.Rows[0]))
	}
	if idx, err := tbl.ColIndex("name"); err != nil || idx != 1 {
		t.Fatalf("the original column stopped resolving: %d, %v", idx, err)
	}
}

func TestAddColumnRejectsEmptyName(t *testing.T) {
	tbl := NewTable("kv", []Column{{Name: "id", Type: IntType}}, false)
	if err := tbl.AddColumn(Column{Type: StringType}); err == nil {
		t.Fatal("AddColumn accepted an unnamed column")
	}
	if len(tbl.Cols) != 1 {
		t.Fatalf("Cols after a rejected AddColumn = %d, want 1", len(tbl.Cols))
	}
}

// TestAddColumnAdvancesStructVersion documents that widening every row counts
// as a structural change: a derived structure that only knows how to grow by
// appending rows (engine's incremental HNSW index) must rebuild rather than
// assume the rows it indexed are unchanged.
func TestAddColumnAdvancesStructVersion(t *testing.T) {
	tbl := NewTable("kv", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = [][]any{{1}}
	before := tbl.StructVersion()

	if err := tbl.AddColumn(Column{Name: "extra", Type: StringType}); err != nil {
		t.Fatal(err)
	}
	if got := tbl.StructVersion(); got == before {
		t.Fatalf("StructVersion did not advance across AddColumn (still %d)", got)
	}
}
