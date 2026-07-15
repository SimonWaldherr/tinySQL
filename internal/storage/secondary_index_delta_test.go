package storage

import "testing"

func TestSecondaryIndexDeltasMaintainLookupAndDeleteRowMapping(t *testing.T) {
	table := NewTable("events", []Column{{Name: "id", Type: IntType}, {Name: "kind", Type: TextType}}, false)
	table.Rows = [][]any{{1, "a"}, {2, "b"}}
	if err := table.CreateSecondaryIndex("idx_kind", []string{"kind"}, false); err != nil {
		t.Fatal(err)
	}

	inserted := []any{3, "a"}
	table.Rows = append(table.Rows, inserted)
	if err := table.InsertSecondaryIndexRow(2, inserted); err != nil {
		t.Fatal(err)
	}
	index := table.FindSecondaryIndex([]string{"kind"})
	rows, err := table.LookupSecondaryIndexPoint(index, []any{"a"})
	if err != nil || len(rows) != 2 || rows[0] != 0 || rows[1] != 2 {
		t.Fatalf("index after insert = %v, %v", rows, err)
	}

	before, after := table.Rows[2], []any{3, "c"}
	table.Rows[2] = after
	if err := table.UpdateSecondaryIndexRow(2, before, after); err != nil {
		t.Fatal(err)
	}
	rows, _ = table.LookupSecondaryIndexPoint(index, []any{"a"})
	if len(rows) != 1 || rows[0] != 0 {
		t.Fatalf("old key after update = %v", rows)
	}
	rows, _ = table.LookupSecondaryIndexPoint(index, []any{"c"})
	if len(rows) != 1 || rows[0] != 2 {
		t.Fatalf("new key after update = %v", rows)
	}

	// Delete the first physical row. Remaining RowIDs are remapped without
	// re-encoding every index key.
	table.Rows = [][]any{{2, "b"}, {3, "c"}}
	table.ReindexSecondaryIndexRows(map[int]int{1: 0, 2: 1})
	rows, _ = table.LookupSecondaryIndexPoint(index, []any{"c"})
	if len(rows) != 1 || rows[0] != 1 {
		t.Fatalf("key after delete remap = %v", rows)
	}
}
