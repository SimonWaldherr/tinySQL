package storage

import (
	"bytes"
	"math"
	"testing"
)

func TestCanonicalIndexValueEqualMatchesEncoding(t *testing.T) {
	tests := []struct {
		name  string
		left  any
		right any
	}{
		{name: "nil", left: nil, right: nil},
		{name: "bool", left: true, right: true},
		{name: "int and int64", left: int(7), right: int64(7)},
		{name: "int and float differ", left: int(7), right: float64(7)},
		{name: "signed zero differs", left: float64(0), right: math.Copysign(0, -1)},
		{name: "text", left: "hello", right: "hello"},
		{name: "blob", left: []byte{1, 2}, right: []byte{1, 2}},
		{name: "empty and nil blob", left: []byte(nil), right: []byte{}},
		{name: "fallback native type", left: int8(7), right: int8(7)},
		{name: "fallback type boundary", left: int8(7), right: int(7)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := bytes.Equal(CanonicalIndexKey([]any{test.left}), CanonicalIndexKey([]any{test.right}))
			if got := CanonicalIndexValueEqual(test.left, test.right); got != want {
				t.Fatalf("CanonicalIndexValueEqual(%#v, %#v) = %v, want %v", test.left, test.right, got, want)
			}
		})
	}
}

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
