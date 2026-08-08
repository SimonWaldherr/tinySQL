package storage

import "testing"

// TestStructVersionOnlyAdvancesOnNonAppendMutation guards the invariant
// engine/vector_index.go's incremental HNSW extension depends on: appending
// rows (MarkDirtyFrom with a non-negative index, as INSERT does) must never
// advance StructVersion, while any non-append mutation (MarkDirtyFrom(-1), as
// DELETE/DDL/foreign-key cascades do, or MarkRowUpdated, as UPDATE does) must
// always advance it.
func TestStructVersionOnlyAdvancesOnNonAppendMutation(t *testing.T) {
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	if got := tbl.StructVersion(); got != 0 {
		t.Fatalf("new table: StructVersion()=%d, want 0", got)
	}

	tbl.MarkDirtyFrom(0) // INSERT-style append
	tbl.MarkDirtyFrom(3)
	if got := tbl.StructVersion(); got != 0 {
		t.Fatalf("after append-only MarkDirtyFrom calls: StructVersion()=%d, want unchanged 0", got)
	}

	tbl.MarkDirtyFrom(-1) // DELETE/DDL-style non-append mutation
	if got := tbl.StructVersion(); got != 1 {
		t.Fatalf("after MarkDirtyFrom(-1): StructVersion()=%d, want 1", got)
	}

	tbl.MarkDirtyFrom(2) // a later append must not un-advance it
	if got := tbl.StructVersion(); got != 1 {
		t.Fatalf("append after a non-append mutation: StructVersion()=%d, want still 1", got)
	}

	tbl.MarkRowUpdated(0) // UPDATE-style in-place row replacement
	if got := tbl.StructVersion(); got != 2 {
		t.Fatalf("after MarkRowUpdated: StructVersion()=%d, want 2", got)
	}
}

// TestStructVersionSurvivesResetDirty confirms StructVersion is independent
// of the WAL checkpoint's dirty-tracking reset: unlike dirtyFrom/dirtyRows,
// it must still reflect every non-append mutation that ever happened, not
// just those since the last checkpoint, because a cached structure that only
// knows how to grow by appending (vector_index.go's HNSW index) may go a long
// time between rebuilds — far longer than the WAL's own checkpoint interval —
// and must never mistake "checkpointed" for "nothing changed".
func TestStructVersionSurvivesResetDirty(t *testing.T) {
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = [][]any{{1}, {2}, {3}}
	tbl.MarkDirtyFrom(-1)
	if got := tbl.StructVersion(); got != 1 {
		t.Fatalf("before ResetDirty: StructVersion()=%d, want 1", got)
	}

	tbl.ResetDirty()
	if got := tbl.StructVersion(); got != 1 {
		t.Fatalf("ResetDirty must not reset StructVersion: got %d, want still 1", got)
	}
}
