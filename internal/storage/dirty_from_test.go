package storage

import "testing"

// TestMarkDirtyFromFullTableSentinelIsSticky guards against a WAL data-loss
// regression: within one transaction an UPDATE/DELETE forces the full-table
// sentinel (dirtyFrom == -1), and a subsequent append-only INSERT must not
// downgrade it to a delta. If it did, LogTransaction would write only the
// appended rows and the earlier UPDATE/DELETE would vanish on recovery.
func TestMarkDirtyFromFullTableSentinelIsSticky(t *testing.T) {
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)

	tbl.MarkDirtyFrom(-1) // UPDATE/DELETE: full-table WAL entry required
	if got := tbl.DirtyFrom(); got != -1 {
		t.Fatalf("after MarkDirtyFrom(-1): DirtyFrom()=%d, want -1", got)
	}

	tbl.MarkDirtyFrom(5) // later INSERT appends rows starting at index 5
	if got := tbl.DirtyFrom(); got != -1 {
		t.Fatalf("append after full-table mutation reset DirtyFrom()=%d, want sticky -1", got)
	}
}

// TestMarkDirtyFromTracksEarliestAppend confirms the pre-existing append-only
// behavior is unchanged: without the full-table sentinel, the earliest dirty
// row index wins so the WAL delta covers every appended row.
func TestMarkDirtyFromTracksEarliestAppend(t *testing.T) {
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = make([][]any, 10) // ResetDirty baselines dirtyFrom to len(Rows)
	tbl.ResetDirty()

	tbl.MarkDirtyFrom(9)
	tbl.MarkDirtyFrom(4)
	tbl.MarkDirtyFrom(7)
	if got := tbl.DirtyFrom(); got != 4 {
		t.Fatalf("append tracking: DirtyFrom()=%d, want 4 (earliest)", got)
	}
}
