package storage

import "testing"

// The in-place row list MarkRowUpdated builds is only ever emptied by
// ResetDirty, which runs at a WAL checkpoint. A database with no WAL attached
// therefore never empties it at all, and one with a WAL still accumulates
// between checkpoints — while every mutating statement's rollback snapshot
// carries the list along. Unbounded, that makes a run of UPDATEs against one
// table quadratic. These tests pin the two bounds that stop it.

// TestMarkRowUpdatedIgnoresConsecutiveRepeats covers the shape that actually
// runs away: the same row rewritten over and over (a counter, a status
// column, a claimed queue slot). Re-appending its index describes nothing new
// and makes both this list and the WAL record built from it grow forever.
func TestMarkRowUpdatedIgnoresConsecutiveRepeats(t *testing.T) {
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = make([][]any, 100)
	tbl.ResetDirty()

	for i := 0; i < 1000; i++ {
		tbl.MarkRowUpdated(7)
	}

	rows, exact := tbl.DirtyRows()
	if !exact {
		t.Fatalf("DirtyRows() reported no exact list after repeated updates of one row")
	}
	if len(rows) != 1 || rows[0] != 7 {
		t.Fatalf("DirtyRows() = %#v after 1000 updates of row 7, want [7]", rows)
	}
}

// TestMarkRowUpdatedGivesUpOnceListReachesTableSize pins the second bound.
// LogTransaction only writes a row-delta record while len(updated) is smaller
// than the table (see its switch); at that point it writes the whole table
// anyway, so continuing to extend the list buys nothing and costs memory plus
// a longer copy in every rollback snapshot. Giving up is the documented safe
// direction — the WAL falls back to a full-table record.
func TestMarkRowUpdatedGivesUpOnceListReachesTableSize(t *testing.T) {
	const rows = 8
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = make([][]any, rows)
	tbl.ResetDirty()

	for i := 0; i < rows; i++ {
		tbl.MarkRowUpdated(i)
	}

	if list, exact := tbl.DirtyRows(); exact {
		t.Fatalf("DirtyRows() still claims an exact list of %d entries for a %d-row table", len(list), rows)
	}
	if got := tbl.DirtyFrom(); got != -1 {
		t.Fatalf("DirtyFrom() = %d after giving up the row list, want the full-table sentinel -1", got)
	}
}

// TestMarkRowUpdatedKeepsListWhileItIsSmallerThanTheTable is the other half of
// the bound: a genuinely selective UPDATE must still get its delta.
func TestMarkRowUpdatedKeepsListWhileItIsSmallerThanTheTable(t *testing.T) {
	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = make([][]any, 1000)
	tbl.ResetDirty()

	for _, idx := range []int{4, 9, 400, 17} {
		tbl.MarkRowUpdated(idx)
	}

	list, exact := tbl.DirtyRows()
	if !exact {
		t.Fatal("DirtyRows() gave up an exact list of 4 rows in a 1000-row table")
	}
	if len(list) != 4 || list[0] != 4 || list[1] != 9 || list[2] != 400 || list[3] != 17 {
		t.Fatalf("DirtyRows() = %#v, want [4 9 400 17] in report order", list)
	}
}

// TestStatementSnapshotRestoresDirtyRowsWithoutCopying documents the other
// half of the fix: the snapshot keeps the list's slice header rather than
// copying its elements. That is only sound because the list is append-only
// within one dirty window, so the pre-statement elements survive whatever the
// statement does to it.
func TestStatementSnapshotRestoresDirtyRowsWithoutCopying(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	tbl := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	tbl.Rows = [][]any{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}}
	tbl.ResetDirty()
	tbl.MarkRowUpdated(2)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowUpdateStatement("default", "t", []int{2})
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	// The statement appends more rows to the list, in place where capacity
	// allows and by reallocating where it does not.
	tbl.MarkRowUpdated(5)
	tbl.MarkRowUpdated(1)
	db.RestoreStatementSnapshot(snapshot)
	db.ReleaseStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	list, exact := tbl.DirtyRows()
	if !exact || len(list) != 1 || list[0] != 2 {
		t.Fatalf("DirtyRows() after rollback = %#v (exact=%v), want [2]", list, exact)
	}
}
