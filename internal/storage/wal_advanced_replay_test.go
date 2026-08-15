package storage

import (
	"bytes"
	"testing"
)

// Replay locates the row an update/delete record refers to. Two things about
// how it does that are load-bearing and were previously wrong: it must not
// compare cells with Go's `!=` (which panics on the slice- and map-backed
// column types this package supports), and it must use the row position the
// record already carries instead of scanning the whole table for every record.

// TestWALReplayHandlesBlobColumns is the crash regression: rowsEqual used to
// compare two `any` values directly, so replaying any update or delete against
// a table with a BLOB, VECTOR or JSON column panicked with "comparing
// uncomparable type" and took the recovering process down with it.
func TestWALReplayHandlesBlobColumns(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	cols := []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BlobType}}
	table := NewTable("blobs", cols, false)
	table.Rows = [][]any{
		{1, []byte("one")},
		{2, []byte("two")},
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	update := &WALRecord{
		Tenant:      "default",
		Table:       "blobs",
		OpType:      WALOpUpdate,
		RowID:       1,
		Columns:     cols,
		BeforeImage: []any{2, []byte("two")},
		AfterImage:  []any{2, []byte("TWO")},
	}
	if _, err := applyOperation(db, update); err != nil {
		t.Fatalf("replaying an update on a BLOB table: %v", err)
	}
	got, err := db.Get("default", "blobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 2 {
		t.Fatalf("rows after update replay = %d, want 2", len(got.Rows))
	}
	if b, ok := got.Rows[1][1].([]byte); !ok || !bytes.Equal(b, []byte("TWO")) {
		t.Fatalf("row 1 payload after update replay = %#v, want TWO", got.Rows[1][1])
	}

	del := &WALRecord{
		Tenant:      "default",
		Table:       "blobs",
		OpType:      WALOpDelete,
		RowID:       0,
		Columns:     cols,
		BeforeImage: []any{1, []byte("one")},
	}
	if _, err := applyOperation(db, del); err != nil {
		t.Fatalf("replaying a delete on a BLOB table: %v", err)
	}
	got, err = db.Get("default", "blobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 1 || got.Rows[0][0] != 2 {
		t.Fatalf("rows after delete replay = %#v, want just id 2", got.Rows)
	}
}

// TestWALReplayFallsBackWhenRowIDIsStale confirms the position hint is only a
// hint. A record whose RowID no longer addresses its before-image — the normal
// situation once an earlier delete in the same log has shifted rows down —
// must still find its row by scanning, exactly as replay always did.
func TestWALReplayFallsBackWhenRowIDIsStale(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("t", cols, false)
	table.Rows = [][]any{{10}, {20}, {30}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	// RowID 0 is wrong for this before-image: row 20 sits at position 1.
	stale := &WALRecord{
		Tenant:      "default",
		Table:       "t",
		OpType:      WALOpUpdate,
		RowID:       0,
		Columns:     cols,
		BeforeImage: []any{20},
		AfterImage:  []any{99},
	}
	if _, err := applyOperation(db, stale); err != nil {
		t.Fatal(err)
	}
	got, _ := db.Get("default", "t")
	if len(got.Rows) != 3 {
		t.Fatalf("a stale RowID caused an append instead of an in-place update: %#v", got.Rows)
	}
	if got.Rows[0][0] != 10 || got.Rows[1][0] != 99 || got.Rows[2][0] != 30 {
		t.Fatalf("rows after replay = %#v, want [10 99 30]", got.Rows)
	}
}

// TestWALDeltaRecordsCarryNoSecondaryIndexes pins the payload that was removed
// from the two delta record shapes. Their replay rebuilds indexes from the
// rows (see handleWalRecord), so shipping a materialized copy of every index
// entry made each single-row write cost — and fsync — a serialization of the
// whole index.
func TestWALDeltaRecordsCarryNoSecondaryIndexes(t *testing.T) {
	table := NewTable("t", []Column{{Name: "id", Type: IntType}, {Name: "bucket", Type: IntType}}, false)
	for i := 0; i < 64; i++ {
		table.Rows = append(table.Rows, []any{i, i % 4})
	}
	if err := table.CreateSecondaryIndex("idx_bucket", []string{"bucket"}, false); err != nil {
		t.Fatal(err)
	}

	appendDelta := tableToDiskRange("default", table, 60, 64)
	if len(appendDelta.Indexes) != 0 {
		t.Fatalf("append-rows delta carries %d secondary indexes, want none", len(appendDelta.Indexes))
	}
	if len(appendDelta.Rows) != 4 {
		t.Fatalf("append-rows delta carries %d rows, want 4", len(appendDelta.Rows))
	}

	updateDelta, idx := tableToDiskRows("default", table, []int{7})
	if len(updateDelta.Indexes) != 0 {
		t.Fatalf("update-rows delta carries %d secondary indexes, want none", len(updateDelta.Indexes))
	}
	if len(idx) != 1 || idx[0] != 7 {
		t.Fatalf("update-rows delta indexes = %#v, want [7]", idx)
	}

	// The full-table record still needs them: its replay installs the decoded
	// table wholesale rather than rebuilding.
	full := tableToDisk("default", table)
	if len(full.Indexes) != 1 {
		t.Fatalf("full-table record carries %d secondary indexes, want 1", len(full.Indexes))
	}
}
