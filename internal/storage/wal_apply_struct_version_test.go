package storage

import "testing"

// TestApplyOperationStructVersion guards a correctness gap that would
// otherwise open up for engine/vector_index.go's incremental HNSW
// extension: applyOperation (used by both WAL crash recovery and the
// replication feed's ApplyWALRecord) mutates table.Rows directly, bypassing
// MarkDirtyFrom/MarkRowUpdated entirely, so it must advance StructVersion by
// hand on every genuine in-place update or removal — and must NOT advance it
// for an append (insert, or an "update" that falls back to append because
// its before-image was not found).
func TestApplyOperationStructVersion(t *testing.T) {
	db := NewDB()
	table := NewTable("t", []Column{{Name: "id", Type: IntType}, {Name: "v", Type: IntType}}, false)
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	before := table.StructVersion()
	if _, err := applyOperation(db, &WALRecord{
		OpType: WALOpInsert, Tenant: "default", Table: "t", AfterImage: []any{1, 10},
	}); err != nil {
		t.Fatal(err)
	}
	if got := table.StructVersion(); got != before {
		t.Fatalf("insert: StructVersion()=%d, want unchanged %d", got, before)
	}
	if len(table.Rows) != 1 {
		t.Fatalf("insert: len(Rows)=%d, want 1", len(table.Rows))
	}

	// An update whose before-image is not found falls back to an append —
	// must not advance StructVersion either.
	if _, err := applyOperation(db, &WALRecord{
		OpType: WALOpUpdate, Tenant: "default", Table: "t",
		BeforeImage: []any{999, 999}, AfterImage: []any{2, 20},
	}); err != nil {
		t.Fatal(err)
	}
	if got := table.StructVersion(); got != before {
		t.Fatalf("update-not-found (append fallback): StructVersion()=%d, want unchanged %d", got, before)
	}
	if len(table.Rows) != 2 {
		t.Fatalf("update-not-found: len(Rows)=%d, want 2", len(table.Rows))
	}

	// A genuine in-place update must advance StructVersion.
	if _, err := applyOperation(db, &WALRecord{
		OpType: WALOpUpdate, Tenant: "default", Table: "t",
		BeforeImage: []any{1, 10}, AfterImage: []any{1, 11},
	}); err != nil {
		t.Fatal(err)
	}
	afterUpdate := table.StructVersion()
	if afterUpdate == before {
		t.Fatalf("in-place update: StructVersion()=%d, want advanced past %d", afterUpdate, before)
	}
	if len(table.Rows) != 2 {
		t.Fatalf("in-place update: len(Rows)=%d, want still 2", len(table.Rows))
	}

	// A delete whose before-image is not found removes nothing — must not
	// advance StructVersion.
	if _, err := applyOperation(db, &WALRecord{
		OpType: WALOpDelete, Tenant: "default", Table: "t", BeforeImage: []any{999, 999},
	}); err != nil {
		t.Fatal(err)
	}
	if got := table.StructVersion(); got != afterUpdate {
		t.Fatalf("delete-not-found: StructVersion()=%d, want unchanged %d", got, afterUpdate)
	}

	// A genuine delete must advance StructVersion.
	if _, err := applyOperation(db, &WALRecord{
		OpType: WALOpDelete, Tenant: "default", Table: "t", BeforeImage: []any{1, 11},
	}); err != nil {
		t.Fatal(err)
	}
	if got := table.StructVersion(); got == afterUpdate {
		t.Fatalf("in-place delete: StructVersion()=%d, want advanced past %d", got, afterUpdate)
	}
	if len(table.Rows) != 1 {
		t.Fatalf("in-place delete: len(Rows)=%d, want 1", len(table.Rows))
	}
}
