package storage

import "testing"

func TestTableScopedStatementSnapshotRestoresOnlyTarget(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	target := NewTable("target", []Column{{Name: "id", Type: IntType}}, false)
	target.Rows = [][]any{{1}}
	other := NewTable("other", []Column{{Name: "id", Type: IntType}}, false)
	other.Rows = [][]any{{10}}
	if err := db.Put("default", target); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("default", other); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForTableStatement("default", "target")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	target.Rows = append(target.Rows, []any{2})
	other.Rows = append(other.Rows, []any{20})
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if got := len(target.Rows); got != 1 {
		t.Fatalf("target rows after restore = %d, want 1", got)
	}
	if got := len(other.Rows); got != 2 {
		t.Fatalf("unrelated table was restored too: got %d rows, want 2", got)
	}
}

func TestAppendOnlyStatementSnapshotTruncatesRowsAndRestoresMetadata(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	table := NewTable("items", []Column{{Name: "id", Type: IntType}}, false)
	table.Rows = [][]any{{1}}
	table.Version = 7
	table.dirtyFrom = 1
	table.Stats = &TableStats{RowCount: 1, Columns: map[string]ColumnStats{"id": {DistinctCount: 1}}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForAppendOnlyTableStatement("default", "items")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.Rows = append(table.Rows, []any{2})
	table.Version++
	table.dirtyFrom = 0
	table.Stats = &TableStats{RowCount: 2}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if got := len(table.Rows); got != 1 {
		t.Fatalf("rows after restore = %d, want 1", got)
	}
	if table.Version != 7 || table.dirtyFrom != 1 {
		t.Fatalf("metadata after restore = version %d, dirtyFrom %d; want 7, 1", table.Version, table.dirtyFrom)
	}
	if table.Stats == nil || table.Stats.RowCount != 1 || table.Stats.Columns["id"].DistinctCount != 1 {
		t.Fatalf("statistics were not restored: %#v", table.Stats)
	}
}

func TestFullStatementSnapshotRestoresAllTablesAndDropsNewTables(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	original := NewTable("original", []Column{{Name: "id", Type: IntType}}, false)
	original.Rows = [][]any{{1}}
	if err := db.Put("default", original); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot := db.SnapshotForStatement()
	original.Rows[0][0] = 2
	if err := db.Put("default", NewTable("created", []Column{{Name: "id", Type: IntType}}, false)); err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if got := original.Rows[0][0]; got != 1 {
		t.Fatalf("original row after restore = %v, want 1", got)
	}
	if _, err := db.Get("default", "created"); err == nil {
		t.Fatal("table created after the snapshot survived rollback")
	}
}

func TestTableScopedSnapshotsRejectMissingTable(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	db.LockContentForWrite()
	defer db.UnlockContentForWrite()
	if _, err := db.SnapshotForTableStatement("default", "missing"); err == nil {
		t.Fatal("table-scoped snapshot of missing table succeeded")
	}
	if _, err := db.SnapshotForAppendOnlyTableStatement("default", "missing"); err == nil {
		t.Fatal("append-only snapshot of missing table succeeded")
	}
}

func TestCollectWALChangesFromStatementSnapshot(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := NewTable("items", []Column{{Name: "id", Type: IntType}}, false)
	table.Rows = [][]any{{1}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot := db.SnapshotForStatement()
	table.Rows[0][0] = 2
	table.Version++
	db.UnlockContentForWrite()

	changes := CollectWALChangesFromSnapshot(snapshot, db)
	if len(changes) != 1 || changes[0].Name != "items" || changes[0].Drop {
		t.Fatalf("changes = %#v, want one update for items", changes)
	}
}
