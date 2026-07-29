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

func TestRowUpdateStatementSnapshotRestoresOnlyCandidatesAndMetadata(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	table := NewTable("items", []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BlobType}}, false)
	table.Rows = [][]any{{1, []byte("one")}, {2, []byte("two")}}
	table.Version = 7
	table.dirtyFrom = 1
	table.dirtyRows = []int{0}
	table.dirtyRowsState = dirtyRowsExact
	table.Stats = &TableStats{RowCount: 2, Columns: map[string]ColumnStats{"id": {DistinctCount: 2}}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowUpdateStatement("default", "items", []int{1})
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.Rows[0] = []any{10, []byte("ten")}
	table.Rows[1] = []any{20, []byte("twenty")}
	table.Version++
	table.dirtyFrom = 0
	table.dirtyRows = []int{1}
	table.Stats = &TableStats{RowCount: 2, Stale: true}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if got := table.Rows[0][0]; got != 10 {
		t.Fatalf("non-candidate row was restored: got %v, want 10", got)
	}
	if got := table.Rows[1][0]; got != 2 {
		t.Fatalf("candidate row after restore = %v, want 2", got)
	}
	if got := string(table.Rows[1][1].([]byte)); got != "two" {
		t.Fatalf("candidate BLOB after restore = %q, want two", got)
	}
	if table.Version != 7 || table.dirtyFrom != 1 || table.dirtyRowsState != dirtyRowsExact {
		t.Fatalf("metadata after restore = version %d, dirtyFrom %d, dirty state %d", table.Version, table.dirtyFrom, table.dirtyRowsState)
	}
	if len(table.dirtyRows) != 1 || table.dirtyRows[0] != 0 {
		t.Fatalf("dirty rows after restore = %#v, want [0]", table.dirtyRows)
	}
	if table.Stats == nil || table.Stats.Stale || table.Stats.Columns["id"].DistinctCount != 2 {
		t.Fatalf("statistics were not restored: %#v", table.Stats)
	}
}

func TestRowDeleteStatementSnapshotReinsertsCandidateAndMetadata(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })

	table := NewTable("items", []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BlobType}}, false)
	table.Rows = [][]any{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}}
	table.Version = 9
	table.dirtyFrom = 2
	table.Stats = &TableStats{RowCount: 3, Columns: map[string]ColumnStats{"id": {DistinctCount: 3}}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowDeleteStatement("default", "items", []int{1})
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	copy(table.Rows[1:], table.Rows[2:])
	table.Rows[len(table.Rows)-1] = nil
	table.Rows = table.Rows[:len(table.Rows)-1]
	table.Version++
	table.dirtyFrom = -1
	table.Stats = &TableStats{RowCount: 2, Stale: true}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if len(table.Rows) != 3 || table.Rows[0][0] != 1 || table.Rows[1][0] != 2 || table.Rows[2][0] != 3 {
		t.Fatalf("rows after point-delete restore = %#v", table.Rows)
	}
	if got := string(table.Rows[1][1].([]byte)); got != "two" {
		t.Fatalf("restored BLOB = %q, want two", got)
	}
	if table.Version != 9 || table.dirtyFrom != 2 {
		t.Fatalf("metadata after restore = version %d, dirtyFrom %d; want 9, 2", table.Version, table.dirtyFrom)
	}
	if table.Stats == nil || table.Stats.Stale || table.Stats.Columns["id"].DistinctCount != 3 {
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
	if _, err := db.SnapshotForRowUpdateStatement("default", "missing", []int{0}); err == nil {
		t.Fatal("row-update snapshot of missing table succeeded")
	}
	if _, err := db.SnapshotForRowDeleteStatement("default", "missing", []int{0}); err == nil {
		t.Fatal("row-delete snapshot of missing table succeeded")
	}
}

// TestCollectWALChangesFromMetaSnapshot covers the pre-image the engine diffs
// against for WALManager logging: a row-less metadata snapshot must report an
// updated table, a created table and a dropped table alike.
func TestCollectWALChangesFromMetaSnapshot(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := NewTable("items", []Column{{Name: "id", Type: IntType}}, false)
	table.Rows = [][]any{{1}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	doomed := NewTable("doomed", []Column{{Name: "id", Type: IntType}}, false)
	if err := db.Put("default", doomed); err != nil {
		t.Fatal(err)
	}

	before := db.MetaSnapshot()

	db.LockContentForWrite()
	table.Rows[0][0] = 2
	table.Version++
	db.UnlockContentForWrite()
	if err := db.Put("default", NewTable("fresh", []Column{{Name: "id", Type: IntType}}, false)); err != nil {
		t.Fatal(err)
	}
	if err := db.Drop("default", "doomed"); err != nil {
		t.Fatal(err)
	}

	changes := CollectWALChanges(before, db)
	got := make(map[string]bool, len(changes))
	for _, ch := range changes {
		got[ch.Name] = ch.Drop
	}
	if len(changes) != 3 {
		t.Fatalf("changes = %#v, want one entry each for items, fresh and doomed", changes)
	}
	if drop, ok := got["items"]; !ok || drop {
		t.Errorf("items: want an update, got drop=%v present=%v", drop, ok)
	}
	if drop, ok := got["fresh"]; !ok || drop {
		t.Errorf("fresh: want a create, got drop=%v present=%v", drop, ok)
	}
	if drop, ok := got["doomed"]; !ok || !drop {
		t.Errorf("doomed: want a drop, got drop=%v present=%v", drop, ok)
	}
}
