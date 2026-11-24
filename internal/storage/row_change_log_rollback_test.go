package storage

import (
	"reflect"
	"testing"
)

// A DELETE that logged its removed rows and is then rolled back must not leave
// that removal behind: the restored rows are the old ones again.
func TestRowsOnlySnapshotRollbackDropsALoggedRemoval(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	before := append([][]any(nil), table.Rows...)
	version := table.StructVersion()

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.Rows = append([][]any(nil), table.Rows[0], table.Rows[3])
	table.MarkRowsRemoved([]int{1, 2})
	if _, ok := table.ChangesSince(version, 4, 0); !ok {
		db.UnlockContentForWrite()
		t.Fatal("the removal was not logged")
	}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if !reflect.DeepEqual(table.Rows, before) {
		t.Fatalf("rows after restore: %v", table.Rows)
	}
	if got, ok := table.ChangesSince(version, 4, 0); ok {
		t.Fatalf("the rolled back removal survived the restore: %+v", got)
	}
	if table.StructVersion() <= version {
		t.Fatal("structVersion did not move on")
	}
	// New events after the restore are served to stamps taken after it.
	after := table.StructVersion()
	table.Rows = append([][]any(nil), table.Rows[1:]...)
	table.MarkRowsRemoved([]int{0})
	if got, ok := table.ChangesSince(after, 4, 0); !ok || !reflect.DeepEqual(got.Dead, []int{0}) || got.Survivors != 3 {
		t.Fatalf("removal after the restore: %+v ok=%v", got, ok)
	}
}

// A rolled back UPDATE keeps its entries; they only over-report.
func TestRowsOnlySnapshotRollbackKeepsUpdateEntries(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	version := table.StructVersion()
	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.MarkRowUpdatedCols(1, ColumnMask(1))
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()
	got, ok := table.ChangesSince(version, 4, ColumnMask(1))
	if !ok || !reflect.DeepEqual(got.Updated, []int{1}) || got.Survivors != 4 {
		t.Fatalf("after a rolled back update: %+v ok=%v", got, ok)
	}
}

// A transaction's private copy carries the change log, removals included, and
// advances it without touching the live table's.
func TestTransactionShadowKeepsItsOwnChangeLog(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	version := table.StructVersion()
	shadowTable := cloneTableForTx(table)
	shadowTable.Rows = append([][]any(nil), shadowTable.Rows[1:]...)
	shadowTable.MarkRowsRemoved([]int{0})
	if got, ok := shadowTable.ChangesSince(version, 4, 0); !ok || !reflect.DeepEqual(got.Dead, []int{0}) {
		t.Fatalf("shadow: %+v ok=%v", got, ok)
	}
	if got, ok := table.ChangesSince(version, 4, 0); !ok || len(got.Dead) != 0 || got.Survivors != 4 {
		t.Fatalf("the live table saw the shadow's removal: %+v ok=%v", got, ok)
	}
	copyOfShadow := cloneTable(shadowTable)
	if got, ok := copyOfShadow.ChangesSince(version, 4, 0); !ok || !reflect.DeepEqual(got.Dead, []int{0}) {
		t.Fatalf("clone of the shadow: %+v ok=%v", got, ok)
	}
}
