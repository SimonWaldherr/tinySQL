package storage

import (
	"reflect"
	"testing"
)

// SnapshotForRowDeleteStatement is chosen for a single-candidate DELETE even when
// the statement runs the general scan (RETURNING, a predicate the point path does
// not take). That scan closes the gap and logs a removal; a rollback must put
// the row back where it was and must not keep the removal.
func TestRowDeleteSnapshotRollbackAfterAClosedUpDelete(t *testing.T) {
	for rowID := 0; rowID < 5; rowID++ {
		for _, closeUp := range []bool{true, false} {
			db := NewDB()
			table := NewTable("chunks", []Column{{Name: "id", Type: IntType}}, false)
			for i := 0; i < 5; i++ {
				table.Rows = append(table.Rows, []any{i})
			}
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			before := append([][]any(nil), table.Rows...)
			version := table.StructVersion()

			db.LockContentForWrite()
			snapshot, err := db.SnapshotForRowDeleteStatement("default", "chunks", []int{rowID})
			if err != nil {
				db.UnlockContentForWrite()
				t.Fatal(err)
			}
			if closeUp {
				table.Rows = append(append([][]any(nil), table.Rows[:rowID]...), table.Rows[rowID+1:]...)
				table.MarkRowsRemoved([]int{rowID})
			} else {
				// The point DELETE: swap-and-pop.
				last := len(table.Rows) - 1
				table.Rows[rowID] = table.Rows[last]
				table.Rows = table.Rows[:last]
				table.MarkDirtyFrom(-1)
			}
			db.RestoreStatementSnapshot(snapshot)
			db.UnlockContentForWrite()

			if !reflect.DeepEqual(table.Rows, before) {
				t.Fatalf("row %d closeUp=%v: rows after restore: %v", rowID, closeUp, table.Rows)
			}
			if got, ok := table.ChangesSince(version, 5, 0); ok {
				t.Fatalf("row %d closeUp=%v: the rolled back removal survived: %+v", rowID, closeUp, got)
			}
			_ = db.Close()
		}
	}
}
