package storage

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestAdvancedWALRecoveryDeleteClearsRemovedRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.wal")
	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	cols := []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BlobType}}
	table := NewTable("events", cols, false)
	table.Rows = [][]any{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}, {4, []byte("four")}}
	backing := table.Rows
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.LogBegin(1); err != nil {
		t.Fatal(err)
	}
	for _, rowID := range []int{1, 3} {
		if _, err := wal.LogDelete(1, "default", "events", int64(rowID), backing[rowID], cols); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := wal.LogCommit(1); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenAdvancedWAL(AdvancedWALConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := reopened.Recover(db)
	if err != nil || recovered != 2 {
		t.Fatalf("Recover = %d, %v; want 2 operations", recovered, err)
	}
	// The first delete shifts row 4, so the second record's RowID is stale.
	// Replay must preserve the surviving order while releasing both old slots.
	if want := [][]any{{1, []byte("one")}, {3, []byte("three")}}; !reflect.DeepEqual(table.Rows, want) {
		t.Fatalf("rows after recovery = %v, want %v", table.Rows, want)
	}
	for i := len(table.Rows); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("removed row remains reachable through backing slot %d: %v", i, backing[i])
		}
	}
}

func TestReplicaDeleteClearsRowsWithoutChangingPinnedSnapshot(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "single"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			db := NewDB()
			t.Cleanup(func() { _ = db.Close() })
			cols := []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BlobType}}
			table := NewTable("events", cols, false)
			table.Rows = [][]any{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}}
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			source, release := pinReplicaTestTable(t, db, "default", "events")
			defer release()
			record := WALRecord{
				LSN: 1, OpType: WALOpDelete, Tenant: "default", Table: "events",
				RowID: 1, BeforeImage: []any{2, []byte("two")}, Columns: cols,
			}
			if batch {
				const epoch = uint64(91)
				if err := InitializeReplicaState(db, epoch, 0); err != nil {
					t.Fatal(err)
				}
				if applied, err := ApplyReplicaWALRecords(db, epoch, []WALRecord{record}); err != nil || applied != 1 {
					t.Fatalf("ApplyReplicaWALRecords = %d, %v; want 1 operation", applied, err)
				}
			} else if _, err := ApplyWALRecord(db, &record); err != nil {
				t.Fatal(err)
			}
			live, err := db.Get("default", "events")
			if err != nil {
				t.Fatal(err)
			}
			if live == source {
				t.Fatal("delete reused the pinned source table")
			}
			if want := [][]any{{1, []byte("one")}, {3, []byte("three")}}; !reflect.DeepEqual(live.Rows, want) {
				t.Fatalf("live rows = %v, want %v", live.Rows, want)
			}
			if want := [][]any{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}}; !reflect.DeepEqual(source.Rows, want) {
				t.Fatalf("pinned rows changed = %v, want %v", source.Rows, want)
			}
			if backing := live.Rows[:cap(live.Rows)]; backing[2] != nil {
				t.Fatalf("deleted row remains in live backing slot: %v", backing[2])
			}
		})
	}
}
