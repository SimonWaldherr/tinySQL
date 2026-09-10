package storage

import "testing"

func TestUpdateHistoryRolloverKeepsLatestDelta(t *testing.T) {
	table := NewTable("t", nil, false)
	previous := table.StructVersion()
	for i := 0; i < 10000; i++ {
		table.noteRowUpdated(i % 3)
		rows, ok := table.UpdatedRowsSince(previous)
		if !ok || len(rows) != 1 || rows[0] != i%3 {
			t.Fatalf("update %d: %v %v", i, rows, ok)
		}
		previous = table.StructVersion()
	}
	if _, ok := table.UpdatedRowsSince(0); ok {
		t.Fatal("expired history accepted")
	}
}
func TestChangeListenerLifecycle(t *testing.T) {
	db := NewDB()
	ch, cancel, err := db.WatchChanges()
	if err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	db.UnlockContentForWrite()
	select {
	case <-ch:
	default:
		t.Fatal("missing wake")
	}
	cancel()
	cancel()
	if db.watchCount.Load() != 0 {
		t.Fatal("listener retained")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.WatchChanges(); err == nil {
		t.Fatal("closed DB accepted listener")
	}
}
