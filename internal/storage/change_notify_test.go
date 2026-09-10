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

func TestScopedChangeWatchRoutingAndCoalescing(t *testing.T) {
	db := NewDB()
	defer db.Close()
	a, err := db.WatchTableChanges([]TableRef{{Tenant: "one", Table: "items"}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.WatchTableChanges([]TableRef{{Tenant: "two", Table: "items"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for i := 0; i < 2; i++ {
		db.LockContentForWrite()
		db.UnlockContentForWriteTables([]TableRef{{Tenant: "one", Table: "ITEMS"}})
	}
	if got := a.Stats(); got.Notifications != 2 || got.Coalesced != 1 {
		t.Fatal(got)
	}
	if got := b.Stats(); got.Notifications != 0 {
		t.Fatal(got)
	}
	<-a.C
	a.SetTables([]TableRef{{Tenant: "two", Table: "items"}})
	db.LockContentForWrite()
	db.UnlockContentForWriteTables([]TableRef{{Tenant: "one", Table: "items"}})
	if got := a.Stats(); got.Notifications != 2 {
		t.Fatal(got)
	}
	db.LockContentForWrite()
	db.UnlockContentForWrite()
	if got := a.Stats(); got.Broadcasts != 1 || got.Notifications != 3 {
		t.Fatal(got)
	}
	if got := b.Stats(); got.Broadcasts != 1 || got.Notifications != 1 {
		t.Fatal(got)
	}
}
