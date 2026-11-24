package storage

import "testing"

// A snapshot save mirrors the live table it copied into the buffer pool. When the
// pool holds a different object under the name by then (the live table was evicted
// clean after the install and the next Get reloaded it from the file just written,
// and that object has taken a write since), the newer object stays: replacing it
// would drop its unsaved change, and a later eviction would write the old rows over
// the newer file.
func TestHybridSnapshotMirrorKeepsANewerPoolEntry(t *testing.T) {
	db := openDiskDB(t, t.TempDir(), ModeHybrid)
	defer db.Close()
	h := db.backend.(*HybridBackend)
	live := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	snap := takeTableSnapshot("default", live, h.beginTableSnapshot("default", "docs"))
	defer snap.release()
	reloaded := snapshotTestTable(t, "docs", 12)
	hook := func() {
		h.pool.Remove("default", "docs") // evicted clean after the install
		if err := h.pool.Put("default", "docs", reloaded); err != nil {
			t.Error(err)
		}
	}
	hybridSnapshotInstalledHook.Store(&hook)
	defer hybridSnapshotInstalledHook.Store(nil)
	if err := h.saveTableSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if cached, ok := h.pool.Get("default", "docs"); !ok || cached != reloaded {
		t.Fatal("the snapshot save replaced a newer pool entry with the stale live table")
	}

	// The usual case still mirrors: the pool holds the live table itself, or nothing.
	h.pool.Remove("default", "docs")
	snap2 := takeTableSnapshot("default", live, h.beginTableSnapshot("default", "docs"))
	defer snap2.release()
	hybridSnapshotInstalledHook.Store(nil)
	if err := h.saveTableSnapshot(snap2); err != nil {
		t.Fatal(err)
	}
	if cached, ok := h.pool.Get("default", "docs"); !ok || cached != live {
		t.Fatal("the live table was not mirrored into an empty pool slot")
	}
}
