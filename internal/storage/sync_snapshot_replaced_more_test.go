package storage

import (
	"path/filepath"
	"testing"
)

// Follow-ups to sync_snapshot_replaced_test.go: the flag that makes the next
// Sync write a replaced table's images must not move the search-index
// generations, a superseded write must retry cleanly, and the swap must also
// work for the object a real stream-DML detach installs.

// A statement that holds the content write lock reads the table's search-index
// generations without the derived-state lock, on the premise that only a search
// run by that statement can move them. The flag a Sync sets on a replaced table
// (it holds no content lock) must therefore leave the generations alone, or a
// rolled back statement would throw the table's images away because of a Sync.
func TestFlagSearchImagesLeavesGenerationsAndRollbackAlone(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := searchImageTable(t, "docs")
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	generation, _ := table.FTSIndexesPersistenceState()
	table.MarkFTSIndexesPersisted(generation)
	vgeneration, _ := table.VectorIndexesPersistenceState()
	table.MarkVectorIndexesPersisted(vgeneration)

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "docs")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	// A Sync flags the images while the statement runs.
	table.flagSearchImagesForNextSync()
	if table.ftsGeneration != generation || table.vectorGeneration != vgeneration {
		t.Errorf("the flag moved the generations to %d/%d, were %d/%d", table.ftsGeneration, table.vectorGeneration, generation, vgeneration)
	}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if len(table.FTSIndexes) != 1 || len(table.VectorIndexes) != 1 {
		t.Fatalf("a rolled back statement dropped the images: FTS=%d ANN=%d", len(table.FTSIndexes), len(table.VectorIndexes))
	}
	if _, dirty := table.FTSIndexesPersistenceState(); !dirty {
		t.Fatal("the FTS images lost their flag")
	}
	if _, dirty := table.VectorIndexesPersistenceState(); !dirty {
		t.Fatal("the ANN images lost their flag")
	}
	// A second flag on an already flagged table changes nothing, and the next
	// persist clears it exactly once.
	table.flagSearchImagesForNextSync()
	g, dirty := table.FTSIndexesPersistenceState()
	if !dirty || g != generation {
		t.Fatalf("after a second flag: generation %d dirty=%v", g, dirty)
	}
	table.MarkFTSIndexesPersisted(g)
	if _, dirty := table.FTSIndexesPersistenceState(); dirty {
		t.Fatal("persisting the current generation did not clear the flag")
	}
}

// A table without images is not flagged at all.
func TestFlagSearchImagesSkipsTablesWithoutImages(t *testing.T) {
	table := snapshotTestTable(t, "docs", 3)
	table.flagSearchImagesForNextSync()
	if _, dirty := table.FTSIndexesPersistenceState(); dirty {
		t.Fatal("a table without FTS images was flagged")
	}
	if _, dirty := table.VectorIndexesPersistenceState(); dirty {
		t.Fatal("a table without ANN images was flagged")
	}
}

// The first write of the image-less snapshot is superseded by another save of
// the same table (SyncTable, a buffer-pool eviction): the attempt is repeated
// from scratch, the table is still dirty and this time takes the normal path
// with its images. Sync returns nil, the file holds the replacement's rows, and
// nothing stays flagged.
func TestSyncRetriesASupersededImagelessWrite(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	defer db.Close()
	old := searchImageTable(t, "docs")
	if err := db.Put("default", old); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	old.Rows = append(old.Rows, []any{3, "gamma", []float64{1, 1}})
	old.Version++
	db.UnlockContentForWrite()

	replacement := searchImageTable(t, "docs")
	replacement.Rows = append(replacement.Rows, []any{3, "gamma", []float64{1, 1}}, []any{4, "delta", []float64{0, 2}})
	replacement.Version = old.Version + 1

	imageCopies, writes := 0, 0
	setImagesHook(t, func() {
		imageCopies++
		if imageCopies == 1 {
			installReplacement(t, db, replacement)
		}
	})
	setSyncHook(t, func() {
		writes++
		if writes != 1 {
			return
		}
		// Another save of the same table lands between the snapshot and its write.
		db.LockContentForWrite()
		defer db.UnlockContentForWrite()
		if err := db.backend.SaveTable("default", replacement); err != nil {
			t.Error(err)
		}
		replacement.Rows = append(replacement.Rows, []any{5, "epsilon", []float64{2, 2}})
		replacement.Version++ // dirty again
	})
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	syncImagesHook.Store(nil)
	syncSnapshotHook.Store(nil)
	if imageCopies != 2 || writes != 2 {
		t.Fatalf("image copies=%d snapshot rounds=%d, want 2 and 2 (one superseded attempt, one retry)", imageCopies, writes)
	}
	if imageCopies > maxSnapshotAttempts {
		t.Fatalf("%d attempts, the bound is %d", imageCopies, maxSnapshotAttempts)
	}
	dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
	if len(dt.Rows) != 5 || dt.Version != replacement.Version {
		t.Fatalf("file holds %d rows at version %d, want 5 at %d", len(dt.Rows), dt.Version, replacement.Version)
	}
	if len(dt.FTSIndexes) != 1 || len(dt.VectorIndexes) != 1 {
		t.Fatalf("file carries FTS=%d ANN=%d images, want 1 and 1 from the retry", len(dt.FTSIndexes), len(dt.VectorIndexes))
	}
	if _, dirty := replacement.FTSIndexesPersistenceState(); dirty {
		t.Fatal("the FTS images are still dirty after the retry wrote them")
	}
	if _, dirty := replacement.VectorIndexesPersistenceState(); dirty {
		t.Fatal("the ANN images are still dirty after the retry wrote them")
	}
	if db.backend.(dirtyTracker).IsDirty("default", "docs", replacement.Version) {
		t.Fatal("the replacement was written but is still dirty")
	}
}

// The object a DML statement installs while a direct SELECT stream is open
// (DetachPinnedTableForWrite, cloneTableForStreamDML) is a swap like a committed
// transaction's: the Sync that started with the old object dirty must write it.
func TestSyncWritesStreamDMLCloneInstalledDuringImageCopy(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	defer db.Close()
	live := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	appendRow(t, live, 10) // dirty when the Sync starts
	db.UnlockContentForWrite()

	var clone *Table
	setImagesHook(t, func() {
		release, ok := db.PinTableForStream(live)
		if !ok {
			t.Error("the table cannot be pinned for a stream")
			return
		}
		defer release()
		db.LockContentForWrite()
		defer db.UnlockContentForWrite()
		if !db.DetachPinnedTableForWrite("default", "docs") {
			t.Error("no stream-DML clone was installed")
			return
		}
		cur, err := db.Get("default", "docs")
		if err != nil {
			t.Error(err)
			return
		}
		if cur == live {
			t.Error("the live object was not replaced")
			return
		}
		appendRow(t, cur, 11)
		clone = cur
	})
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	syncImagesHook.Store(nil)
	if clone == nil {
		t.Fatal("the hook did not install a clone")
	}
	dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
	if len(dt.Rows) != 12 || dt.Version != clone.Version {
		t.Fatalf("Sync returned nil but the file holds %d rows at version %d, want the clone's 12 at %d", len(dt.Rows), dt.Version, clone.Version)
	}
	if got := rowsAfterReopen(t, dir, ModeDisk, "docs"); got != 12 {
		t.Fatalf("a reopened copy has %d rows, want 12", got)
	}
	if db.backend.(dirtyTracker).IsDirty("default", "docs", clone.Version) {
		t.Fatal("the clone was written but is still dirty")
	}
}
