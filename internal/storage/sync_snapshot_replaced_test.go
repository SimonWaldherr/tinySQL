package storage

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// A table object can be swapped for another one while DB.Sync deep-copies the
// search-index images: a committed explicit transaction installs its private
// copy (ApplyWALChanges), a DML statement issued while a direct SELECT stream is
// open installs a stream-DML clone, Replace swaps an object too. The new object
// carries everything the old one had. These tests pin that a Sync that returns
// nil has put such a table on disk, and that only a table that was clean when
// the Sync started may be left for the next one.

// installReplacement swaps the live object of nt's table for nt, the way a
// committed transaction does.
func installReplacement(t *testing.T, db *DB, nt *Table) {
	t.Helper()
	db.LockContentForWrite()
	defer db.UnlockContentForWrite()
	if err := db.ApplyWALChanges([]WALChange{{Tenant: "default", Name: nt.Name, Table: nt}}); err != nil {
		t.Fatal(err)
	}
}

// copyStorageDir copies a database directory file by file while the database
// is still open, which is what a crash leaves behind for the next process.
func copyStorageDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

// rowsAfterReopen opens a copy of dir with a fresh DB and returns the number of
// rows of the named table.
func rowsAfterReopen(t *testing.T, dir string, mode StorageMode, name string) int {
	t.Helper()
	db := openDiskDB(t, copyStorageDir(t, dir), mode)
	defer db.Close()
	got, err := db.Get("default", name)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatalf("table %s is missing after the reopen", name)
	}
	return len(got.Rows)
}

// A table that was dirty when Sync started and whose object is replaced while
// the images are copied is written by that Sync, with the replacement's rows.
func TestSyncWritesTableReplacedDuringImageCopy(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			db := openDiskDB(t, dir, mode)
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

			replacement := snapshotTestTable(t, "docs", 13)
			replacement.Version = live.Version + 1
			setImagesHook(t, func() { installReplacement(t, db, replacement) })
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			syncImagesHook.Store(nil)

			if dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl")); len(dt.Rows) != 13 || dt.Version != replacement.Version {
				t.Fatalf("Sync returned nil but the file holds %d rows at version %d, want the replacement's 13 at %d", len(dt.Rows), dt.Version, replacement.Version)
			}
			if got := rowsAfterReopen(t, dir, mode, "docs"); got != 13 {
				t.Fatalf("a reopened copy of the directory has %d rows, want 13", got)
			}
			if db.backend.(dirtyTracker).IsDirty("default", "docs", replacement.Version) {
				t.Fatal("the replacement was written but is still dirty")
			}
		})
	}
}

// Starvation: with a writer that swaps the table object during the image copy
// of every Sync, each Sync must still persist the table. Before the fix every
// one of them skipped it and returned nil.
func TestSyncPersistsTableReplacedDuringEveryImageCopy(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	defer db.Close()
	cur := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", cur); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	setImagesHook(t, func() {
		next := snapshotTestTable(t, "docs", len(cur.Rows)+1)
		next.Version = cur.Version + 1
		installReplacement(t, db, next)
		cur = next
	})
	for i := 1; i <= 4; i++ {
		db.LockContentForWrite()
		appendRow(t, cur, 1000+i) // the table is dirty when this Sync starts
		db.UnlockContentForWrite()
		if err := db.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
		want := len(cur.Rows) // the replacement installed during this Sync
		if dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl")); len(dt.Rows) != want {
			t.Fatalf("Sync %d returned nil but the file holds %d rows, want %d", i, len(dt.Rows), want)
		}
	}
	syncImagesHook.Store(nil)
	if got := rowsAfterReopen(t, dir, ModeDisk, "docs"); got != len(cur.Rows) {
		t.Fatalf("a reopened copy has %d rows, want %d", got, len(cur.Rows))
	}
}

// A table that was clean when the Sync started and is replaced by a dirty
// object during the image copy changed after the Sync began: that Sync leaves
// it for the next one (and the next one writes it), exactly as for an object
// that was only modified in place.
func TestSyncLeavesCleanTableReplacedAfterImageCopyForTheNextSync(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	defer db.Close()
	a := snapshotTestTable(t, "a", 10)
	b := snapshotTestTable(t, "b", 10)
	for _, tb := range []*Table{a, b} {
		if err := db.Put("default", tb); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	appendRow(t, a, 10) // only a is dirty when the Sync starts
	db.UnlockContentForWrite()

	nb := snapshotTestTable(t, "b", 12)
	nb.Version = b.Version + 1
	setImagesHook(t, func() { installReplacement(t, db, nb) })
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	syncImagesHook.Store(nil)
	if dt := readRawDiskTable(t, filepath.Join(dir, "default", "a.tbl")); len(dt.Rows) != 11 {
		t.Fatalf("a holds %d rows, want 11", len(dt.Rows))
	}
	if dt := readRawDiskTable(t, filepath.Join(dir, "default", "b.tbl")); len(dt.Rows) != 10 {
		t.Fatalf("b holds %d rows, want the 10 it had when the Sync started", len(dt.Rows))
	}
	if !db.backend.(dirtyTracker).IsDirty("default", "b", nb.Version) {
		t.Fatal("b was marked clean without being written")
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if dt := readRawDiskTable(t, filepath.Join(dir, "default", "b.tbl")); len(dt.Rows) != 12 {
		t.Fatalf("b holds %d rows after the next Sync, want 12", len(dt.Rows))
	}
}

// The replaced table is written without search-index images (the image copied
// from its predecessor must never be attached to a different object), and its
// images are flagged so that the next Sync copies and writes them.
func TestSyncReplacedTableGetsItsImagesOnTheNextSync(t *testing.T) {
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
	// The replacement's images count as persisted, as if its generations were
	// copied from an object whose images were written: only the fix can make the
	// next Sync write them.
	generation, _ := replacement.FTSIndexesPersistenceState()
	replacement.MarkFTSIndexesPersisted(generation)
	generation, _ = replacement.VectorIndexesPersistenceState()
	replacement.MarkVectorIndexesPersisted(generation)

	setImagesHook(t, func() { installReplacement(t, db, replacement) })
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	syncImagesHook.Store(nil)

	dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
	if len(dt.Rows) != 4 {
		t.Fatalf("file holds %d rows, want the replacement's 4", len(dt.Rows))
	}
	if len(dt.FTSIndexes) != 0 || len(dt.VectorIndexes) != 0 {
		t.Fatalf("file carries FTS=%d ANN=%d images, want none (the predecessor's copies are not the replacement's)", len(dt.FTSIndexes), len(dt.VectorIndexes))
	}
	if len(dt.Indexes) != 1 {
		t.Fatalf("secondary indexes were dropped from the file: %d", len(dt.Indexes))
	}
	if _, dirty := replacement.FTSIndexesPersistenceState(); !dirty {
		t.Fatal("the FTS images of the replacement are not flagged for the next Sync")
	}
	if _, dirty := replacement.VectorIndexesPersistenceState(); !dirty {
		t.Fatal("the ANN images of the replacement are not flagged for the next Sync")
	}

	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	dt = readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
	if len(dt.Rows) != 4 || len(dt.FTSIndexes) != 1 || len(dt.VectorIndexes) != 1 {
		t.Fatalf("second Sync: file holds %d rows, FTS=%d ANN=%d, want 4, 1, 1", len(dt.Rows), len(dt.FTSIndexes), len(dt.VectorIndexes))
	}
	if _, dirty := replacement.FTSIndexesPersistenceState(); dirty {
		t.Fatal("the FTS images are still dirty after the second Sync")
	}
}
