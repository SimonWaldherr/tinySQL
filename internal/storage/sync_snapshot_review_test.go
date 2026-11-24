package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func setImagesHook(t *testing.T, f func()) {
	t.Helper()
	syncImagesHook.Store(&f)
	t.Cleanup(func() { syncImagesHook.Store(nil) })
}

func setTableFileWriteHook(t *testing.T, h *tableFileWriteTestHook) {
	t.Helper()
	tableFileWriteHook.Store(h)
	t.Cleanup(func() { tableFileWriteHook.Store(nil) })
}

// waitOrFail waits for done for d.
func waitOrFail(t *testing.T, done <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("timed out: %s", what)
	}
}

// An FTS or ANN index build holds a table's derived-state lock for its whole
// duration. A Sync must not wait for it inside the content lock's read side: a
// writer queued behind that read lock parks every later reader, so the whole
// database would stall for the length of the build.
func TestSyncNeverWaitsForDerivedLockInsideContentLock(t *testing.T) {
	db := openDiskDB(t, t.TempDir(), ModeDisk)
	defer db.Close()
	live := snapshotTestTable(t, "docs", 20)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	appendRow(t, live, 20)
	db.UnlockContentForWrite()

	live.DerivedLock() // an index build is running
	locked := true
	defer func() {
		if locked {
			live.DerivedUnlock()
		}
	}()
	syncDone := make(chan struct{})
	var syncErr error
	go func() {
		syncErr = db.Sync()
		close(syncDone)
	}()
	// Sync parks in copySearchImages, outside the content lock: a writer must
	// get through at once.
	time.Sleep(100 * time.Millisecond)
	writerDone := make(chan struct{})
	go func() {
		db.LockContentForWrite()
		db.UnlockContentForWrite()
		close(writerDone)
	}()
	waitOrFail(t, writerDone, 5*time.Second, "a writer could not take the content lock while Sync waited for an index build")
	select {
	case <-syncDone:
		t.Fatal("Sync finished although the table's derived-state lock was held")
	default:
	}
	locked = false
	live.DerivedUnlock()
	waitOrFail(t, syncDone, 10*time.Second, "Sync did not finish after the index build ended")
	if syncErr != nil {
		t.Fatal(syncErr)
	}
}

// With the search-index images skipped the derived-state lock is never needed.
func TestSyncWithSkippedImagesNeverTouchesDerivedLock(t *testing.T) {
	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: t.TempDir(), SkipSearchIndexPersistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	live := snapshotTestTable(t, "docs", 20)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	appendRow(t, live, 20)
	db.UnlockContentForWrite()

	live.DerivedLock()
	skipLocked := true
	defer func() {
		if skipLocked {
			live.DerivedUnlock()
		}
	}()
	done := make(chan struct{})
	var syncErr error
	go func() {
		syncErr = db.Sync()
		close(done)
	}()
	waitOrFail(t, done, 10*time.Second, "Sync waited for the derived-state lock although no image is written")
	skipLocked = false
	live.DerivedUnlock()
	if syncErr != nil {
		t.Fatal(syncErr)
	}
}

// A table that becomes dirty only after the images were copied is left for the
// next Sync: its images were not copied, and copying them inside the content
// lock would reintroduce the stall.
func TestSyncLeavesTablesDirtiedAfterImageCopyForTheNextSync(t *testing.T) {
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
	appendRow(t, a, 10)
	db.UnlockContentForWrite()
	setImagesHook(t, func() {
		db.LockContentForWrite()
		appendRow(t, b, 10) // dirty only now
		db.UnlockContentForWrite()
	})
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
	if !db.backend.(dirtyTracker).IsDirty("default", "b", b.Version) {
		t.Fatal("b was marked clean without being written")
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if dt := readRawDiskTable(t, filepath.Join(dir, "default", "b.tbl")); len(dt.Rows) != 11 {
		t.Fatalf("b holds %d rows after the next Sync, want 11", len(dt.Rows))
	}
}

// An image copied before the rows may be older than them; rows that were
// updated, deleted and appended in between must still come out right after a
// cold reopen (the loader extends or rebuilds a stale image, it never trusts it).
func TestImageOlderThanSnapshotRowsReopensConsistently(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	live := snapshotTestTable(t, "docs", 30)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	appendRow(t, live, 30)
	db.UnlockContentForWrite()
	setImagesHook(t, func() {
		db.LockContentForWrite()
		live.Rows[2] = []any{2, "updated after the image copy", []float64{2, 2}}
		last := len(live.Rows) - 1
		live.Rows[5] = live.Rows[last] // swap-remove
		live.Rows[last] = nil
		live.Rows = live.Rows[:last]
		appendRow(t, live, 31)
		live.Version++
		if err := live.RebuildSecondaryIndexes(); err != nil {
			t.Fatal(err)
		}
		db.UnlockContentForWrite()
	})
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	syncImagesHook.Store(nil)
	want := imageOf(tableToDisk("default", live))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openDiskDB(t, dir, ModeDisk)
	defer reopened.Close()
	got, err := reopened.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	have := imageOf(tableToDisk("default", got))
	if fmt.Sprint(have.rows) != fmt.Sprint(want.rows) {
		t.Fatal("rows after reopen differ from the live table")
	}
	for name, entries := range want.entries {
		if len(have.entries[name]) != len(entries) {
			t.Fatalf("index %s has %d entries after reopen, want %d", name, len(have.entries[name]), len(entries))
		}
	}
}

// The snapshot's columns must not share the pointers of the live ones: RENAME
// COLUMN rewrites a ForeignKeyRef in place while the encoder reads it.
func TestSnapshotColumnsDoNotShareForeignKeyOrDefaults(t *testing.T) {
	live := NewTable("child", []Column{
		{Name: "id", Type: IntType},
		{Name: "parent_id", Type: IntType, Constraint: ForeignKey, ForeignKey: &ForeignKeyRef{Table: "parent", Column: "id"}},
		{Name: "tags", Type: BlobType, HasDefault: true, DefaultValue: []byte{1, 2, 3}},
	}, false)
	live.Version = 1
	snap := takeTableSnapshot("default", live, snapshotToken{})
	defer snap.release()
	live.Cols[1].ForeignKey.Column = "renamed" // what RENAME COLUMN does
	live.Cols[2].DefaultValue.([]byte)[0] = 9
	if got := snap.table.Cols[1].ForeignKey.Column; got != "id" {
		t.Fatalf("snapshot foreign key column = %q, the live rename leaked into it", got)
	}
	if got := snap.table.Cols[2].DefaultValue.([]byte)[0]; got != 1 {
		t.Fatalf("snapshot default value changed with the live table (%d)", got)
	}
}

// DROP TABLE that runs while a save of the table is encoding must win, for the
// plain save path (no snapshot) as well.
func TestPlainSaveDoesNotResurrectTableDroppedWhileEncoding(t *testing.T) {
	for _, hybrid := range []bool{false, true} {
		t.Run(fmt.Sprintf("hybrid=%v", hybrid), func(t *testing.T) {
			dir := t.TempDir()
			var save func(*Table) error
			var del func() error
			var exists func() bool
			var inPool func() bool
			if hybrid {
				h, err := NewHybridBackend(dir, 64<<20, false, ModeHybrid)
				if err != nil {
					t.Fatal(err)
				}
				save = func(tb *Table) error { return h.SaveTable("default", tb) }
				del = func() error { return h.DeleteTable("default", "docs") }
				exists = func() bool { return h.TableExists("default", "docs") }
				inPool = func() bool { _, ok := h.pool.Get("default", "docs"); return ok }
			} else {
				b, err := NewDiskBackend(dir, false)
				if err != nil {
					t.Fatal(err)
				}
				save = func(tb *Table) error { return b.SaveTable("default", tb) }
				del = func() error { return b.DeleteTable("default", "docs") }
				exists = func() bool { return b.TableExists("default", "docs") }
				inPool = func() bool { return false }
			}
			live := snapshotTestTable(t, "docs", 10)
			if err := save(live); err != nil {
				t.Fatal(err)
			}

			parked := make(chan struct{})
			release := make(chan struct{})
			setTableFileWriteHook(t, &tableFileWriteTestHook{Before: func(string) error {
				close(parked)
				<-release
				return nil
			}})
			appendRow(t, live, 10)
			saved := make(chan error, 1)
			go func() { saved <- save(live) }()
			waitOrFail(t, parked, 5*time.Second, "the save never started writing")
			// The write holds no b.mu: DeleteTable must not wait for it.
			delDone := make(chan error, 1)
			go func() { delDone <- del() }()
			select {
			case err := <-delDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("DeleteTable waited for the save's encode")
			}
			tableFileWriteHook.Store(nil)
			close(release)
			if err := <-saved; err != nil {
				t.Fatalf("save of a concurrently dropped table: %v", err)
			}
			if exists() {
				t.Fatal("the dropped table is known to the backend again")
			}
			if _, err := os.Stat(filepath.Join(dir, "default", "docs.tbl")); !os.IsNotExist(err) {
				t.Fatalf("the dropped table's file is back: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "default", "docs.tbl.tmp")); !os.IsNotExist(err) {
				t.Fatalf("temporary file left behind: %v", err)
			}
			if inPool() {
				t.Fatal("the dropped table was cached in the pool")
			}
		})
	}
}

// A table the backend never persisted (created in a transaction, dropped before
// any Sync wrote it) is not resurrected by a snapshot that was being written.
func TestSyncDoesNotResurrectNeverPersistedTableDroppedDuringTheWrite(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			db := openDiskDB(t, dir, mode)
			defer db.Close()
			tb := snapshotTestTable(t, "ghost", 10)
			db.LockContentForWrite()
			if err := db.ApplyWALChanges([]WALChange{{Tenant: "default", Name: "ghost", Table: tb}}); err != nil {
				t.Fatal(err)
			}
			db.UnlockContentForWrite()
			if db.backend.TableExists("default", "ghost") {
				t.Fatal("test premise: the backend must not know the table yet")
			}

			// The DROP arrives while the write of the snapshot is in flight.
			released := make(chan struct{})
			setTableFileWriteHook(t, &tableFileWriteTestHook{Before: func(string) error {
				db.LockContentForWrite()
				err := db.Drop("default", "ghost")
				db.UnlockContentForWrite()
				close(released)
				return err
			}})
			if err := db.Sync(); err != nil {
				t.Fatalf("Sync with a concurrent DROP: %v", err)
			}
			tableFileWriteHook.Store(nil)
			<-released
			if db.TableExists("default", "ghost") || db.backend.TableExists("default", "ghost") {
				t.Fatal("dropped table exists after Sync")
			}
			if _, err := os.Stat(filepath.Join(dir, "default", "ghost.tbl")); !os.IsNotExist(err) {
				t.Fatalf("dropped table's file exists: %v", err)
			}
			if h, ok := db.backend.(*HybridBackend); ok {
				if _, cached := h.pool.Get("default", "ghost"); cached {
					t.Fatal("dropped table is cached in the pool")
				}
			}
		})
	}
}

// A DROP that runs between a snapshot's install and the pool mirroring must
// not leave the dropped table cached, and must not remove a newer table that
// was created under the same name meanwhile.
func TestHybridSnapshotDoesNotCacheTableDroppedAfterInstall(t *testing.T) {
	db := openDiskDB(t, t.TempDir(), ModeHybrid)
	defer db.Close()
	h := db.backend.(*HybridBackend)
	live := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	snap := takeTableSnapshot("default", live, h.beginTableSnapshot("default", "docs"))
	defer snap.release()
	hook := func() {
		if err := h.DeleteTable("default", "docs"); err != nil {
			t.Error(err)
		}
	}
	hybridSnapshotInstalledHook.Store(&hook)
	defer hybridSnapshotInstalledHook.Store(nil)
	if err := h.saveTableSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if _, cached := h.pool.Get("default", "docs"); cached {
		t.Fatal("the dropped table is cached in the pool")
	}

	// A newer table under the same name is left alone.
	newer := snapshotTestTable(t, "docs", 3)
	if err := h.SaveTable("default", newer); err != nil {
		t.Fatal(err)
	}
	recreated := snapshotTestTable(t, "docs", 4)
	snap2 := takeTableSnapshot("default", newer, h.beginTableSnapshot("default", "docs"))
	defer snap2.release()
	hook2 := func() {
		if err := h.DeleteTable("default", "docs"); err != nil {
			t.Error(err)
		}
		if err := h.SaveTable("default", recreated); err != nil { // re-created
			t.Error(err)
		}
	}
	hybridSnapshotInstalledHook.Store(&hook2)
	if err := h.saveTableSnapshot(snap2); err != nil {
		t.Fatal(err)
	}
	if cached, ok := h.pool.Get("default", "docs"); !ok || cached != recreated {
		t.Fatal("the re-created table is no longer cached")
	}
}

// A Sync that is superseded again and again does not fall back to writing the
// live table under the content lock; it reports an error, leaves what was
// written intact, and the next Sync (without interference) settles everything.
func TestSyncSupersededRepeatedlyFailsInsteadOfStoppingTheWorld(t *testing.T) {
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
	appendRow(t, live, 10)
	db.UnlockContentForWrite()

	attempts := 0
	setSyncHook(t, func() {
		attempts++
		db.LockContentForWrite()
		defer db.UnlockContentForWrite()
		appendRow(t, live, 100+attempts)
		if err := db.backend.SaveTable("default", live); err != nil { // a SyncTable
			t.Error(err)
		}
		appendRow(t, live, 200+attempts) // dirty again
	})
	err := db.Sync()
	if err == nil || !strings.Contains(err.Error(), "could not settle") {
		t.Fatalf("Sync = %v, want a could-not-settle error", err)
	}
	if attempts != maxSnapshotAttempts {
		t.Fatalf("%d attempts, want %d", attempts, maxSnapshotAttempts)
	}
	if !db.backend.(dirtyTracker).IsDirty("default", "docs", live.Version) {
		t.Fatal("the table was marked clean although its latest state was never written")
	}
	syncSnapshotHook.Store(nil)
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync without interference: %v", err)
	}
	if dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl")); len(dt.Rows) != len(live.Rows) {
		t.Fatalf("file holds %d rows, want %d", len(dt.Rows), len(live.Rows))
	}
}

// A failure after some bytes were written (not just at creating the file)
// leaves the old file intact, no temporary file and the table dirty; so does a
// failing rename; and when the first of two tables fails the second is not
// marked clean either, while a table written before the failure is.
func TestSnapshotWriteFailureModes(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		for _, tc := range []string{"mid-write", "rename"} {
			t.Run(mode.String()+"/"+tc, func(t *testing.T) {
				dir := t.TempDir()
				db := openDiskDB(t, dir, mode)
				defer db.Close()
				good := snapshotTestTable(t, "a_good", 20)
				bad := snapshotTestTable(t, "b_bad", 20)
				for _, tb := range []*Table{good, bad} {
					if err := db.Put("default", tb); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Sync(); err != nil {
					t.Fatal(err)
				}
				badPath := filepath.Join(dir, "default", "b_bad.tbl")
				before, err := os.ReadFile(badPath)
				if err != nil {
					t.Fatal(err)
				}
				db.LockContentForWrite()
				appendRow(t, good, 20)
				appendRow(t, bad, 20)
				db.UnlockContentForWrite()

				switch tc {
				case "mid-write":
					setTableFileWriteHook(t, &tableFileWriteTestHook{
						Wrap: func(tmp string, w io.Writer) io.Writer {
							if strings.Contains(tmp, "b_bad") {
								return &failingWriter{w: w, left: 64}
							}
							return w
						},
					})
				case "rename":
					// A non-empty directory where the file must go: the rename fails.
					if err := os.Remove(badPath); err != nil {
						t.Fatal(err)
					}
					if err := os.MkdirAll(filepath.Join(badPath, "blocker"), 0o755); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Sync(); err == nil {
					t.Fatal("Sync succeeded although a table file could not be written")
				}
				tableFileWriteHook.Store(nil)
				if tc == "mid-write" {
					after, err := os.ReadFile(badPath)
					if err != nil {
						t.Fatal(err)
					}
					if string(before) != string(after) {
						t.Fatal("a failed write changed the table file")
					}
				}
				if _, err := os.Stat(badPath + ".tmp"); !os.IsNotExist(err) {
					t.Fatalf("temporary file left behind: %v", err)
				}
				dc := db.backend.(dirtyTracker)
				if !dc.IsDirty("default", "b_bad", bad.Version) {
					t.Fatal("the table whose write failed was marked clean")
				}
				if dc.IsDirty("default", "a_good", good.Version) {
					t.Fatal("the table written before the failure was not marked clean")
				}
				if tc == "rename" {
					if err := os.RemoveAll(badPath); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Sync(); err != nil {
					t.Fatalf("Sync after the failure is gone: %v", err)
				}
				if dt := readRawDiskTable(t, badPath); len(dt.Rows) != 21 {
					t.Fatalf("b_bad holds %d rows after recovery, want 21", len(dt.Rows))
				}
			})
		}
	}
}

// A truncated temporary file left behind by a killed process is neither loaded
// as the table nor an obstacle to the next write.
func TestStaleTemporaryFileIsIgnoredOnReopen(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	live := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "default", "docs.tbl")
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", good[:len(good)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	reopened := openDiskDB(t, dir, ModeDisk)
	got, err := reopened.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 10 {
		t.Fatalf("reopened table has %d rows, want 10", len(got.Rows))
	}
	db2live := got
	reopened.LockContentForWrite()
	appendRow(t, db2live, 10)
	reopened.UnlockContentForWrite()
	if err := reopened.Sync(); err != nil {
		t.Fatalf("Sync over a stale temporary file: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("stale temporary file survived the next write: %v", err)
	}
}

// A pool eviction must not wait for a snapshot write of the same table (it
// holds the pool lock for the whole call): the table stays cached instead.
func TestPoolEvictionVetoedWhileSnapshotWriteInFlight(t *testing.T) {
	a := snapshotTestTable(t, "a", 400)
	bTable := snapshotTestTable(t, "b", 400)
	limit := EstimateTableSize(a) + EstimateTableSize(bTable) - 1
	h, err := NewHybridBackend(t.TempDir(), limit, false, ModeHybrid)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.SaveTable("default", a); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.pool.Get("default", "a"); !ok {
		t.Fatal("test premise: a must be cached")
	}
	a.Version++ // modified in place since it was saved

	// A snapshot write of a is in flight: its file lock is held.
	unlock := h.disk.lockTableFile("default", "a")
	done := make(chan error, 1)
	go func() { done <- h.pool.Put("default", "b", bTable) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the pool eviction waited for the in-flight write of the same table")
	}
	if cached, ok := h.pool.Get("default", "a"); !ok || cached != a {
		t.Fatal("the dirty table was evicted although it could not be saved")
	}
	unlock()

	// Once the write is done the eviction saves and evicts it.
	if err := h.pool.Put("default", "b", bTable); err != nil {
		t.Fatalf("Put after the write ended: %v", err)
	}
	if _, ok := h.pool.Get("default", "a"); ok {
		t.Fatal("a stayed cached although nothing prevents its eviction any more")
	}
	if h.disk.IsDirty("default", "a", a.Version) {
		t.Fatal("a was evicted without being saved")
	}
}

type failingWriter struct {
	w    io.Writer
	left int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, errors.New("injected write failure")
	}
	if len(p) > f.left {
		n, _ := f.w.Write(p[:f.left])
		f.left = 0
		return n, errors.New("injected write failure")
	}
	f.left -= len(p)
	return f.w.Write(p)
}

// While a snapshot is being encoded no lock is held that a statement or a
// metadata call needs: the content lock (both sides), the table's derived-state
// lock, and the backend's own mutex (ListTableNames/TableExists back every
// statement's foreign-key check). The write is parked deterministically.
func TestNoLockHeldWhileTheSnapshotIsEncoded(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			db := openDiskDB(t, t.TempDir(), mode)
			defer db.Close()
			live := snapshotTestTable(t, "docs", 20)
			other := snapshotTestTable(t, "other", 5)
			for _, tb := range []*Table{live, other} {
				if err := db.Put("default", tb); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			db.LockContentForWrite()
			appendRow(t, live, 20)
			db.UnlockContentForWrite()

			parked := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			setTableFileWriteHook(t, &tableFileWriteTestHook{Before: func(string) error {
				once.Do(func() { close(parked) })
				<-release
				return nil
			}})
			syncDone := make(chan error, 1)
			go func() { syncDone <- db.Sync() }()
			waitOrFail(t, parked, 10*time.Second, "the write never started")

			probes := map[string]func(){
				"content write lock": func() { db.LockContentForWrite(); appendRow(t, live, 21); db.UnlockContentForWrite() },
				"content read lock":  func() { db.LockContentForRead(); _ = len(live.Rows); db.UnlockContentForRead() },
				"derived lock":       func() { live.DerivedLock(); live.DerivedUnlock() },
				"TableExists":        func() { _ = db.backend.TableExists("default", "docs") },
				"ListTableNames":     func() { _, _ = db.backend.ListTableNames("default") },
				"IsDirty":            func() { _ = db.backend.(dirtyTracker).IsDirty("default", "docs", live.Version) },
				"Get":                func() { _, _ = db.Get("default", "docs") },
			}
			for name, probe := range probes {
				done := make(chan struct{})
				go func() { probe(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					close(release)
					t.Fatalf("%s is held while the snapshot is encoded", name)
				}
			}
			tableFileWriteHook.Store(nil)
			close(release)
			if err := <-syncDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The catalog sidecar is replaced atomically: a failed write leaves the previous
// catalog, and no temporary file remains.
func TestCatalogSidecarIsReplacedAtomically(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	defer db.Close()
	if err := db.Put("default", snapshotTestTable(t, "docs", 5)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ".catalog.gob")
	before, err := os.ReadFile(path)
	if err != nil || len(before) == 0 {
		t.Fatalf("catalog sidecar after Sync: %d bytes, %v", len(before), err)
	}
	if err := os.Mkdir(path+".tmp", 0o755); err != nil { // the write cannot create its temporary file
		t.Fatal(err)
	}
	if err := db.saveBackendCatalog(); err == nil {
		t.Fatal("saving the catalog succeeded although its temporary file could not be created")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("a failed catalog write changed the sidecar (%v)", err)
	}
	if err := os.Remove(path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := db.saveBackendCatalog(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary catalog file left behind: %v", err)
	}
	// Concurrent writers (a catalog change and a Sync both save it) never
	// interleave their bytes.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if err := db.saveBackendCatalog(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := db.loadBackendCatalog(); err != nil {
		t.Fatalf("the catalog sidecar is unreadable after concurrent saves: %v", err)
	}
}
