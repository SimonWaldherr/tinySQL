package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// snapshotTestTable builds a table with an INT id, a TEXT body, a VECTOR and a
// secondary index on id, n rows long.
func snapshotTestTable(t *testing.T, name string, n int) *Table {
	t.Helper()
	table := NewTable(name, []Column{
		{Name: "id", Type: IntType},
		{Name: "body", Type: TextType},
		{Name: "emb", Type: VectorType},
	}, false)
	for i := 0; i < n; i++ {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("row %d", i), []float64{float64(i), float64(i) + 0.5}})
	}
	table.Version = 1
	if err := table.CreateSecondaryIndex("idx_id", []string{"id"}, false); err != nil {
		t.Fatal(err)
	}
	return table
}

// appendRow mimics an INSERT: append a row, keep the index in step, bump Version.
func appendRow(t *testing.T, table *Table, id int) {
	t.Helper()
	row := []any{id, fmt.Sprintf("row %d", id), []float64{float64(id), 1}}
	table.Rows = append(table.Rows, row)
	if err := table.InsertSecondaryIndexRow(len(table.Rows)-1, row, table.SortedIndexNames()); err != nil {
		t.Fatal(err)
	}
	table.Version++
}

// diskImage is what a table file holds, reduced to what the tests compare.
type diskImage struct {
	rows    [][]any
	version int
	entries map[string][]IndexEntry
}

func imageOf(dt diskTable) diskImage {
	img := diskImage{rows: dt.Rows, version: dt.Version, entries: map[string][]IndexEntry{}}
	for name, idx := range dt.Indexes {
		img.entries[name] = idx.Entries
	}
	return img
}

func openDiskDB(t *testing.T, dir string, mode StorageMode) *DB {
	t.Helper()
	db, err := OpenDB(StorageConfig{Mode: mode, Path: dir, MaxMemoryBytes: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func setSyncHook(t *testing.T, f func()) {
	t.Helper()
	syncSnapshotHook.Store(&f)
	t.Cleanup(func() { syncSnapshotHook.Store(nil) })
}

// The snapshot must keep describing the table as it was when it was taken,
// whatever the live table does afterwards: new rows, replaced rows, a
// swap-removed row, index changes.
func TestTableSnapshotIsImmutableAfterLiveChanges(t *testing.T) {
	for _, hot := range []bool{false, true} {
		t.Run(fmt.Sprintf("hotIndex=%v", hot), func(t *testing.T) {
			live := snapshotTestTable(t, "docs", 50)
			if hot {
				// Force the skip list into existence, as any write does.
				appendRow(t, live, 50)
			} else {
				// A reloaded table's index has Entries only, no skip list.
				live = diskToTable(tableToDisk("default", live))
				if live.Indexes["idx_id"].fast != nil {
					t.Fatal("expected a cold index after a reload")
				}
			}
			want := imageOf(tableToDisk("default", live))

			snap := takeTableSnapshot("default", live, snapshotToken{})
			defer func() { snap.release() }()

			// Live changes after the snapshot.
			appendRow(t, live, 1000)
			appendRow(t, live, 1001)
			live.Rows[3] = []any{3, "replaced", []float64{9, 9}} // UPDATE replaces the slice
			last := len(live.Rows) - 1
			live.Rows[0] = live.Rows[last] // swap-remove
			live.Rows[last] = nil
			live.Rows = live.Rows[:last]
			live.Version++
			if err := live.RebuildSecondaryIndexes(); err != nil {
				t.Fatal(err)
			}

			got := imageOf(snapshotToDisk("default", snap, true))
			if !reflect.DeepEqual(got.rows, want.rows) {
				t.Fatalf("snapshot rows changed with the live table")
			}
			if got.version != want.version {
				t.Fatalf("snapshot version = %d, want %d", got.version, want.version)
			}
			if !reflect.DeepEqual(got.entries, want.entries) {
				t.Fatalf("snapshot index changed with the live table")
			}
		})
	}
}

// A snapshot that was written and released must not make the live index copy
// its skip list on its next write; one that is still held must.
func TestSnapshotReleaseKeepsLiveIndexWritesCheap(t *testing.T) {
	live := snapshotTestTable(t, "docs", 20)
	appendRow(t, live, 20) // hydrate the skip list
	idx := live.Indexes["idx_id"]
	before := idx.fast

	snap := takeTableSnapshot("default", live, snapshotToken{})
	if idx.fast != before || before.sharers.Load() != 1 {
		t.Fatalf("lending the index must share its list (sharers=%d)", before.sharers.Load())
	}
	snap.release()
	if before.sharers.Load() != 0 {
		t.Fatalf("release left sharers = %d", before.sharers.Load())
	}
	appendRow(t, live, 21)
	if idx.fast != before {
		t.Fatal("a write after release copied the skip list")
	}

	held := takeTableSnapshot("default", live, snapshotToken{})
	appendRow(t, live, 22)
	if idx.fast == before {
		t.Fatal("a write while a snapshot still reads the list must copy it")
	}
	// The snapshot still sees the list as it was, and releasing it after the
	// live index took its own copy must not underflow the shared count.
	if n := len(snapshotToDisk("default", held, true).Indexes["idx_id"].Entries); n != 22 {
		t.Fatalf("held snapshot index has %d entries, want 22", n)
	}
	held.release()
	if before.sharers.Load() < 0 {
		t.Fatalf("sharers underflow: %d", before.sharers.Load())
	}
}

// Rows added while Sync is writing are not in that Sync's file, and the table
// stays dirty so the next Sync picks them up.
func TestSyncWritesTheSnapshotNotTheLaterRows(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			db := openDiskDB(t, dir, mode)
			live := snapshotTestTable(t, "docs", 40)
			if err := db.Put("default", live); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			base := db.BackendStats().TableWrites

			db.LockContentForWrite()
			appendRow(t, live, 40)
			db.UnlockContentForWrite()
			setSyncHook(t, func() {
				db.LockContentForWrite()
				appendRow(t, live, 41)
				appendRow(t, live, 42)
				live.Rows[1] = []any{1, "changed after the snapshot", []float64{0, 0}}
				db.UnlockContentForWrite()
			})
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			syncSnapshotHook.Store(nil)

			dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
			if len(dt.Rows) != 41 {
				t.Fatalf("file holds %d rows, want the 41 of the snapshot", len(dt.Rows))
			}
			if dt.Rows[1][1] != "row 1" {
				t.Fatalf("row 1 = %v, the later UPDATE leaked into the snapshot", dt.Rows[1][1])
			}
			if len(dt.Indexes["idx_id"].Entries) != 41 {
				t.Fatalf("index holds %d entries, want 41", len(dt.Indexes["idx_id"].Entries))
			}
			if got := db.BackendStats().TableWrites; got != base+1 {
				t.Fatalf("writes = %d, want %d", got, base+1)
			}

			// The table changed after the snapshot, so the next Sync rewrites it.
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			dt = readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
			if len(dt.Rows) != 43 || dt.Rows[1][1] != "changed after the snapshot" {
				t.Fatalf("second Sync did not catch up: %d rows", len(dt.Rows))
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := db.BackendStats().TableWrites; got != base+2 {
				t.Fatalf("an unchanged table was written again (writes %d, want %d)", got, base+2)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The live table must stay the cached one in ModeHybrid, never the snapshot.
func TestHybridSnapshotSaveMirrorsLiveTableIntoPool(t *testing.T) {
	db := openDiskDB(t, t.TempDir(), ModeHybrid)
	defer db.Close()
	live := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	appendRow(t, live, 10)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	h := db.backend.(*HybridBackend)
	cached, ok := h.pool.Get("default", "docs")
	if !ok || cached != live {
		t.Fatalf("pool holds %p (ok=%v), want the live table %p", cached, ok, live)
	}
}

func TestHybridSnapshotClearsDirtyFlagOnlyForItsOwnState(t *testing.T) {
	db := openDiskDB(t, t.TempDir(), ModeHybrid)
	defer db.Close()
	h := db.backend.(*HybridBackend)
	live := snapshotTestTable(t, "docs", 5)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	dirty := func() bool {
		h.dirtyLock.Lock()
		defer h.dirtyLock.Unlock()
		return h.dirty["default"]["docs"]
	}

	h.MarkDirty("default", "docs")
	snap := takeTableSnapshot("default", live, h.beginTableSnapshot("default", "docs"))
	h.MarkDirty("default", "docs") // marked again after the snapshot
	if err := h.saveTableSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	snap.release()
	if !dirty() {
		t.Fatal("the dirty flag was cleared although the table was marked dirty after the snapshot")
	}

	snap = takeTableSnapshot("default", live, h.beginTableSnapshot("default", "docs"))
	if err := h.saveTableSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	snap.release()
	if dirty() {
		t.Fatal("the dirty flag survived a snapshot that covers it")
	}
}

// A snapshot whose table was dropped, or saved by someone else, since it was
// based must not be installed: it would resurrect the table or roll the file
// back.
func TestSnapshotSupersededByDeleteOrNewerSave(t *testing.T) {
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	live := snapshotTestTable(t, "docs", 10)
	if err := b.SaveTable("default", live); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "default", "docs.tbl")

	// 1. Dropped while the snapshot was being prepared.
	snap := takeTableSnapshot("default", live, b.beginTableSnapshot("default", "docs"))
	if err := b.DeleteTable("default", "docs"); err != nil {
		t.Fatal(err)
	}
	if err := b.saveTableSnapshot(snap); !errors.Is(err, errSnapshotSuperseded) {
		t.Fatalf("snapshot of a dropped table: err = %v, want errSnapshotSuperseded", err)
	}
	snap.release()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("dropped table's file came back: %v", err)
	}
	if b.TableExists("default", "docs") {
		t.Fatal("dropped table is known to the backend again")
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}

	// 2. Re-created and saved with newer content than the snapshot.
	if err := b.SaveTable("default", live); err != nil {
		t.Fatal(err)
	}
	old := takeTableSnapshot("default", live, b.beginTableSnapshot("default", "docs"))
	appendRow(t, live, 10)
	if err := b.SaveTable("default", live); err != nil {
		t.Fatal(err)
	}
	if err := b.saveTableSnapshot(old); !errors.Is(err, errSnapshotSuperseded) {
		t.Fatalf("snapshot older than a newer save: err = %v, want errSnapshotSuperseded", err)
	}
	old.release()
	if dt := readRawDiskTable(t, path); len(dt.Rows) != 11 {
		t.Fatalf("file holds %d rows, the newer save was overwritten", len(dt.Rows))
	}
	if b.IsDirty("default", "docs", live.Version) {
		t.Fatal("the version of the newer save was lost")
	}
}

// DROP TABLE during a Sync must not bring the table back.
func TestSyncDoesNotResurrectTableDroppedDuringTheWrite(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	if err := db.Put("default", snapshotTestTable(t, "docs", 10)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("default", snapshotTestTable(t, "keep", 10)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	live, _ := db.Get("default", "docs")
	db.LockContentForWrite()
	appendRow(t, live, 10)
	db.UnlockContentForWrite()
	setSyncHook(t, func() {
		db.LockContentForWrite()
		if err := db.Drop("default", "docs"); err != nil {
			t.Error(err)
		}
		db.UnlockContentForWrite()
	})
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync after a concurrent DROP: %v", err)
	}
	syncSnapshotHook.Store(nil)
	if db.TableExists("default", "docs") {
		t.Fatal("dropped table exists after Sync")
	}
	if _, err := os.Stat(filepath.Join(dir, "default", "docs.tbl")); !os.IsNotExist(err) {
		t.Fatalf("dropped table's file exists: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openDiskDB(t, dir, ModeDisk)
	defer reopened.Close()
	if reopened.TableExists("default", "docs") || !reopened.TableExists("default", "keep") {
		t.Fatal("reopened catalog is wrong after the concurrent DROP")
	}
}

// A write that fails leaves the previous file untouched, the table dirty and
// no temporary file around; the next Sync succeeds.
func TestFailedSnapshotWriteKeepsOldFileAndTableDirty(t *testing.T) {
	dir := t.TempDir()
	db := openDiskDB(t, dir, ModeDisk)
	live := snapshotTestTable(t, "docs", 10)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "default", "docs.tbl")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	appendRow(t, live, 10)
	// A directory where the temporary file must go makes the write fail.
	if err := os.Mkdir(path+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}
	writes := db.BackendStats().TableWrites
	if err := db.Sync(); err == nil {
		t.Fatal("Sync succeeded although the table file could not be written")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("a failed Sync changed the table file")
	}
	if !db.backend.(dirtyTracker).IsDirty("default", "docs", live.Version) {
		t.Fatal("the table was marked clean although its write failed")
	}
	if db.BackendStats().TableWrites != writes {
		t.Fatal("a failed write was counted")
	}
	if err := os.Remove(path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync after the obstacle is gone: %v", err)
	}
	if dt := readRawDiskTable(t, path); len(dt.Rows) != 11 {
		t.Fatalf("file holds %d rows after recovery, want 11", len(dt.Rows))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncOfUnchangedDatabaseWritesNothing(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			db := openDiskDB(t, dir, mode)
			defer db.Close()
			for _, name := range []string{"a", "b"} {
				if err := db.Put("default", snapshotTestTable(t, name, 30)); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			stats := db.BackendStats()
			mod := map[string]int64{}
			for _, name := range []string{"a", "b"} {
				info, err := os.Stat(filepath.Join(dir, "default", name+".tbl"))
				if err != nil {
					t.Fatal(err)
				}
				mod[name] = info.ModTime().UnixNano()
			}
			for i := 0; i < 3; i++ {
				if err := db.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if got := db.BackendStats().TableWrites; got != stats.TableWrites {
				t.Fatalf("unchanged Sync wrote %d table files", got-stats.TableWrites)
			}
			for name, ns := range mod {
				info, err := os.Stat(filepath.Join(dir, "default", name+".tbl"))
				if err != nil {
					t.Fatal(err)
				}
				if info.ModTime().UnixNano() != ns {
					t.Fatalf("table file %s was rewritten", name)
				}
			}
			if left, _ := filepath.Glob(filepath.Join(dir, "default", "*.tmp")); len(left) != 0 {
				t.Fatalf("temporary files left behind: %v", left)
			}
		})
	}
}

// The snapshot path writes what the live-table path wrote: the same decoded
// image, with the search images and secondary index included.
func TestSnapshotFileMatchesLiveTableImage(t *testing.T) {
	for _, format := range []string{"gob", "json"} {
		t.Run(format, func(t *testing.T) {
			live := snapshotTestTable(t, "docs", 25)
			live.FTSIndexes["1"] = searchImageTable(t, "x").FTSIndexes["1"]
			live.VectorIndexes = searchImageTable(t, "x").VectorIndexes
			live.Stats = &TableStats{RowCount: 25}

			snapDir, liveDir := t.TempDir(), t.TempDir()
			mk := NewDiskBackend
			ext := ".tbl"
			if format == "json" {
				mk = NewJSONBackend
				ext = ".json"
			}
			snapB, err := mk(snapDir, false)
			if err != nil {
				t.Fatal(err)
			}
			liveB, err := mk(liveDir, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := snapB.SaveTable("default", live); err != nil { // creates the entry
				t.Fatal(err)
			}
			snap := takeTableSnapshot("default", live, snapB.beginTableSnapshot("default", "docs"))
			snap.attachSearchImages(live.copySearchImages())
			if err := snapB.saveTableSnapshot(snap); err != nil {
				t.Fatal(err)
			}
			snap.release()
			if err := liveB.SaveTable("default", live); err != nil {
				t.Fatal(err)
			}
			a, err := snapB.LoadTable("default", "docs")
			if err != nil || a == nil {
				t.Fatalf("load snapshot file: %v", err)
			}
			b, err := liveB.LoadTable("default", "docs")
			if err != nil || b == nil {
				t.Fatalf("load live file: %v", err)
			}
			if !reflect.DeepEqual(a.Rows, b.Rows) || a.Version != b.Version || !reflect.DeepEqual(a.Cols, b.Cols) {
				t.Fatal("rows, version or schema differ between the snapshot and the live-table file")
			}
			if !reflect.DeepEqual(a.FTSIndexes, b.FTSIndexes) || !reflect.DeepEqual(a.VectorIndexes, b.VectorIndexes) || !reflect.DeepEqual(a.Stats, b.Stats) {
				t.Fatal("search images or statistics differ")
			}
			if !reflect.DeepEqual(imageOf(tableToDisk("default", a)).entries, imageOf(tableToDisk("default", b)).entries) {
				t.Fatal("secondary index differs")
			}
			if format == "gob" {
				if _, err := os.Stat(filepath.Join(snapDir, "default", "docs"+ext+".tmp")); !os.IsNotExist(err) {
					t.Fatal("temporary file left behind")
				}
			}
		})
	}
}

// Rows of tables with a JSON column must be converted exactly as before (the
// snapshot path may only alias rows that need no conversion).
func TestSnapshotOfJSONColumnTableMatchesLiveImage(t *testing.T) {
	live := NewTable("j", []Column{{Name: "id", Type: IntType}, {Name: "doc", Type: JsonType}}, false)
	live.Rows = [][]any{{1, map[string]any{"a": 1.0}}, {2, `{"b":2}`}, {3, nil}}
	live.Version = 2
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SaveTable("default", live); err != nil {
		t.Fatal(err)
	}
	want := readRawDiskTable(t, filepath.Join(dir, "default", "j.tbl"))
	snap := takeTableSnapshot("default", live, b.beginTableSnapshot("default", "j"))
	defer snap.release()
	got := snapshotToDisk("default", snap, true)
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("JSON rows differ: %#v vs %#v", got.Rows, want.Rows)
	}
}

// Snapshots do not take the search-image copy when the backend skips them, and
// the generation bookkeeping stays quiet.
func TestSyncSnapshotHonoursSkipSearchIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dir, SkipSearchIndexPersistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	live := searchImageTable(t, "docs")
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	appendRowTo := func() {
		db.LockContentForWrite()
		live.Rows = append(live.Rows, []any{9, "more", []float64{1, 1}})
		live.Version++
		db.UnlockContentForWrite()
	}
	appendRowTo()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
	if len(dt.FTSIndexes) != 0 || len(dt.VectorIndexes) != 0 || len(dt.Rows) != 3 || len(dt.Indexes) != 1 {
		t.Fatalf("snapshot file: FTS=%d ANN=%d rows=%d indexes=%d", len(dt.FTSIndexes), len(dt.VectorIndexes), len(dt.Rows), len(dt.Indexes))
	}
}

func TestConcurrentSyncsAreSerialized(t *testing.T) {
	db := openDiskDB(t, t.TempDir(), ModeDisk)
	live := snapshotTestTable(t, "docs", 200)
	if err := db.Put("default", live); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 5; i++ {
				db.LockContentForWrite()
				live.Rows = append(live.Rows, []any{1000*g + i, "x", []float64{1, 2}})
				live.Version++
				db.UnlockContentForWrite()
				if err := db.Sync(); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(g)
	}
	for g := 0; g < 8; g++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	dt := readRawDiskTable(t, filepath.Join(db.config.Path, "default", "docs.tbl"))
	if len(dt.Rows) != 240 {
		t.Fatalf("file holds %d rows, want 240", len(dt.Rows))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
