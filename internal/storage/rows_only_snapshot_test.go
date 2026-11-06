package storage

import (
	"reflect"
	"sort"
	"testing"
)

func rowsOnlyTestTable(t *testing.T, db *DB) *Table {
	t.Helper()
	table := NewTable("chunks", []Column{{Name: "id", Type: IntType}, {Name: "source", Type: TextType}, {Name: "vec", Type: BlobType}}, false)
	table.Rows = [][]any{
		{1, "a", []byte("v1")},
		{2, "a", []byte("v2")},
		{3, "b", []byte("v3")},
		{4, "c", []byte("v4")},
	}
	table.Version = 5
	table.dirtyFrom = 2
	table.dirtyRows = []int{1}
	table.dirtyRowsState = dirtyRowsExact
	table.Stats = &TableStats{RowCount: 4, Columns: map[string]ColumnStats{"id": {DistinctCount: 4}}}
	if err := table.CreateSecondaryIndex("idx_source", []string{"source"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	return table
}

func indexRows(t *testing.T, table *Table, source string) []int {
	t.Helper()
	ids, err := table.LookupSecondaryIndexPoint(table.Indexes["idx_source"], []any{source})
	if err != nil {
		t.Fatal(err)
	}
	ids = append([]int(nil), ids...)
	sort.Ints(ids)
	return ids
}

// An UPDATE writes new row slices at the same positions and moves index entries
// in place; rollback must give back the rows and an index that agrees with them.
func TestRowsOnlySnapshotRestoresAnInPlaceUpdateAndItsIndex(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	before := [][]any{}
	for _, r := range table.Rows {
		before = append(before, append([]any(nil), r...))
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	// What executeSimpleUpdateFastPath does: a new row at the same position and an
	// in-place index move for the changed key.
	old := table.Rows[0]
	table.Rows[0] = []any{1, "z", []byte("changed")}
	if err := table.UpdateSecondaryIndexRow(0, old, table.Rows[0], []string{"idx_source"}); err != nil {
		t.Fatal(err)
	}
	table.Rows[3] = []any{4, "a", []byte("also")}
	if err := table.UpdateSecondaryIndexRow(3, []any{4, "c", []byte("v4")}, table.Rows[3], []string{"idx_source"}); err != nil {
		t.Fatal(err)
	}
	table.Version++
	table.dirtyFrom = 0
	table.dirtyRows = []int{0, 3}
	table.Stats = &TableStats{Stale: true}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if !reflect.DeepEqual(table.Rows, before) {
		t.Fatalf("rows after restore:\n got %v\nwant %v", table.Rows, before)
	}
	if got := indexRows(t, table, "a"); !reflect.DeepEqual(got, []int{0, 1}) {
		t.Errorf("index entries for a = %v, want [0 1]", got)
	}
	if got := indexRows(t, table, "z"); len(got) != 0 {
		t.Errorf("index still has the rolled-back key z: %v", got)
	}
	if got := indexRows(t, table, "c"); !reflect.DeepEqual(got, []int{3}) {
		t.Errorf("index entries for c = %v, want [3]", got)
	}
	if table.Version != 5 || table.dirtyFrom != 2 || table.dirtyRowsState != dirtyRowsExact || len(table.dirtyRows) != 1 || table.dirtyRows[0] != 1 {
		t.Fatalf("metadata after restore: version %d dirtyFrom %d state %d rows %v", table.Version, table.dirtyFrom, table.dirtyRowsState, table.dirtyRows)
	}
	if table.Stats == nil || table.Stats.Stale || table.Stats.Columns["id"].DistinctCount != 4 {
		t.Fatalf("statistics not restored: %#v", table.Stats)
	}
}

// A DELETE builds a new outer slice and replaces the index lists; a swap-and-pop
// point delete shifts rows inside the old array. Rollback must undo both.
func TestRowsOnlySnapshotRestoresAMultiRowDeleteAndASwapAndPop(t *testing.T) {
	for name, mutate := range map[string]func(table *Table){
		"compacting delete": func(table *Table) {
			kept := [][]any{table.Rows[2], table.Rows[3]}
			table.Rows = kept
			table.ReindexSecondaryIndexRows(map[int]int{2: 0, 3: 1})
		},
		"swap and pop": func(table *Table) {
			last := len(table.Rows) - 1
			table.Rows[0] = table.Rows[last]
			table.Rows = table.Rows[:last]
		},
		"delete everything": func(table *Table) {
			table.Rows = nil
			table.ClearSecondaryIndexes()
		},
	} {
		t.Run(name, func(t *testing.T) {
			db := NewDB()
			t.Cleanup(func() { _ = db.Close() })
			table := rowsOnlyTestTable(t, db)
			before := [][]any{}
			for _, r := range table.Rows {
				before = append(before, append([]any(nil), r...))
			}
			db.LockContentForWrite()
			snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
			if err != nil {
				db.UnlockContentForWrite()
				t.Fatal(err)
			}
			mutate(table)
			table.Version++
			db.RestoreStatementSnapshot(snapshot)
			db.UnlockContentForWrite()

			if !reflect.DeepEqual(table.Rows, before) {
				t.Fatalf("rows after restore:\n got %v\nwant %v", table.Rows, before)
			}
			for src, want := range map[string][]int{"a": {0, 1}, "b": {2}, "c": {3}} {
				if got := indexRows(t, table, src); !reflect.DeepEqual(got, want) {
					t.Errorf("index entries for %s = %v, want %v", src, got, want)
				}
			}
			if table.Version != 5 {
				t.Errorf("version after restore = %d, want 5", table.Version)
			}
		})
	}
}

// The pre-image must be independent of what the statement does to the live
// slice afterwards: an append that reuses spare capacity must not leak into it.
func TestRowsOnlySnapshotIsIndependentOfTheLiveBackingArray(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	table.Rows = append(make([][]any, 0, 16), table.Rows...) // spare capacity

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.Rows[1] = []any{99, "q", []byte("x")}
	table.Rows = append(table.Rows, []any{5, "d", []byte("v5")})
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	if len(table.Rows) != 4 || table.Rows[1][0] != 2 {
		t.Fatalf("restore must give the original four rows back, got %v", table.Rows)
	}
}

// A table that has been searched has full-text and vector index structures. They
// must not push it back to the deep clone, and they must survive a rollback that
// did not search, but not one during which they were rebuilt.
func TestRowsOnlySnapshotKeepsSearchIndexesUnlessTheStatementChangedThem(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	table.FTSIndexes["f"] = &FTSIndex{}
	table.VectorIndexes["v"] = &VectorIndex{}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	db.UnlockContentForWrite()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.rowsOnly == nil {
		t.Fatal("a searched table must still get the rows-only snapshot")
	}
	if refs, ok := snapshot.RestoredTables(); !ok || len(refs) != 1 || refs[0].Table != "chunks" {
		t.Fatalf("RestoredTables = %v %v", refs, ok)
	}

	// A statement that did not search: the structures stay.
	db.LockContentForWrite()
	snapshot, _ = db.SnapshotForRowsOnlyStatement("default", "chunks")
	table.Rows[0] = []any{1, "z", []byte("x")}
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()
	if len(table.FTSIndexes) != 1 || len(table.VectorIndexes) != 1 {
		t.Fatalf("untouched search indexes must survive a rollback: fts %d vector %d", len(table.FTSIndexes), len(table.VectorIndexes))
	}

	// A statement during which they were rebuilt: dropped, to rebuild on next use.
	db.LockContentForWrite()
	snapshot, _ = db.SnapshotForRowsOnlyStatement("default", "chunks")
	table.DerivedLock()
	table.FTSIndexes["f"] = &FTSIndex{BuiltRows: 99}
	table.MarkFTSIndexesChanged()
	table.VectorIndexes["w"] = &VectorIndex{}
	table.MarkVectorIndexesChanged()
	table.DerivedUnlock()
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()
	if len(table.FTSIndexes) != 0 || len(table.VectorIndexes) != 0 {
		t.Fatalf("search indexes changed by the failed statement must be dropped: fts %d vector %d", len(table.FTSIndexes), len(table.VectorIndexes))
	}

	// A missing table is an error, a plain one works.
	if _, err := db.SnapshotForRowsOnlyStatement("default", "gibt-es-nicht"); err == nil {
		t.Fatal("a missing table must be an error")
	}
}

func TestRowsOnlySnapshotFallsBackForAPinnedStreamSource(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	release, ok := db.PinTableForStream(table)
	if !ok {
		t.Fatal("test setup: the table must be pinnable")
	}
	defer release()
	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	db.UnlockContentForWrite()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.rowsOnly != nil {
		t.Fatal("a table a stream is reading must keep the deep snapshot, so the stream's source is never restored in place")
	}
}

// Rollback invalidation must remain dirty even if a search index generation
// was persisted after it changed and before the statement was rolled back.
func TestRowsOnlySnapshotInvalidationMarksPersistedIndexesDirty(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		t.Fatal(err)
	}
	table.DerivedLock()
	table.FTSIndexes["f"] = &FTSIndex{}
	table.VectorIndexes["v"] = &VectorIndex{}
	table.MarkFTSIndexesChanged()
	table.MarkVectorIndexesChanged()
	fg, vg := table.ftsGeneration, table.vectorGeneration
	table.DerivedUnlock()
	table.MarkFTSIndexesPersisted(fg)
	table.MarkVectorIndexesPersisted(vg)
	db.RestoreStatementSnapshot(snapshot)
	if got, dirty := table.FTSIndexesPersistenceState(); !dirty || got <= fg {
		t.Fatalf("FTS generation %d dirty %v, discarded %d", got, dirty, fg)
	}
	if got, dirty := table.VectorIndexesPersistenceState(); !dirty || got <= vg {
		t.Fatalf("vector generation %d dirty %v, discarded %d", got, dirty, vg)
	}
}

func TestRowsOnlySnapshotReadsSearchGenerationsUnderLock(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			table.DerivedLock()
			table.MarkFTSIndexesChanged()
			table.MarkVectorIndexesChanged()
			table.DerivedUnlock()
		}
	}()
	for i := 0; i < 2000; i++ {
		snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
		if err != nil {
			t.Fatal(err)
		}
		db.ReleaseStatementSnapshot(snapshot)
	}
	<-done
}

func TestRowsOnlySnapshotPersistsSearchIndexInvalidation(t *testing.T) {
	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	if err := db.SyncTable("default", table); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		t.Fatal(err)
	}
	defer db.ReleaseStatementSnapshot(snapshot)
	table.DerivedLock()
	table.FTSIndexes["f"] = &FTSIndex{}
	table.VectorIndexes["v"] = &VectorIndex{}
	table.MarkFTSIndexesChanged()
	table.MarkVectorIndexesChanged()
	table.DerivedUnlock()
	if err := db.SyncTable("default", table); err != nil {
		t.Fatal(err)
	}
	db.RestoreStatementSnapshot(snapshot)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	backend, ok := db.backend.(*DiskBackend)
	if !ok {
		t.Fatalf("unexpected backend %T", db.backend)
	}
	loaded, err := backend.LoadTable("default", "chunks")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.FTSIndexes) != 0 || len(loaded.VectorIndexes) != 0 {
		t.Fatalf("discarded indexes were persisted: FTS %d vector %d", len(loaded.FTSIndexes), len(loaded.VectorIndexes))
	}
}
