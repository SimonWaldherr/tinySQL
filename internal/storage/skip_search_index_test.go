package storage

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// searchImageTable builds a small table that carries one FTS and one ANN image
// plus a secondary index, the three kinds of derived state a table file can
// hold.
func searchImageTable(t *testing.T, name string) *Table {
	t.Helper()
	table := NewTable(name, []Column{
		{Name: "id", Type: IntType},
		{Name: "body", Type: TextType},
		{Name: "emb", Type: VectorType},
	}, false)
	table.Rows = [][]any{
		{1, "alpha", []float64{1, 0}},
		{2, "beta", []float64{0, 1}},
	}
	table.Version = 3
	table.structVersion = 1
	table.Indexes = map[string]*SecondaryIndex{"idx_id": {
		Name: "idx_id", Columns: []string{"id"},
		Entries: []IndexEntry{{Key: []byte{1}, RowIDs: []int{0}}, {Key: []byte{2}, RowIDs: []int{1}}},
	}}
	table.FTSIndexes["1"] = &FTSIndex{
		Format: 2, Version: 3, StructVersion: 1, BuiltRows: 2,
		Docs:      []FTSDocument{{TermCount: 1, TokenCount: 1, DocLen: 1, Valid: true}, {TermCount: 1, TokenCount: 1, DocLen: 1, Valid: true}},
		AvgDocLen: 1, TotalDocLen: 2, NumDocs: 2,
		Postings: map[string][]int32{"alpha": {0}}, TermIDs: map[string]int32{"alpha": 0},
		PostingCounts: map[string][]int32{"alpha": {1}},
		PostingBlocks: map[string][]FTSPostingBlock{"alpha": {{MaxFrequency: 1, MinDocLen: 1}}},
		DocTermIDs:    []int32{0, 0}, DocTermCounts: []int32{1, 1}, DocTokenIDs: []int32{0, 0},
	}
	table.VectorIndexes = map[string]*VectorIndex{"hnsw:2:cosine": {
		Format: 1, Kind: "hnsw", Column: 2, Metric: "cosine", Version: 3, StructVersion: 1,
		BuiltRows: 2, Dims: 2, Entry: 0, MaxLevel: 0,
		Levels: []int{0, 0}, Neighbors: [][][]int{{{1}}, {{0}}},
	}}
	table.MarkFTSIndexesChanged()
	table.MarkVectorIndexesChanged()
	return table
}

// readRawDiskTable decodes the table file at path without going through the
// backend, so a test sees exactly what is on disk.
func readRawDiskTable(t *testing.T, path string) diskTable {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var dt diskTable
	if err := gob.NewDecoder(f).Decode(&dt); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return dt
}

func TestSkipSearchIndexPersistenceKeepsImagesOutOfTableFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip bool
	}{{"default", false}, {"skip", true}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dir, SkipSearchIndexPersistence: tc.skip})
			if err != nil {
				t.Fatal(err)
			}
			table := searchImageTable(t, "docs")
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			dt := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
			if tc.skip {
				if len(dt.FTSIndexes) != 0 || len(dt.VectorIndexes) != 0 {
					t.Fatalf("skip: file carries FTS=%d ANN=%d images", len(dt.FTSIndexes), len(dt.VectorIndexes))
				}
			} else if len(dt.FTSIndexes) != 1 || len(dt.VectorIndexes) != 1 {
				t.Fatalf("default: file carries FTS=%d ANN=%d images, want 1 and 1", len(dt.FTSIndexes), len(dt.VectorIndexes))
			}
			// The secondary index is authoritative state and is always kept.
			if len(dt.Indexes) != 1 || len(dt.Indexes["idx_id"].Entries) != 2 {
				t.Fatalf("secondary index not persisted: %#v", dt.Indexes)
			}
			if len(dt.Rows) != 2 {
				t.Fatalf("rows = %d, want 2", len(dt.Rows))
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dir, SkipSearchIndexPersistence: tc.skip})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.Get("default", "docs")
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Rows) != 2 || got.Version != 3 {
				t.Fatalf("reopened rows=%d version=%d", len(got.Rows), got.Version)
			}
			if tc.skip && (len(got.FTSIndexes) != 0 || len(got.VectorIndexes) != 0) {
				t.Fatalf("skip: reopened table holds images FTS=%d ANN=%d", len(got.FTSIndexes), len(got.VectorIndexes))
			}
			if !tc.skip && (len(got.FTSIndexes) != 1 || len(got.VectorIndexes) != 1) {
				t.Fatalf("default: reopened table lost its images FTS=%d ANN=%d", len(got.FTSIndexes), len(got.VectorIndexes))
			}
		})
	}
}

func TestSkipSearchIndexPersistenceGenerationsDoNotDirtyTable(t *testing.T) {
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			cfg := StorageConfig{Mode: mode, Path: dir, SkipSearchIndexPersistence: true, MaxMemoryBytes: 64 << 20}
			db, err := OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			table := searchImageTable(t, "docs")
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			writes := db.BackendStats().TableWrites
			if writes == 0 {
				t.Fatal("first Sync wrote nothing")
			}

			// What FTS_WARM / VEC_WARM do to a table: bump the generations.
			table.DerivedLock()
			table.MarkFTSIndexesChanged()
			table.MarkVectorIndexesChanged()
			table.DerivedUnlock()
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := db.BackendStats().TableWrites; got != writes {
				t.Fatalf("Sync rewrote a table that only changed search-index generations (writes %d -> %d)", writes, got)
			}

			// A real change still gets written.
			table.Rows = append(table.Rows, []any{3, "gamma", []float64{1, 1}})
			table.Version++
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := db.BackendStats().TableWrites; got != writes+1 {
				t.Fatalf("Sync after a row change wrote %d files, want 1", got-writes)
			}
		})
	}
}

func TestSearchIndexGenerationsStillDirtyTableByDefault(t *testing.T) {
	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table := searchImageTable(t, "docs")
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	writes := db.BackendStats().TableWrites
	table.DerivedLock()
	table.MarkFTSIndexesChanged()
	table.DerivedUnlock()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := db.BackendStats().TableWrites; got != writes+1 {
		t.Fatalf("default behaviour changed: a new FTS generation wrote %d files, want 1", got-writes)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := db.BackendStats().TableWrites; got != writes+1 {
		t.Fatalf("an unchanged table was rewritten (%d writes)", got-writes)
	}
}

// The flag off must write what the code wrote before the flag existed: the
// complete tableToDisk image.
func TestDefaultTableFileMatchesFullTableImage(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	table := searchImageTable(t, "docs")
	if err := backend.SaveTable("default", table); err != nil {
		t.Fatal(err)
	}
	got := readRawDiskTable(t, filepath.Join(dir, "default", "docs.tbl"))
	want := tableToDisk("default", table)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("default table file differs from the full image\n got: %#v\nwant: %#v", got, want)
	}
}

// A file written by a configuration that persists the images loads under the
// flag, and its images are dropped rather than kept as stale memory.
func TestSkipSearchIndexPersistenceDropsImagesOfOlderFiles(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.SaveTable("default", searchImageTable(t, "docs")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	reader.SetSkipSearchIndexPersistence(true)
	got, err := reader.LoadTable("default", "docs")
	if err != nil || got == nil {
		t.Fatalf("load: %v %v", got, err)
	}
	if len(got.FTSIndexes) != 0 || len(got.VectorIndexes) != 0 {
		t.Fatalf("images kept: FTS=%d ANN=%d", len(got.FTSIndexes), len(got.VectorIndexes))
	}
	if len(got.Indexes) != 1 || len(got.Rows) != 2 {
		t.Fatalf("rows or secondary index lost: rows=%d indexes=%d", len(got.Rows), len(got.Indexes))
	}
}

func TestSkipSearchIndexPersistenceJSONBackend(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(StorageConfig{Mode: ModeJSON, Path: dir, SkipSearchIndexPersistence: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("default", searchImageTable(t, "docs")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "default", "docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{`"hnsw"`, `"Postings"`} {
		if bytes.Contains(raw, []byte(needle)) {
			t.Fatalf("JSON table file contains %s despite the flag", needle)
		}
	}
}
