package storage

import (
	"bytes"
	"encoding/gob"
	"os"
	"reflect"
	"runtime"
	"runtime/metrics"
	"testing"
)

func indexedLoadFixture(tb testing.TB, rows int) *DB {
	tb.Helper()
	db := NewDB()
	table := NewTable("indexed_docs", []Column{{Name: "id", Type: IntType}, {Name: "body", Type: TextType}, {Name: "embedding", Type: VectorType}}, false)
	table.Rows = make([][]any, rows)
	postings := make([]int32, rows)
	counts := make([]int32, rows)
	docs := make([]FTSDocument, rows)
	levels := make([]int, rows)
	neighbors := make([][][]int, rows)
	for i := range table.Rows {
		table.Rows[i] = []any{i, "document", []float64{1, 0}}
		postings[i], counts[i] = int32(i), 1
		docs[i] = FTSDocument{TermCount: 1, TokenCount: 1, DocLen: 1, Valid: true}
		neighbors[i] = [][]int{{(i + 1) % rows}}
	}
	if err := table.CreateSecondaryIndex("doc_id", []string{"id"}, true); err != nil {
		tb.Fatal(err)
	}
	table.FTSIndexes["1"] = &FTSIndex{
		Format: 2, BuiltRows: rows, Docs: docs, NumDocs: rows,
		AvgDocLen: 1, TotalDocLen: float64(rows),
		Postings:      map[string][]int32{"document": postings},
		PostingCounts: map[string][]int32{"document": counts},
		PostingBlocks: map[string][]FTSPostingBlock{"document": {{MaxFrequency: 1, MinDocLen: 1}}},
		TermIDs:       map[string]int32{"document": 0},
		DocTermIDs:    make([]int32, rows), DocTermCounts: append([]int32(nil), counts...), DocTokenIDs: make([]int32, rows),
	}
	table.VectorIndexes["hnsw:2:cosine"] = &VectorIndex{
		Format: 1, Kind: "hnsw", Column: 2, Metric: "cosine", BuiltRows: rows,
		Dims: 2, Levels: levels, Neighbors: neighbors,
	}
	table.Analyze()
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	return db
}

func TestDecodedIndexesTransferOwnership(t *testing.T) {
	db := indexedLoadFixture(t, 8)
	table, _ := db.Get("default", "indexed_docs")
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(tableToDisk("default", table)); err != nil {
		t.Fatal(err)
	}
	var decoded diskTable
	if err := gob.NewDecoder(&data).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	secondary := decoded.Indexes["doc_id"]
	fts := decoded.FTSIndexes["1"]
	vector := decoded.VectorIndexes["hnsw:2:cosine"]
	stats := decoded.Stats
	want := diskToTable(decoded)
	got := diskToTableOwned(decoded)
	if got.Indexes["doc_id"] != secondary || got.FTSIndexes["1"] != fts ||
		got.VectorIndexes["hnsw:2:cosine"] != vector || got.Stats != stats {
		t.Fatal("freshly decoded index or statistics image was duplicated")
	}
	if !reflect.DeepEqual(got.FTSIndexes, want.FTSIndexes) || !reflect.DeepEqual(got.VectorIndexes, want.VectorIndexes) ||
		!reflect.DeepEqual(got.Stats, want.Stats) || !reflect.DeepEqual(got.Indexes["doc_id"].Entries, want.Indexes["doc_id"].Entries) {
		t.Fatal("ownership transfer changed persisted index values")
	}
}

func TestIndexedSnapshotLoadsRemainIndependent(t *testing.T) {
	source := indexedLoadFixture(t, 8)
	data, err := SaveToBytes(source)
	if err != nil {
		t.Fatal(err)
	}
	first, err := LoadFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	one, _ := first.Get("default", "indexed_docs")
	two, _ := second.Get("default", "indexed_docs")
	original, _ := source.Get("default", "indexed_docs")
	before := one.Rows[0]
	one.Rows[0] = []any{100, "changed", []float64{0, 1}}
	if err := one.UpdateSecondaryIndexRow(0, before, one.Rows[0], []string{"doc_id"}); err != nil {
		t.Fatal(err)
	}
	one.FTSIndexes["1"].Postings["document"][0] = 99
	one.FTSIndexes["1"].PostingCounts["document"][0] = 99
	one.FTSIndexes["1"].PostingBlocks["document"][0].MaxFrequency = 99
	one.VectorIndexes["hnsw:2:cosine"].Neighbors[0][0][0] = 99
	one.VectorIndexes["hnsw:2:cosine"].Levels[0] = 99
	one.Stats.Columns["id"] = ColumnStats{NullCount: 99}
	for _, untouched := range []*Table{two, original} {
		ids, err := untouched.LookupSecondaryIndexPoint(untouched.Indexes["doc_id"], []any{0})
		if err != nil || !reflect.DeepEqual(ids, []int{0}) {
			t.Fatalf("another image's secondary index changed: %v, %v", ids, err)
		}
		if untouched.FTSIndexes["1"].Postings["document"][0] != 0 ||
			untouched.FTSIndexes["1"].PostingCounts["document"][0] != 1 ||
			untouched.FTSIndexes["1"].PostingBlocks["document"][0].MaxFrequency != 1 ||
			untouched.VectorIndexes["hnsw:2:cosine"].Neighbors[0][0][0] != 1 ||
			untouched.VectorIndexes["hnsw:2:cosine"].Levels[0] != 0 || untouched.Stats.Columns["id"].NullCount != 0 {
			t.Fatal("independent loads share mutable index data or statistics")
		}
	}
}

func TestDecodedIndexesLegacyMissingFields(t *testing.T) {
	for _, withNilEntries := range []bool{false, true} {
		dt := diskTable{Name: "legacy", Stats: &TableStats{}}
		if withNilEntries {
			dt.Indexes = map[string]*SecondaryIndex{"missing": nil, "empty": {}, "entries": {Columns: []string{}, Entries: []IndexEntry{{Key: []byte{}, RowIDs: []int{}}}}}
			dt.FTSIndexes = map[string]*FTSIndex{"missing": nil, "old": {Format: 1}, "empty": {
				Docs: []FTSDocument{}, DocTermIDs: []int32{}, DocTermCounts: []int32{}, DocTokenIDs: []int32{},
				Postings: map[string][]int32{"empty": {}}, PostingCounts: map[string][]int32{"empty": {}},
				PostingBlocks: map[string][]FTSPostingBlock{"empty": {}},
			}}
			dt.VectorIndexes = map[string]*VectorIndex{"missing": nil, "empty": {}, "layers": {Levels: []int{}, Neighbors: [][][]int{nil, {}, {{}, nil}}}}
		}
		want := diskToTable(dt)
		got := diskToTableOwned(dt)
		if !reflect.DeepEqual(got.Indexes, want.Indexes) || !reflect.DeepEqual(got.FTSIndexes, want.FTSIndexes) ||
			!reflect.DeepEqual(got.VectorIndexes, want.VectorIndexes) || !reflect.DeepEqual(got.Stats, want.Stats) {
			t.Fatal("legacy nil entries or omitted metadata changed during restore")
		}
		if withNilEntries {
			got.FTSIndexes["old"].Postings["new"] = []int32{0}
			got.FTSIndexes["old"].PostingCounts["new"] = []int32{1}
			got.FTSIndexes["old"].PostingBlocks["new"] = nil
			got.FTSIndexes["old"].TermIDs["new"] = 0
		}
	}
}

func BenchmarkIndexedSnapshotLoadReader(b *testing.B) {
	data, err := SaveToBytes(indexedLoadFixture(b, 10000))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		loaded, err := LoadFromReader(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		table, err := loaded.Get("default", "indexed_docs")
		if err != nil || len(table.Rows) != 10000 || table.VectorIndexes["hnsw:2:cosine"].BuiltRows != 10000 {
			b.Fatal("indexed snapshot lost data", err)
		}
	}
}

// An opt-in retained-heap observation, not a timing or GC-threshold assertion.
func TestIndexedSnapshotLoadFootprint(t *testing.T) {
	if os.Getenv("TINYSQL_INDEXED_LOAD_MEMORY") == "" {
		t.Skip("set TINYSQL_INDEXED_LOAD_MEMORY=1 for a retained-heap observation")
	}
	data, err := SaveToBytes(indexedLoadFixture(t, 100000))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	scan := []metrics.Sample{{Name: "/gc/scan/heap:bytes"}}
	metrics.Read(scan)
	beforeScan := scan[0].Value.Uint64()
	runtime.ReadMemStats(&before)
	loaded, err := LoadFromReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	metrics.Read(scan)
	t.Logf("indexed load: live bytes=%d live objects=%d scannable bytes=%d",
		int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(after.HeapObjects)-int64(before.HeapObjects),
		int64(scan[0].Value.Uint64())-int64(beforeScan))
	runtime.KeepAlive(loaded)
	runtime.KeepAlive(data)
}
