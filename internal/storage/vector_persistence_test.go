package storage

import "testing"

func TestVectorIndexSurvivesDiskAndSnapshotRoundTrips(t *testing.T) {
	table := NewTable("docs", []Column{{Name: "embedding", Type: VectorType}}, false)
	table.Rows = [][]any{{[]float64{1, 0}}}
	table.Version = 4
	table.structVersion = 2
	table.VectorIndexes["hnsw:0:cosine"] = &VectorIndex{
		Format: 1, Kind: "hnsw", Column: 0, Metric: "cosine",
		Version: 4, StructVersion: 2, BuiltRows: 1, Dims: 2,
		Entry: 0, MaxLevel: 0, Levels: []int{0}, Neighbors: [][][]int{{{}}},
	}

	for name, roundTrip := range map[string]func(*Table) *Table{
		"disk":     func(source *Table) *Table { return diskToTable(tableToDisk("default", source)) },
		"snapshot": cloneTable,
	} {
		t.Run(name, func(t *testing.T) {
			got := roundTrip(table)
			index := got.VectorIndexes["hnsw:0:cosine"]
			if index == nil || index.BuiltRows != 1 || index.Dims != 2 || index.Entry != 0 {
				t.Fatalf("vector index lost in %s round trip: %#v", name, index)
			}
			if got.StructVersion() != 2 {
				t.Fatalf("StructVersion after %s round trip = %d, want 2", name, got.StructVersion())
			}
			index.Levels[0] = 99
			if table.VectorIndexes["hnsw:0:cosine"].Levels[0] != 0 {
				t.Fatalf("%s round trip shares mutable graph topology with source", name)
			}
		})
	}
}
