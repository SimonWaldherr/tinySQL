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

func TestCloneVectorTopologyIsIndependent(t *testing.T) {
	levels := []int{1, 0, 0}
	neighbors := [][][]int{{{1, 2}, {2}}, {{0}}, {{}}, {}}
	gotLevels, got := CloneVectorTopology(levels, neighbors)
	if len(gotLevels) != 3 || len(got) != 4 || len(got[0]) != 2 || len(got[3]) != 0 || got[3] == nil {
		t.Fatalf("clone shape = %v %v", gotLevels, got)
	}
	if got[2][0] != nil {
		t.Fatalf("empty neighbor list cloned as %#v, want nil", got[2][0])
	}
	// Appending to one list must not overwrite the next list in the arena.
	got[0][0] = append(got[0][0], 9)
	got[1][0][0] = 7
	gotLevels[0] = 5
	if got[0][1][0] != 2 || neighbors[1][0][0] != 0 || levels[0] != 1 || len(neighbors[0][0]) != 2 {
		t.Fatalf("clone aliases storage: clone=%v source=%v", got, neighbors)
	}
}
