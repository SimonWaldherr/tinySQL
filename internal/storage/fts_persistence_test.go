package storage

import "testing"

func TestFTSIndexSurvivesDiskAndSnapshotRoundTrips(t *testing.T) {
	table := NewTable("docs", []Column{{Name: "body", Type: TextType}}, false)
	table.Rows = [][]any{{"alpha"}}
	table.Version = 4
	table.structVersion = 2
	table.FTSIndexes["0"] = &FTSIndex{
		Format: 1, Version: 4, StructVersion: 2, BuiltRows: 1,
		Docs:      []FTSDocument{{TermCount: 1, TokenCount: 1, DocLen: 1, Valid: true}},
		AvgDocLen: 1, TotalDocLen: 1, NumDocs: 1,
		Postings: map[string][]int32{"alpha": {0}}, TermIDs: map[string]int32{"alpha": 0},
		DocTermIDs: []int32{0}, DocTermCounts: []int32{1}, DocTokenIDs: []int32{0},
	}

	for name, roundTrip := range map[string]func(*Table) *Table{
		"disk":     func(source *Table) *Table { return diskToTable(tableToDisk("default", source)) },
		"snapshot": cloneTable,
	} {
		t.Run(name, func(t *testing.T) {
			got := roundTrip(table)
			index := got.FTSIndexes["0"]
			if index == nil || index.BuiltRows != 1 || index.Postings["alpha"][0] != 0 {
				t.Fatalf("FTS index lost in %s round trip: %#v", name, index)
			}
			if got.StructVersion() != 2 {
				t.Fatalf("StructVersion after %s round trip = %d, want 2", name, got.StructVersion())
			}
			index.Postings["alpha"][0] = 99
			if table.FTSIndexes["0"].Postings["alpha"][0] != 0 {
				t.Fatalf("%s round trip shares mutable postings with source", name)
			}
		})
	}
}
