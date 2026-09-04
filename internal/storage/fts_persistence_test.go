package storage

import "testing"

func TestFTSIndexSurvivesDiskAndSnapshotRoundTrips(t *testing.T) {
	table := NewTable("docs", []Column{{Name: "body", Type: TextType}}, false)
	table.Rows = [][]any{{"alpha"}}
	table.Version = 4
	table.structVersion = 2
	table.FTSIndexes["0"] = &FTSIndex{
		Format: 2, Version: 4, StructVersion: 2, BuiltRows: 1,
		Docs:      []FTSDocument{{TermCount: 1, TokenCount: 1, DocLen: 1, Valid: true}},
		AvgDocLen: 1, TotalDocLen: 1, NumDocs: 1,
		Postings: map[string][]int32{"alpha": {0}}, TermIDs: map[string]int32{"alpha": 0},
		PostingCounts: map[string][]int32{"alpha": {1}},
		PostingBlocks: map[string][]FTSPostingBlock{"alpha": {{MaxFrequency: 1, MinDocLen: 1}}},
		DocTermIDs:    []int32{0}, DocTermCounts: []int32{1}, DocTokenIDs: []int32{0},
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
			if index.PostingCounts["alpha"][0] != 1 || index.PostingBlocks["alpha"][0].MaxFrequency != 1 {
				t.Fatal("posting metadata lost")
			}
			index.PostingCounts["alpha"][0] = 9
			index.PostingBlocks["alpha"][0].MaxFrequency = 9
			if table.FTSIndexes["0"].PostingCounts["alpha"][0] != 1 || table.FTSIndexes["0"].PostingBlocks["alpha"][0].MaxFrequency != 1 {
				t.Fatal("posting metadata aliases source")
			}
			index.Postings["alpha"][0] = 99
			if table.FTSIndexes["0"].Postings["alpha"][0] != 0 {
				t.Fatalf("%s round trip shares mutable postings with source", name)
			}
		})
	}
}
