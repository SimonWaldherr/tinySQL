package storage

import "testing"

func materializationBenchmarkEntries(keys, rowsPerKey int) []IndexEntry {
	entries := make([]IndexEntry, keys)
	for key := range entries {
		rows := make([]int, rowsPerKey)
		for row := range rows {
			rows[row] = key*rowsPerKey + row
		}
		entries[key] = IndexEntry{Key: CanonicalIndexKey([]any{key}), RowIDs: rows}
	}
	return entries
}

func BenchmarkSecondaryIndexEncodeSnapshot(b *testing.B) {
	for _, warm := range []bool{false, true} {
		name := "Cold"
		if warm {
			name = "Warm"
		}
		b.Run(name, func(b *testing.B) {
			idx := &SecondaryIndex{Name: "idx", Columns: []string{"key"}, Entries: materializationBenchmarkEntries(10000, 1)}
			if warm {
				idx.hydrate()
			}
			indexes := map[string]*SecondaryIndex{"idx": idx}
			b.ReportAllocs()
			for b.Loop() {
				out := materializeSecondaryIndexesForEncode(indexes)
				if len(out["idx"].Entries) != 10000 {
					b.Fatal("incomplete snapshot")
				}
			}
		})
	}
}
