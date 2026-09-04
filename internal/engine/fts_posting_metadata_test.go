package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func checkPostingMetadata(t *testing.T, cache ftsDocCacheEntry) {
	t.Helper()
	for term, rows := range cache.postings {
		counts := cache.postingCounts[term]
		if len(counts) != len(rows) {
			t.Fatalf("%s frequencies misaligned", term)
		}
		for i, row := range rows {
			doc := cache.docs[row]
			if got, want := int(counts[i]), cache.termFrequency(doc, cache.termIDs[term]); got != want {
				t.Fatalf("%s row %d: %d != %d", term, row, got, want)
			}
			block := cache.postingBlocks[term][i/storage.FTSPostingBlockSize]
			if block.MaxFrequency < counts[i] || block.MinDocLen > doc.DocLen {
				t.Fatal("unsafe block bounds")
			}
		}
	}
}

func TestFTSPostingMetadataMaintenanceAndUpgrade(t *testing.T) {
	table := storage.NewTable("posting_metadata", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 300; i++ {
		table.Rows = append(table.Rows, []any{strings.Repeat("alpha ", i%7+1) + "beta"})
	}
	table.Version++
	tenant := "posting-metadata"
	check := func() { checkPostingMetadata(t, getFTSDocCache(tenant, table, []int{0})) }
	check()
	for _, ri := range []int{0, 127, 128, 299} {
		table.Rows[ri][0] = "beta beta new"
		table.Version++
		table.MarkRowUpdated(ri)
		check()
	}
	table.Rows[4][0] = nil
	table.Version++
	table.MarkRowUpdated(4)
	check()
	for i := 0; i < 130; i++ {
		table.Rows = append(table.Rows, []any{"alpha new new"})
	}
	table.Version++
	table.MarkDirtyFrom(300)
	check()
	// DELETE shifts row IDs and must rebuild aligned postings and blocks.
	table.Rows = append(table.Rows[:10], table.Rows[11:]...)
	table.Version++
	table.MarkDirtyFrom(-1)
	check()
	// An old-format or structurally incomplete derived index must rebuild.
	for _, legacy := range []bool{true, false} {
		old := table.FTSIndexes["0"]
		if legacy {
			old.Format = 1
		} else {
			old.PostingCounts["alpha"] = nil
		}
		deleteFTSDocCacheEntry(ftsDocCacheKey{tenant: tenant, table: table.Name, cols: "0"})
		check()
		if table.FTSIndexes["0"] == old {
			t.Fatal("invalid index was reused")
		}
	}
}

func blockPostingFixture(tb testing.TB) ftsDocCacheEntry {
	tb.Helper()
	table := storage.NewTable("block_postings", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 20000; i++ {
		body := "alpha " + strings.Repeat("filler ", 100)
		if i < 128 {
			body = strings.Repeat("alpha ", 30)
		}
		table.Rows = append(table.Rows, []any{body})
	}
	table.Version++
	return getFTSDocCache("block-postings", table, []int{0})
}
func TestFTSBlockPruningMatchesScan(t *testing.T) {
	cache := blockPostingFixture(t)
	idf := ftsIDFLookup(cache)
	for _, query := range []string{"alpha", "alpha OR missing", "alpha OR filler"} {
		node := ftsBindIDF(ftsParseQuery(query), idf, cache.termIDs)
		for _, k := range []int{1, 24, 129, 500} {
			got, err := ftsScanTopK(t.Context(), cache, node, idf, nil, false, k)
			if err != nil {
				t.Fatal(err)
			}
			heap, err := ftsScanRange(t.Context(), cache, node, idf, nil, false, 0, len(cache.docs), k)
			if err != nil {
				t.Fatal(err)
			}
			want := ftsTopKFromHeap(&heap, k)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s k=%d differs", query, k)
			}
		}
	}
}
func BenchmarkFTSPostingMetadata(b *testing.B) {
	cache := blockPostingFixture(b)
	idf := ftsIDFLookup(cache)
	for _, mode := range []string{"document_scan", "frequencies", "frequencies_blocks"} {
		b.Run(mode, func(b *testing.B) {
			entry := cache
			node := ftsBindIDF(ftsParseQuery("alpha"), idf, cache.termIDs)
			if mode == "document_scan" {
				node.idfBound = false
			}
			if mode == "frequencies" {
				entry.postingBlocks = nil
			}
			b.ReportAllocs()
			for b.Loop() {
				hits, err := ftsScanTopK(b.Context(), entry, node, idf, nil, false, 24)
				if err != nil || len(hits) != 24 {
					b.Fatal(fmt.Sprint(err))
				}
			}
		})
	}
}
