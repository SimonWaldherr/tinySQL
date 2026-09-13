package engine

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGFilteredPostingsMatchAuthorizedScan(t *testing.T) {
	table := storage.NewTable("posting_intersection", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 4096; i++ {
		text := strings.Repeat("common ", 1+i%13)
		if i%3 == 0 {
			text += strings.Repeat("alpha ", 1+i%7)
		}
		if i%71 == 0 {
			text += "rare rare "
		}
		if i%5 == 0 {
			text += "beta "
		}
		table.Rows = append(table.Rows, []any{text})
	}
	table.Version++
	cache := getFTSDocCache("default", table, []int{0})
	for _, stride := range []int{1, 2, 73, 777, 9000} {
		for _, offset := range []int{0, 127, 4095, 5000} {
			filter := &ragRowFilter{}
			allowed := make([]int32, 0)
			for i := offset; i < len(table.Rows); i += stride {
				filter.rows = append(filter.rows, i)
				allowed = append(allowed, int32(i))
			}
			local, idf := ragFilteredFTSStatistics(table, "0", cache, filter)
			if idf == nil {
				idf = func(string) float64 { return 0 }
			}
			for _, query := range []string{"common", "rare", "missing", "common OR alpha OR rare OR beta", "rare OR rare OR missing"} {
				node := ftsBindIDF(ftsExpandQuery(ftsParseQuery(query), cache.postings), idf, cache.termIDs)
				for _, k := range []int{1, 10, 200} {
					got, err := ftsDisjunctionTopK(t.Context(), local, node, k, allowed)
					if err != nil {
						t.Fatal(err)
					}
					h, err := ftsScanRange(t.Context(), local, node, nil, allowed, true, 0, len(allowed), k)
					if err != nil {
						t.Fatal(err)
					}
					want := ftsTopKFromHeap(&h, k)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("stride=%d offset=%d query=%s k=%d: got %v want %v", stride, offset, query, k, got, want)
					}
					if k == 10 {
						planned, err := ragFTSSearchCandidatesFiltered(t.Context(), "default", table, query, k, []int{0}, filter)
						if err != nil || !reflect.DeepEqual(planned, want) {
							t.Fatalf("adaptive retrieval differs: %v / %v (%v)", planned, want, err)
						}
					}
				}
			}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	node := ftsBindIDF(ftsParseQuery("common"), ftsIDFLookup(cache), cache.termIDs)
	if _, err := ftsDisjunctionTopK(ctx, cache, node, 1, []int32{4095}); err == nil {
		t.Fatal("ignored cancellation")
	}
}
