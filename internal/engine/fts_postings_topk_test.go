package engine

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSPostingTopKMatchesDocumentScan(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	table := storage.NewTable("posting_topk", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for row := 0; row < 400; row++ {
		terms := make([]string, rng.Intn(100)+1)
		for i := range terms {
			terms[i] = fmt.Sprintf("term%d", rng.Intn(30))
		}
		table.Rows = append(table.Rows, []any{strings.Join(terms, " ")})
	}
	table.Rows = append(table.Rows, []any{nil}, []any{""})
	table.Version++
	cache := getFTSDocCache("default", table, []int{0})
	idf := ftsIDFLookup(cache)
	for trial := 0; trial < 70; trial++ {
		terms := make([]string, 2+rng.Intn(10))
		for i := range terms {
			terms[i] = fmt.Sprintf("term%d", rng.Intn(40))
		}
		if trial == 0 {
			terms = []string{"absent", "missing"}
		}
		bound := ftsBindIDF(ftsParseQuery(strings.Join(terms, " OR ")), idf, cache.termIDs)
		for _, k := range []int{1, 3, 24, 500} {
			got, err := ftsScanTopK(t.Context(), cache, bound, idf, nil, false, k)
			if err != nil {
				t.Fatal(err)
			}
			heap, err := ftsScanRange(t.Context(), cache, bound, idf, nil, false, 0, len(cache.docs), k)
			if err != nil {
				t.Fatal(err)
			}
			want := ftsTopKFromHeap(&heap, k)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("trial=%d k=%d got=%v want=%v", trial, k, got, want)
			}
			// A restricted candidate set must never be widened by the posting path.
			rows := []int32{1, 4, 9}
			got, err = ftsScanTopK(t.Context(), cache, bound, idf, rows, true, k)
			if err != nil {
				t.Fatal(err)
			}
			for _, hit := range got {
				if hit.rowIdx != 1 && hit.rowIdx != 4 && hit.rowIdx != 9 {
					t.Fatal("restricted row escaped")
				}
			}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	bound := ftsBindIDF(ftsParseQuery("term1 OR term2"), idf, cache.termIDs)
	if _, err := ftsScanTopK(ctx, cache, bound, idf, nil, false, 3); err == nil {
		t.Fatal("ignored cancellation")
	}
}
