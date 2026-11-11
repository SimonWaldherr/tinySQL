package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Keep the former heap selection as a reference, including exact fallback
// when the graph traversal produces fewer than k candidates.
func legacyFilteredANNSelection(ctx context.Context, idx *vecHNSWIndex, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry, allowed *ragRowFilter) ([]vecScoredRow, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.entry < 0 || k <= 0 {
		return nil, nil
	}
	visited := acquireVisited(cache.rowCount())
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	scorer := newVecRowScorer(idx.metric, query, queryNorm, newVecRowResolver(&cache, metricNeedsNorms(idx.metric)))
	candidates, err := idx.searchCandidates(ctx, &scorer, chooseHNSWEfSearch(k), visited, scratch)
	if err != nil {
		return nil, err
	}
	result := newScoredHeap(k, len(candidates))
	for _, row := range candidates {
		pushTopK(result, row.rowIdx, row.distance, k)
	}
	if result.Len() < k {
		return ragVecTopKAllowed(ctx, allowed.rows, len(query), k, cache,
			buildVecDistanceFunc(idx.metric, query, queryNorm), vecDistanceFuncNeedsNorm(idx.metric))
	}
	return topKFromHeap(result, k), nil
}

func filteredANNSelectionFixture(tb testing.TB, rows, dims int, metric string) (*vecHNSWIndex, vecSearchColumnCacheEntry, *ragRowFilter) {
	tb.Helper()
	table := storage.NewTable("filtered_selection", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	allowed := &ragRowFilter{}
	for i := range rows {
		vec := make([]float64, dims)
		for d := range vec {
			vec[d] = math.Sin(float64(i/2+1)*0.3 + float64(d)*0.2) // paired ties
		}
		if i == 3 {
			vec = vec[:dims-1]
		} else if i == 5 {
			clear(vec) // no cosine distance, valid for the other metrics
		}
		table.Rows = append(table.Rows, []any{vec})
		if i%4 != 0 {
			allowed.rows = append(allowed.rows, i)
		}
	}
	cache := buildVecColumnCache(table, 0, true)
	idx, err := buildRAGFilteredANNIndex(tb.Context(), table, metric, dims, cache, allowed)
	if err != nil {
		tb.Fatal(err)
	}
	return idx, cache, allowed
}

func TestFilteredANNSelectionMatchesHeap(t *testing.T) {
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		t.Run(metric, func(t *testing.T) {
			idx, cache, allowed := filteredANNSelectionFixture(t, 128, 8, metric)
			queries := [][]float64{make([]float64, 8), {math.NaN(), 0, 0, 0, 0, 0, 0, 0}}
			for queryID := range 4 {
				query := make([]float64, 8)
				for d := range query {
					query[d] = math.Cos(float64(queryID+d) * 0.4)
				}
				queries = append(queries, query)
			}
			for _, query := range queries {
				queryNorm := vectorL2Norm(query)
				for _, k := range []int{0, 1, 4, 20, 80, 200} {
					want, wantErr := legacyFilteredANNSelection(t.Context(), idx, query, queryNorm, k, cache, allowed)
					got, gotErr := idx.searchFiltered(t.Context(), query, queryNorm, k, cache, allowed)
					if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
						t.Fatalf("query=%v k=%d: got %v/%v, want %v/%v", query, k, got, gotErr, want, wantErr)
					}
					for _, row := range got {
						if row.rowIdx%4 == 0 {
							t.Fatal("returned a forbidden row", row)
						}
					}
				}
			}
		})
	}
}

func TestFilteredANNSelectionOwnsResultsAndCancels(t *testing.T) {
	idx, cache, allowed := filteredANNSelectionFixture(t, 128, 8, "l2")
	query := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	got, err := idx.searchFiltered(t.Context(), query, 0, 4, cache, allowed)
	if err != nil || len(got) != 4 {
		t.Fatalf("selection: %v / %v", got, err)
	}
	want := append([]vecScoredRow(nil), got...)
	other := []float64{-1, 2, -3, 4, -5, 6, -7, 8}
	if _, err := idx.searchFiltered(t.Context(), other, 0, 40, cache, allowed); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("later traversal overwrote returned pooled storage")
	}
	got[0].rowIdx, got[0].distance = 0, -999
	got, err = idx.searchFiltered(t.Context(), query, 0, 4, cache, allowed)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("caller mutation changed the index or scratch: %v / %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := idx.searchFiltered(ctx, query, 0, 4, cache, allowed); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
}

func TestFilteredANNSelectionRAGPermissionsAndMutation(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("filtered_prefix", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "scope", Type: storage.TextType}, {Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = append(table.Rows, []any{0, "blocked", []float64{0, 0}})
	for id := 1; id <= vecSearchParallelMinRows; id++ {
		table.Rows = append(table.Rows, []any{id, "allowed", []float64{float64((id-1)/2 + 1), 0}})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	query := `SELECT id,_vec_rank,_vec_distance FROM RAG_SEARCH('filtered_prefix','embedding','[0,0]',3,
		'{"metric":"l2","index":"hnsw","pre_filter":{"equals":{"scope":"allowed"}}}')`
	for range 2 {
		got := execSQL(t, db, query)
		want := []Row{{"id": 1, "_vec_rank": 1, "_vec_distance": float64(1)}, {"id": 2, "_vec_rank": 2, "_vec_distance": float64(1)}, {"id": 3, "_vec_rank": 3, "_vec_distance": float64(2)}}
		if !reflect.DeepEqual(got.Rows, want) {
			t.Fatalf("public filtered ANN returned %v, want %v", got.Rows, want)
		}
		got.Rows[0]["id"] = -1
	}
	execSQL(t, db, "UPDATE filtered_prefix SET embedding='[10000,0]' WHERE id=1")
	got := execSQL(t, db, query)
	want := []Row{{"id": 2, "_vec_rank": 1, "_vec_distance": float64(1)}, {"id": 3, "_vec_rank": 2, "_vec_distance": float64(2)}, {"id": 4, "_vec_rank": 3, "_vec_distance": float64(2)}}
	if !reflect.DeepEqual(got.Rows, want) {
		t.Fatalf("mutation reused stale filtered ANN results: %v, want %v", got.Rows, want)
	}
}
