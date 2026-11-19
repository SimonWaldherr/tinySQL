package engine

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGSparseVectorScores(t *testing.T) {
	table := storage.NewTable("sparse_scores", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, 1024)
	copy(table.Rows, [][]any{
		{[]float64{1, 0}}, {"[1,0]"}, {nil}, {}, {[]float64{1}}, {"bad"},
		{[]float64{0, 0}}, {[]float64{-1, 1}}, {[]float64{1e300, 1e300}},
		{[]float64{math.NaN(), 1}}, {[]float64{1, 0}}, // equal but outside the filter
	})
	filter := &ragRowFilter{rows: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}
	env := ExecEnv{tenant: "default", ctx: t.Context()}
	key := vecSearchColumnCacheKey{source: table, tenant: env.tenant, table: table.Name, colIdx: 0}
	t.Cleanup(func() { purgeVectorCachesFor(env.tenant, table.Name) })
	verify := func(t *testing.T, warm bool) {
		t.Helper()
		ref := buildVecColumnCache(table, 0, true)
		for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
			args := vecSearchArgs{colName: "embedding", queryVec: []float64{1, 0}, metric: metric, indexMode: vecIndexFlat}
			for _, k := range []int{1, 2, 100} {
				args.k = k
				got, err := ragVecSearchCandidatesFiltered(t.Context(), env, args, table, filter)
				want := referenceFilteredVectorScores(filter.rows, 2, k, ref, buildVecDistanceFunc(metric, args.queryVec, 1), metric == "cosine")
				finalizeVecScoredRows(metric, want)
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s k=%d: got %v (%v), want %v", metric, k, got, err, want)
				}
			}
		}
		if _, ok := currentVecColumnCache(key, false); ok != warm {
			t.Fatalf("current column cache present = %v, want %v", ok, warm)
		}
	}
	verify(t, false)
	getVecColumnCache(env.tenant, table, 0, true)
	verify(t, true)
	table.Rows[0] = []any{[]float64{-1, 0}}
	table.MarkRowUpdated(0)
	table.Version++
	verify(t, false)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	args := vecSearchArgs{colName: "embedding", queryVec: []float64{1, 0}, k: 2, metric: "cosine"}
	if _, err := ragVecSearchCandidatesFiltered(ctx, env, args, table, filter); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled search returned %v", err)
	}
}
