package engine

import (
	"context"
	"math/rand"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// hnswRecallCorpus imitates text embeddings: a low-dimensional latent space
// with topic clusters, projected into dims dimensions plus a little noise.
// Queries are drawn from the same distribution as the rows.
func hnswRecallCorpus(rows, dims, queries int) (*storage.Table, [][]float64) {
	rng := rand.New(rand.NewSource(42))
	const rank, topics = 12, 40
	proj := make([][]float64, dims)
	for d := range proj {
		proj[d] = make([]float64, rank)
		for r := range proj[d] {
			proj[d][r] = rng.NormFloat64()
		}
	}
	centres := make([][]float64, topics)
	for c := range centres {
		centres[c] = make([]float64, rank)
		for r := range centres[c] {
			centres[c][r] = 2 * rng.NormFloat64()
		}
	}
	sample := func() []float64 {
		centre := centres[rng.Intn(topics)]
		z := make([]float64, rank)
		for r := range z {
			z[r] = centre[r] + rng.NormFloat64()
		}
		vec := make([]float64, dims)
		for d := range vec {
			for r, w := range proj[d] {
				vec[d] += w * z[r]
			}
			vec[d] += 0.1 * rng.NormFloat64()
		}
		return vec
	}
	table := storage.NewTable("hnsw_recall", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	for i := 0; i < rows; i++ {
		table.Rows = append(table.Rows, []any{i, sample()})
	}
	qs := make([][]float64, queries)
	for i := range qs {
		qs[i] = sample()
	}
	return table, qs
}

// TestVecHNSWRecallOnClusteredEmbeddings guards the graph's quality, not just
// its plumbing. Keeping the plain nearest neighbors (no selection heuristic)
// left each topic cluster an island, and a beam search that never left the
// entry point on the upper layers then missed most of the true top-k: this
// corpus measured 9% (cosine) and 83% (l2) recall@10 before those fixes.
func TestVecHNSWRecallOnClusteredEmbeddings(t *testing.T) {
	const rows, dims, k = 3000, 32, 10
	table, queries := hnswRecallCorpus(rows, dims, 40)
	for _, metric := range []string{"cosine", "l2"} {
		cache := buildVecColumnCache(table, 1, metricNeedsNorms(metric))
		idx, err := buildVecHNSWIndex(context.Background(), table, metric, dims, cache)
		if err != nil {
			t.Fatal(err)
		}
		hits, total := 0, 0
		for _, query := range queries {
			queryNorm := vectorL2Norm(query)
			distFn := buildVecDistanceFunc(metric, query, queryNorm)
			exact, err := vecSearchTopK(context.Background(), table.Rows, dims, k, cache, distFn, vecDistanceFuncNeedsNorm(metric))
			if err != nil {
				t.Fatal(err)
			}
			approx, err := idx.search(context.Background(), query, queryNorm, k, cache)
			if err != nil {
				t.Fatal(err)
			}
			want := make(map[int]bool, len(exact))
			for _, r := range exact {
				want[r.rowIdx] = true
			}
			for _, r := range approx {
				if want[r.rowIdx] {
					hits++
				}
			}
			total += len(exact)
		}
		if recall := float64(hits) / float64(total); recall < 0.95 {
			t.Errorf("metric=%s: HNSW recall@%d = %.3f, want >= 0.95", metric, k, recall)
		}
	}
}

// TestVecHNSWSearchLayerLeavesEntryPoint pins searchLayer's stopping rule. The
// start node is both the nearest candidate and the worst result, so stopping
// on "candidate not better than worst result" ended every ef == 1 search
// before it expanded a single edge.
func TestVecHNSWSearchLayerLeavesEntryPoint(t *testing.T) {
	const n = 10
	table := storage.NewTable("hnsw_path", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	idx := &vecHNSWIndex{metric: "l2", dims: 2, entry: 0, levels: make([]int, n), neighbors: make([][][]int, n)}
	for i := 0; i < n; i++ {
		table.Rows = append(table.Rows, []any{[]float64{float64(i), 0}})
		var links []int
		if i > 0 {
			links = append(links, i-1)
		}
		if i < n-1 {
			links = append(links, i+1)
		}
		idx.neighbors[i] = [][]int{links}
	}
	cache := buildVecColumnCache(table, 0, false)
	query := []float64{n - 1, 0}
	scorer := newVecRowScorer("l2", query, 0, newVecRowResolver(&cache, false))
	entryDist, ok := scorer.distance(0)
	if !ok {
		t.Fatal("entry not scorable")
	}
	visited := acquireVisited(n)
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for _, ef := range []int{1, 3} {
		got := idx.searchLayer(&scorer, 0, entryDist, ef, 0, visited, scratch)
		if len(got) == 0 || got[0].rowIdx != n-1 {
			t.Fatalf("ef=%d: searchLayer returned %v, want nearest row %d first", ef, got, n-1)
		}
	}
	if best, _ := idx.greedyClosest(&scorer, 0, entryDist, 0); best != n-1 {
		t.Fatalf("greedyClosest stopped at %d, want %d", best, n-1)
	}
}
