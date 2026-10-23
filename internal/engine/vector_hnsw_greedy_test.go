package engine

import (
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The original traversal is kept as a reference for exactly matching the
// selected row, distance bits, and tie-breaking while skipping repeated work.
func legacyVecHNSWGreedyClosest(idx *vecHNSWIndex, s *vecRowScorer, current int, currentDist float64, layer int) (int, float64) {
	for changed := true; changed; {
		changed = false
		for _, nb := range idx.neighborLayer(current, layer) {
			d, ok := s.distance(nb)
			if ok && vecScoredRowLess(vecScoredRow{rowIdx: nb, distance: d}, vecScoredRow{rowIdx: current, distance: currentDist}) {
				current, currentDist, changed = nb, d, true
			}
		}
	}
	return current, currentDist
}

func greedyHNSWFixture(rows, dims int) (*vecHNSWIndex, vecSearchColumnCacheEntry) {
	table := storage.NewTable("greedy", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	idx := &vecHNSWIndex{dims: dims, entry: rows - 1, maxLevel: 2, neighbors: make([][][]int, rows)}
	for i := range rows {
		vector := make([]float64, dims)
		for j := range vector {
			vector[j] = math.Sin(float64(i/2+1)*0.3 + float64(j)*0.2) // paired ties
		}
		if i == 3 {
			vector = vector[:dims-1] // an unscorable graph neighbor
		}
		table.Rows = append(table.Rows, []any{vector})
		idx.neighbors[i] = make([][]int, 3)
		for layer := range 3 {
			for step := -4; step <= 4; step++ {
				idx.neighbors[i][layer] = append(idx.neighbors[i][layer], (i+step*(layer+1)+rows)%rows)
			}
			idx.neighbors[i][layer] = append(idx.neighbors[i][layer], 0, 3, rows-1)
		}
	}
	return idx, buildVecColumnCache(table, 0, true)
}

func TestVecHNSWGreedyVisitedMatchesReference(t *testing.T) {
	const rows, dims = 48, 8
	idx, cache := greedyHNSWFixture(rows, dims)
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		t.Run(metric, func(t *testing.T) {
			visited := acquireVisited(rows)
			defer releaseVisited(visited)
			for queryID := range 12 {
				query := make([]float64, dims)
				for i := range query {
					query[i] = math.Cos(float64(queryID+i) * 0.4)
				}
				scorer := newVecRowScorer(metric, query, vectorL2Norm(query), newVecRowResolver(&cache, metricNeedsNorms(metric)))
				for entry := range rows {
					entryDist, ok := scorer.distance(entry)
					if !ok {
						continue
					}
					current, dist := entry, entryDist
					want, wantDist := entry, entryDist
					visited.reset()
					visited.visit(entry)
					for layer := idx.maxLevel; layer >= 0; layer-- {
						want, wantDist = legacyVecHNSWGreedyClosest(idx, &scorer, want, wantDist, layer)
						current, dist = idx.greedyClosest(&scorer, current, dist, layer, visited)
						if current != want || math.Float64bits(dist) != math.Float64bits(wantDist) {
							t.Fatalf("query=%d entry=%d layer=%d: got (%d,%g), want (%d,%g)", queryID, entry, layer, current, dist, want, wantDist)
						}
					}
				}
			}
		})
	}
}

func BenchmarkVecHNSWGreedyTraversal(b *testing.B) {
	for _, dims := range []int{64, 768} {
		idx, cache := greedyHNSWFixture(128, dims)
		query := make([]float64, dims)
		for i := range query {
			query[i] = math.Cos(float64(i) * 0.3)
		}
		scorer := newVecRowScorer("cosine", query, vectorL2Norm(query), newVecRowResolver(&cache, true))
		entryDist, _ := scorer.distance(idx.entry)
		for _, cached := range []bool{false, true} {
			b.Run(fmt.Sprintf("dims%d/visited=%t", dims, cached), func(b *testing.B) {
				visited := acquireVisited(len(idx.neighbors))
				defer releaseVisited(visited)
				b.ReportAllocs()
				for b.Loop() {
					current, dist := idx.entry, entryDist
					if cached {
						visited.reset()
						visited.visit(current)
					}
					for layer := idx.maxLevel; layer > 0; layer-- {
						if cached {
							current, dist = idx.greedyClosest(&scorer, current, dist, layer, visited)
						} else {
							current, dist = legacyVecHNSWGreedyClosest(idx, &scorer, current, dist, layer)
						}
					}
				}
			})
		}
	}
}
