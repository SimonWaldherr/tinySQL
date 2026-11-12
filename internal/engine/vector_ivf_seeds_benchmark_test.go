package engine

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"testing"
)

func ivfSeedQueries(dims int) [][]float64 {
	queries := make([][]float64, 8)
	for group := range queries {
		queries[group] = make([]float64, dims)
		for d := range queries[group] {
			queries[group][d] = math.Sin(float64(d+group)) * 0.03
			if d%8 == group {
				queries[group][d] += 10
			}
		}
	}
	return queries
}

func BenchmarkIVFSeedOrder(b *testing.B) {
	for _, order := range []string{"periodic", "sorted", "shuffled"} {
		for _, metric := range []string{"l2", "cosine"} {
			b.Run(fmt.Sprintf("%s/%s", order, metric), func(b *testing.B) {
				table, cache := ivfSeedQualityFixture(order, 4096, 64)
				idx, err := buildVecIVFIndex(b.Context(), table, metric, 64, cache)
				if err != nil {
					b.Fatal(err)
				}
				queries := ivfSeedQueries(64)
				var loss float64
				occupied := 0
				for c, list := range idx.lists {
					if len(list) != 0 {
						occupied++
					}
					for _, row := range list {
						loss += ivfIndependentDistance(metric, cache.vector(row), idx.centroids[c])
					}
				}
				hits, candidates := 0, 0
				for _, query := range queries {
					want := ivfIndependentTopK(cache, query, metric, 10)
					got, err := idx.search(b.Context(), query, vectorL2Norm(query), 10, cache)
					if err != nil || len(got) != 10 {
						b.Fatalf("search: %d rows, %v", len(got), err)
					}
					for _, row := range got {
						if slices.Contains(want, row.rowIdx) {
							hits++
						}
					}
					centroids := make([]int, len(idx.centroids))
					distances := make([]float64, len(centroids))
					for c := range centroids {
						centroids[c] = c
						distances[c] = ivfIndependentDistance(metric, query, idx.centroids[c])
					}
					sort.Slice(centroids, func(i, j int) bool {
						a, z := centroids[i], centroids[j]
						return distances[a] < distances[z] || distances[a] == distances[z] && a < z
					})
					count := 0
					for rank, c := range centroids {
						if rank >= chooseIVFNProbe(len(centroids), 10) && count >= 10 {
							break
						}
						count += len(idx.lists[c])
					}
					candidates += count
				}
				b.Run("build", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						built, err := buildVecIVFIndex(b.Context(), table, metric, 64, cache)
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(built)
					}
					b.ReportMetric(loss/float64(cache.rowCount()), "mean-loss")
					b.ReportMetric(float64(occupied), "used-lists")
				})
				b.Run("search", func(b *testing.B) {
					b.ReportAllocs()
					i := 0
					for b.Loop() {
						query := queries[i%len(queries)]
						result, err := idx.search(b.Context(), query, vectorL2Norm(query), 10, cache)
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(result)
						i++
					}
					b.ReportMetric(float64(candidates)/float64(len(queries)), "candidates/query")
					b.ReportMetric(float64(hits)/float64(len(queries)*10), "recall@10")
				})
			})
		}
	}
}
