package engine

import "math/rand/v2"

// seedIVFCentroids samples one row from each disjoint part of the row set.
// Jitter breaks the fixed stride's alignment with periodically interleaved
// documents, while stratification still covers insertion-sorted collections.
// A private, fixed seed makes cold builds reproducible without global RNG state.
func seedIVFCentroids(rows []int, nlist int, cache vecSearchColumnCacheEntry) [][]float64 {
	centroids := make([][]float64, nlist)
	rng := rand.New(rand.NewPCG(0, 1))
	width, remainder := len(rows)/nlist, len(rows)%nlist
	start := 0
	for i := range centroids {
		size := width
		if i < remainder {
			size++
		}
		row := rows[start+rng.IntN(size)]
		centroids[i] = append([]float64(nil), cache.vector(row)...)
		start += size
	}
	return centroids
}
