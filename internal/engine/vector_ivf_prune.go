package engine

import (
	"math"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
)

const (
	ivfL2PruneMinDims      = 256
	ivfL2PruneMinCentroids = 32
	ivfL2PrunePrefix       = 64
	ivfL2PruneMaxMisses    = 4
)

// nearestCentroidL2Pruned starts from the previous assignment, scored against
// the current centroid. The hint changes evaluation order, never tie order.
// A 64-coordinate prefix uses the same kernel and aligned reduction groups
// as a full distance. Its nonnegative accumulators form an exact lower bound
// for all current backends. Survivors are scored afresh by the full kernel.
func nearestCentroidL2Pruned(vec []float64, centroids [][]float64, hint int) int {
	if len(vec) < ivfL2PruneMinDims || len(centroids) < ivfL2PruneMinCentroids ||
		hint < 0 || hint >= len(centroids) || len(centroids[hint]) != len(vec) {
		return nearestCentroidL2(vec, centroids)
	}
	best := hint
	bestDist := search.VectorL2Squared(centroids[hint], vec)
	// Keep the original MaxFloat64 sentinel and its all-invalid fallback.
	if !(bestDist >= 0 && bestDist < math.MaxFloat64) {
		return nearestCentroidL2(vec, centroids)
	}
	misses := 0
	for c, centroid := range centroids {
		if bestDist == 0 && c >= best {
			return best
		}
		if c == hint || len(centroid) != len(vec) {
			continue
		}
		if misses < ivfL2PruneMaxMisses {
			prefix := search.VectorL2Squared(centroid[:ivfL2PrunePrefix], vec[:ivfL2PrunePrefix])
			if prefix > bestDist || prefix == bestDist && c > best {
				misses = 0
				continue
			}
			// Unstructured inputs often cannot reject on a short prefix. Bound
			// their extra work, then use full distances for the remaining rows.
			misses++
		}
		distance := search.VectorL2Squared(centroid, vec)
		if distance < bestDist || distance == bestDist && c < best {
			best, bestDist = c, distance
		}
	}
	return best
}
