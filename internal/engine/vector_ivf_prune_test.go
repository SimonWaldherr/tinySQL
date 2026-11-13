package engine

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
)

func ivfPruneFixture(dims, nlist int, clustered bool) ([]float64, [][]float64, int) {
	rng := rand.New(rand.NewSource(831))
	query := make([]float64, dims)
	centroids := make([][]float64, nlist)
	for d := range query {
		query[d] = rng.NormFloat64()
	}
	for c := range centroids {
		centroids[c] = make([]float64, dims)
		for d := range query {
			centroids[c][d] = rng.NormFloat64()
			if clustered && c != nlist/2 {
				centroids[c][d] += float64(c%8+1) * 8
			}
		}
	}
	if clustered && nlist != 0 {
		for d := range query {
			centroids[nlist/2][d] = query[d] + rng.NormFloat64()*0.1
		}
	}
	return query, centroids, nearestCentroidL2(query, centroids)
}

func assertIVFPrunedCentroid(t *testing.T, query []float64, centroids [][]float64, hint int) {
	t.Helper()
	want := nearestCentroidL2(query, centroids)
	if got := nearestCentroidL2Pruned(query, centroids, hint); got != want {
		t.Fatalf("dimensions=%d hint=%d: centroid=%d, want %d", len(query), hint, got, want)
	}
}

func TestIVFPrunedCentroidMatchesFullDistances(t *testing.T) {
	for _, dims := range []int{0, 1, 17, 255, 256, 257, 263, 264, 271, 272, 383, 384, 385, 768, 769} {
		for _, nlist := range []int{0, 1, 16, 31, 32, 64} {
			for _, clustered := range []bool{false, true} {
				t.Run(fmt.Sprintf("dims%d/lists%d/clustered%t", dims, nlist, clustered), func(t *testing.T) {
					query, centroids, nearest := ivfPruneFixture(dims, nlist, clustered)
					for _, hint := range []int{-1, 0, nearest, nlist / 2, nlist - 1, nlist} {
						assertIVFPrunedCentroid(t, query, centroids, hint)
					}
				})
			}
		}
	}
}

func TestIVFPrunedCentroidPreservesTiesAndInvalidDistances(t *testing.T) {
	const dims = 257
	for _, special := range []float64{0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64} {
		query := make([]float64, dims)
		centroids := make([][]float64, 64)
		for c := range centroids {
			centroids[c] = make([]float64, dims)
			for d := range query {
				centroids[c][d] = float64(c%5 + 1)
			}
		}
		centroids[0] = nil
		centroids[1] = make([]float64, dims-1)
		for _, c := range []int{3, 13, 33} {
			centroids[c] = make([]float64, dims)
			centroids[c][0] = special
		}
		for hint := -1; hint <= len(centroids); hint++ {
			assertIVFPrunedCentroid(t, query, centroids, hint)
		}
		query[dims-1] = special
		for _, hint := range []int{3, 13, 33, 63} {
			assertIVFPrunedCentroid(t, query, centroids, hint)
		}
	}
	// Prefix ties must not hide a lower-index full-distance tie. A last-bit
	// difference outside the prefix must still be evaluated by the full kernel.
	query, centroids, _ := ivfPruneFixture(dims, 64, true)
	for d := range query {
		query[d] = 0
		centroids[0][d], centroids[32][d] = 0, 0
	}
	centroids[0][0], centroids[32][0] = 1, 1
	centroids[0][dims-1] = math.SmallestNonzeroFloat64
	assertIVFPrunedCentroid(t, query, centroids, 32)
	centroids[0][0] = math.Nextafter(1, math.Inf(1))
	assertIVFPrunedCentroid(t, query, centroids, 32)
}

func TestIVFPrunedCentroidPreservesMaxFloatSentinel(t *testing.T) {
	query := make([]float64, 257)
	centroids := make([][]float64, 64)
	root := math.Sqrt(math.MaxFloat64)
	tail := math.Sqrt(math.MaxFloat64 - root*root)
	for c := range centroids {
		centroids[c] = make([]float64, len(query))
		centroids[c][0], centroids[c][1] = root, tail
	}
	if distance := search.VectorL2Squared(centroids[0], query); distance != math.MaxFloat64 {
		t.Fatalf("sentinel fixture has distance %g instead of MaxFloat64", distance)
	}
	for _, hint := range []int{0, 17, 63} {
		assertIVFPrunedCentroid(t, query, centroids, hint)
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, centroid := range centroids {
			centroid[0] = value
		}
		for _, hint := range []int{0, 17, 63} {
			assertIVFPrunedCentroid(t, query, centroids, hint)
		}
	}
}

func TestIVFPrunedTrainingMatchesFullPasses(t *testing.T) {
	for _, metric := range []string{"l2", "cosine", "manhattan", "dot"} {
		for _, clustered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clustered%t", metric, clustered), func(t *testing.T) {
				table, cache := ivfConvergenceFixture(4096, 257, clustered)
				assertIVFConvergenceParity(t, table, cache, metric, 257)
			})
		}
	}
	for _, order := range []string{"periodic", "sorted", "shuffled"} {
		t.Run("noisy/"+order, func(t *testing.T) {
			table, cache := ivfSeedQualityFixture(order, 4096, 257)
			assertIVFConvergenceParity(t, table, cache, "l2", 257)
		})
	}
}
