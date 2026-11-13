package engine

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// Preserve the per-centroid allocation path as a value/ownership oracle.
func legacyIVFSeedCentroids(rows []int, nlist int, cache vecSearchColumnCacheEntry) [][]float64 {
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

func TestIVFCentroidStoragePreservesValuesAndBoundaries(t *testing.T) {
	for _, dims := range []int{1, 7, 64, 129} {
		_, cache := ivfConvergenceFixture(1031, dims, false)
		rows := validVectorRows(cache, dims)
		lists := chooseIVFListCount(len(rows))
		got := seedIVFCentroids(rows, lists, cache)
		want := legacyIVFSeedCentroids(rows, lists, cache)
		for c := range got {
			if len(got[c]) != dims || cap(got[c]) != dims {
				t.Fatalf("dimension %d: centroid %d has len=%d cap=%d", dims, c, len(got[c]), cap(got[c]))
			}
			for d, value := range got[c] {
				if math.Float64bits(value) != math.Float64bits(want[c][d]) {
					t.Fatalf("dimension %d: centroid %d coordinate %d changed bits", dims, c, d)
				}
			}
			_ = append(got[c], -99)
		}
		for c := range got {
			if !slices.Equal(got[c], want[c]) {
				t.Fatal("appending one centroid changed a neighbor")
			}
		}
		got[0][0] = math.NaN()
		for _, row := range rows {
			if math.IsNaN(cache.vector(row)[0]) {
				t.Fatal("centroid storage aliases the source cache")
			}
		}
	}
}

func TestIVFTrainingCacheLayoutsPreserveAssignments(t *testing.T) {
	table, original := ivfConvergenceFixture(256, 7, false)
	for _, layout := range []string{"contiguous", "segments", "overrides", "gap"} {
		for _, normsReady := range []bool{false, true} {
			for _, metric := range []string{"l2", "cosine", "manhattan", "dot"} {
				t.Run(fmt.Sprintf("%s/norms%t/%s", layout, normsReady, metric), func(t *testing.T) {
					cache := original
					cache.normsReady = normsReady
					if layout == "segments" || layout == "gap" {
						cache.segments = nil
						segment := original.segments[0]
						for start := 0; start < cache.rows; start += 64 {
							end := start + 64
							cache.segments = append(cache.segments, vecColumnSegment{
								start: start, vectors: segment.vectors[start:end],
								valid: segment.valid[start:end], norms: segment.norms[start:end],
							})
						}
						if layout == "gap" {
							cache.segments = cache.segments[1:]
						}
					}
					if layout == "overrides" {
						vector := []float64{1, 2, 3, 4, 5, 6, 7}
						cache.overrides = map[int]vecColumnOverride{
							0: {}, 4: {vector: vector, norm: vectorL2Norm(vector), valid: true},
						}
					}
					assertIVFConvergenceParity(t, table, cache, metric, 7)
				})
			}
		}
	}
}
