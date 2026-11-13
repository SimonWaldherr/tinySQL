package engine

import (
	"context"
	"math"
)

const vecTopKBatchMin = 64

// fillVecTopKBatch collects the initial candidates without per-row heap work.
// Return the first unconsumed physical row and a valid heap, including when
// fewer than k rows score. The remaining scan can use its normal threshold.
func fillVecTopKBatch(ctx context.Context, start, end, dims, k int, cache *vecSearchColumnCacheEntry, distance vecDistanceFunc, needNorm bool, rows *vecScoredHeap) (int, error) {
	// A direct scan rejects mixed dimensions before calculating missing
	// norms. Keep that ordering without discarding override-provided norms.
	computeNorm := needNorm && !cache.normsReady && len(cache.overrides) == 0
	resolver := newVecRowResolver(cache, needNorm && !computeNorm)
	next := end
	for row := start; row < end; row++ {
		if row&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return 0, err
			}
		}
		vector, norm, valid := resolver.resolve(row)
		if !valid || len(vector) != dims {
			continue
		}
		if computeNorm {
			norm = vectorL2Norm(vector)
		}
		dist, ok := distance(vector, norm)
		if !ok || math.IsNaN(dist) {
			continue
		}
		*rows = append(*rows, vecScoredRow{rowIdx: row, distance: dist})
		if len(*rows) == k {
			next = row + 1
			break
		}
	}
	for parent := len(*rows)/2 - 1; parent >= 0; parent-- {
		vecScoredHeapDown(*rows, parent)
	}
	return next, nil
}
