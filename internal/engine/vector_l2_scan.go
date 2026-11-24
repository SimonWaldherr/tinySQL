package engine

import (
	"context"
	"math"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
)

const (
	// The prefix aligns with the accumulator groups of every L2 kernel,
	// making its floating-point distance a lower bound on the full result.
	l2PrunePrefix      = 64
	l2ScanPruneMinDims = 512
	l2ScanPruneMisses  = 8
)

// scanVecL2Range ranks a worker range in ascending physical row order.
// A new row tied with the heap root always loses on row ID. The full distance
// still uses the original kernel, preserving its floating-point reduction.
func scanVecL2Range(ctx context.Context, segments []vecColumnSegment, query []float64, start, end, k int) (vecScoredHeap, error) {
	rows := make(vecScoredHeap, 0, min(k, end-start))
	var prefix []float64
	missesLeft := 0
	var retryBelow float64
	if len(query) >= l2ScanPruneMinDims {
		prefix = query[:l2PrunePrefix]
		missesLeft = l2ScanPruneMisses
	}
	// One heap and cutoff per worker, even when its range crosses segments.
	for i := range segments {
		segment := &segments[i]
		if segment.start >= end {
			break
		}
		from := max(start-segment.start, 0)
		to := min(end-segment.start, len(segment.vectors))
		if from >= to {
			continue
		}
		base := segment.start + from
		valid := segment.valid[from:to]
		for i, vector := range segment.vectors[from:to] {
			row := base + i
			if row&1023 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			if !valid[i] || len(vector) != len(query) {
				continue
			}
			full := len(rows) == k
			if full && prefix != nil {
				cutoff := rows[0].distance
				if missesLeft == 0 && cutoff < retryBelow {
					missesLeft = l2ScanPruneMisses
				}
				if missesLeft > 0 {
					bound := search.VectorL2SquaredEqualLength(vector[:l2PrunePrefix], prefix)
					if bound >= cutoff {
						missesLeft = l2ScanPruneMisses
						continue
					}
					// Stop probing when the prefix rarely excludes a candidate.
					// A substantially tighter heap can make pruning useful again.
					missesLeft--
					if missesLeft == 0 {
						retryBelow = cutoff * 0.5
					}
				}
			}
			distance := search.VectorL2SquaredEqualLength(vector, query)
			if full {
				if distance < rows[0].distance {
					rows[0] = vecScoredRow{rowIdx: row, distance: distance}
					vecScoredHeapDown(rows, 0)
				}
			} else if !math.IsNaN(distance) {
				rows = append(rows, vecScoredRow{rowIdx: row, distance: distance})
				if len(rows) == k {
					heapifyVecScoredRows(rows)
				}
			}
		}
	}
	if len(rows) < k {
		heapifyVecScoredRows(rows)
	}
	return rows, nil
}
