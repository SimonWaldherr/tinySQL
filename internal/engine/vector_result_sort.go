package engine

import "math"

const vecResultSortMin = 256

// Equal rows have identical bits except for signed zero; NaNs also violate
// the ordering contract. Reordered paths retain the old heap logic for both.
func vecScoredRowReorderable(row vecScoredRow) bool {
	return !math.IsNaN(row.distance) && math.Float64bits(row.distance) != uint64(1)<<63
}

// A globally worst-first heap needs only reversal, not a full heap drain.
// Check without modifying it, and fail early on ordinary unordered heaps.
func takeOrderedVecHeap(heap *vecScoredHeap) ([]vecScoredRow, bool) {
	rows := []vecScoredRow(*heap)
	if len(rows) < vecResultSortMin || !vecScoredRowReorderable(rows[0]) {
		return nil, false
	}
	for i := 1; i < len(rows); i++ {
		if !vecScoredRowReorderable(rows[i]) || vecScoredRowLess(rows[i-1], rows[i]) {
			return nil, false
		}
	}
	for left, right := 0, len(rows)-1; left < right; left, right = left+1, right-1 {
		rows[left], rows[right] = rows[right], rows[left]
	}
	*heap = nil
	return rows, true
}
