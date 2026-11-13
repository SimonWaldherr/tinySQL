package engine

const vecFullMergeMin = 1024

type vecSearchWorkerResult struct {
	heapRows vecScoredHeap
	err      error
}

// Worker heaps belong to this request. When one already has enough capacity,
// consume it as the merge heap instead of copying its entries into a new one.
// Large k retains a bounded reservation rather than growing a worker buffer.
func mergeVecSearchResults(results []vecSearchWorkerResult, k, candidateCount int) ([]vecScoredRow, error) {
	var merged vecScoredHeap
	haveHeap := false
	capacity := max(0, min(k, candidateCount))
	if k >= vecFullMergeMin {
		if rows, ok := mergeFullVecHeaps(results, k, capacity); ok {
			return rows, nil
		}
	}
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		if !haveHeap {
			haveHeap = true
			if cap(results[i].heapRows) >= capacity {
				merged = results[i].heapRows
				results[i].heapRows = nil
				continue
			}
			merged = *newScoredHeap(k, candidateCount)
		}
		for _, row := range results[i].heapRows {
			pushTopK(&merged, row.rowIdx, row.distance, k)
		}
	}
	return topKFromHeap(&merged, k), nil
}

type vecMergeHead struct {
	row    vecScoredRow
	worker int
}

// When every worker candidate fits, sort the owned worker heaps and merge
// their runs. This avoids inserting the entire union into another large heap.
// Reserve exactly the union's size, and only use this path when the previous
// merge would also allocate: retain its zero-allocation worker-buffer reuse.
func mergeFullVecHeaps(results []vecSearchWorkerResult, k, capacity int) ([]vecScoredRow, bool) {
	if len(results) < 2 || len(results) > 64 || cap(results[0].heapRows) >= capacity {
		return nil, false
	}
	total := 0
	for _, result := range results {
		if result.err != nil || len(result.heapRows) > k-total {
			return nil, false
		}
		total += len(result.heapRows)
	}
	if total < vecFullMergeMin {
		return nil, false
	}
	var bounds [64]struct{ low, high int }
	for worker, result := range results {
		for pos, row := range result.heapRows {
			if !vecScoredRowReorderable(row) || pos > 0 && result.heapRows.Less(pos, (pos-1)/2) {
				return nil, false
			}
			if pos == 0 {
				bounds[worker].low, bounds[worker].high = row.rowIdx, row.rowIdx
			} else {
				bounds[worker].low = min(bounds[worker].low, row.rowIdx)
				bounds[worker].high = max(bounds[worker].high, row.rowIdx)
			}
		}
		// Worker scans normally own disjoint row ranges. Overlapping ranges
		// can also indicate aliased input buffers; leave those inputs intact
		// for the original merge rather than sorting one run under another.
		if len(result.heapRows) != 0 {
			for previous := 0; previous < worker; previous++ {
				if len(results[previous].heapRows) != 0 && bounds[previous].low <= bounds[worker].high && bounds[worker].low <= bounds[previous].high {
					return nil, false
				}
			}
		}
	}
	var runs [64][]vecScoredRow
	var positions [64]int
	var headStorage [64]vecMergeHead
	heads := headStorage[:0]
	for worker := range results {
		runs[worker] = topKFromHeap(&results[worker].heapRows, len(results[worker].heapRows))
		if len(runs[worker]) != 0 {
			heads = append(heads, vecMergeHead{row: runs[worker][0], worker: worker})
		}
	}
	for parent := len(heads)/2 - 1; parent >= 0; parent-- {
		vecMergeHeadDown(heads, parent)
	}
	rows := make([]vecScoredRow, total)
	for pos := range rows {
		head := heads[0]
		rows[pos] = head.row
		positions[head.worker]++
		if next := positions[head.worker]; next < len(runs[head.worker]) {
			heads[0].row = runs[head.worker][next]
		} else {
			heads[0] = heads[len(heads)-1]
			heads = heads[:len(heads)-1]
		}
		vecMergeHeadDown(heads, 0)
	}
	return rows, true
}

func vecMergeHeadDown(heads []vecMergeHead, parent int) {
	for {
		left := 2*parent + 1
		if left >= len(heads) {
			return
		}
		child := left
		if right := left + 1; right < len(heads) && vecScoredRowLess(heads[right].row, heads[left].row) {
			child = right
		}
		if !vecScoredRowLess(heads[child].row, heads[parent].row) {
			return
		}
		heads[child], heads[parent] = heads[parent], heads[child]
		parent = child
	}
}
