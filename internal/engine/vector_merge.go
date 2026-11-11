package engine

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
