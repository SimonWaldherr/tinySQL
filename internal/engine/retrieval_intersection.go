package engine

// retrievalIntersectSparse intersects sorted, duplicate-free row IDs when one
// list is much shorter. Exponential seeks skip gaps in the longer list, while
// nearby matches need only a few comparisons. Both input slices stay immutable.
// A nil output counts matches only, for filter-local BM25 document frequency.
func retrievalIntersectSparse[A, B ~int | ~int32](short []A, long []B, out []int32) ([]int32, int) {
	count := 0
	for _, row := range short {
		target := int64(row)
		if len(long) == 0 {
			break
		}
		if int64(long[0]) < target {
			hi := 1
			for hi < len(long) && int64(long[hi]) < target {
				hi *= 2
			}
			lo := hi / 2
			if hi > len(long) {
				hi = len(long)
			}
			for lo < hi {
				mid := lo + (hi-lo)/2
				if int64(long[mid]) < target {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			long = long[lo:]
		}
		if len(long) > 0 && int64(long[0]) == target {
			count++
			if out != nil {
				out = append(out, int32(row))
			}
			long = long[1:]
		}
	}
	return out, count
}
