package engine

import (
	"math"
	"sort"
	"strings"
)

// cachedWindowIntRange finds numeric RANGE frame bounds by binary search for
// sorted integer time axes. Other types and NULLs retain the general path.
func cachedWindowIntRange(env ExecEnv, ex *FuncCall, row Row, rows []Row, current int) ([]Row, bool, error) {
	if env.windowPartitions == nil || ex.Over.Frame == nil || ex.Over.Frame.Mode != "RANGE" || len(ex.Over.OrderBy) != 1 || current < 0 || current >= len(rows) {
		return nil, false, nil
	}
	cache := env.windowPartitions
	shape := cache.shapeFor(ex)
	if !shape.shareable {
		return nil, false, nil
	}
	entry := cache.entries[windowPartitionCacheKey{shape: shape, partition: formatWindowPartitionKey(env, shape.partitionBy, row)}]
	if entry == nil {
		return nil, false, nil
	}
	order := ex.Over.OrderBy[0]
	name := strings.ToLower(order.Col)
	values, found := entry.integerRanges[name]
	if !found {
		values = make([]int64, len(rows))
		for i, r := range rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, true, err
				}
			}
			v, ok := getValLower(r, name)
			if !ok {
				values = nil
				break
			}
			switch n := v.(type) {
			case int:
				values[i] = int64(n)
			case int64:
				values[i] = n
			default:
				values = nil
			}
			if values == nil {
				break
			}
			if i > 0 && ((!order.Desc && values[i] < values[i-1]) || (order.Desc && values[i] > values[i-1])) {
				values = nil
				break
			}
		}
		if entry.integerRanges == nil {
			entry.integerRanges = make(map[string][]int64)
		}
		entry.integerRanges[name] = values
	}
	if values == nil {
		return nil, false, nil
	}
	frame := ex.Over.Frame
	start := intRangeBound(values, current, order.Desc, frame.StartType, frame.StartValue, true)
	end := intRangeBound(values, current, order.Desc, frame.EndType, frame.EndValue, false)
	if start > end || start >= len(rows) || end < 0 {
		return nil, true, nil
	}
	return rows[start : end+1], true, nil
}

func intRangeBound(values []int64, current int, desc bool, kind string, offset int, start bool) int {
	switch kind {
	case "UNBOUNDED_PRECEDING":
		return 0
	case "UNBOUNDED_FOLLOWING":
		return len(values) - 1
	}
	target := values[current]
	// overflow is the target's position relative to all int64 values.
	overflow := 0
	if kind == "OFFSET_PRECEDING" || kind == "OFFSET_FOLLOWING" {
		delta := int64(offset)
		if (kind == "OFFSET_PRECEDING") != desc {
			if target < math.MinInt64+delta {
				overflow = -1
			} else {
				target -= delta
			}
		} else {
			if target > math.MaxInt64-delta {
				overflow = 1
			} else {
				target += delta
			}
		}
	}
	// Compare in window order. The lower bound finds >= target; the upper
	// bound finds > target. Out-of-range targets never require saturation.
	compare := func(v int64) int {
		cmp := 0
		if overflow < 0 || (overflow == 0 && v > target) {
			cmp = 1
		} else if overflow > 0 || (overflow == 0 && v < target) {
			cmp = -1
		}
		if desc {
			cmp = -cmp
		}
		return cmp
	}
	if start {
		return sort.Search(len(values), func(i int) bool { return compare(values[i]) >= 0 })
	}
	return sort.Search(len(values), func(i int) bool { return compare(values[i]) > 0 }) - 1
}
