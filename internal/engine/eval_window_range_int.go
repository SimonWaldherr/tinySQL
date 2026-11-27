package engine

import (
	"context"
	"math"
	"sort"
	"strings"
)

// windowIntegerRange caches sorted integer keys for RANGE bound searches.
// Other types and NULLs retain the general bound evaluator.
func windowIntegerRange(ctx context.Context, entry *windowPartitionEntry, orderBy []OrderItem) ([]int64, error) {
	if entry == nil || len(orderBy) != 1 {
		return nil, nil
	}
	// All functions sharing this entry have the same ORDER BY.
	if entry.integerRangeChecked {
		return entry.integerRange, nil
	}
	order := orderBy[0]
	name := strings.ToLower(order.Col)
	values := make([]int64, len(entry.rows))
	for i, row := range entry.rows {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		value, _ := getValLower(row, name)
		number, ok := integerOperand(value)
		if !ok {
			values = nil
			break
		}
		if i > 0 && ((!order.Desc && number < values[i-1]) || (order.Desc && number > values[i-1])) {
			values = nil
			break
		}
		values[i] = number
	}
	entry.integerRange = values
	entry.integerRangeChecked = true
	return values, nil
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
	if isOffsetWindowBound(kind) {
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
