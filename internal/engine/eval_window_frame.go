package engine

import (
	"fmt"
	"math/big"
	"strings"
)

func evalWindowFrameValue(env ExecEnv, ex *FuncCall, rows []Row, current int, entry *windowPartitionEntry, last bool) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s requires one argument", ex.Name)
	}
	// Value functions with only peer or unbounded endpoints do not need
	// to prepare numeric keys for the entire partition.
	if frame := ex.Over.Frame; frame != nil && !isOffsetWindowBound(frame.StartType) && !isOffsetWindowBound(frame.EndType) {
		entry = nil
	}
	start, end, err := windowFrameBounds(env, rows, current, ex.Over, entry)
	if err != nil || start == end {
		return nil, err
	}
	index := start
	if last {
		index = end - 1
	}
	return evalExpr(env, ex.Args[0], rows[index])
}

// windowFrameBounds returns a half-open interval in the ordered partition.
func windowFrameBounds(env ExecEnv, rows []Row, current int, over *OverClause, entry *windowPartitionEntry) (int, int, error) {
	if len(rows) == 0 || current < 0 || current >= len(rows) {
		return 0, 0, nil
	}
	frame := over.Frame
	if frame == nil {
		if len(over.OrderBy) == 0 {
			return 0, len(rows), nil
		}
		return 0, lastWindowPeerIndex(rows, current, over.OrderBy) + 1, nil
	}
	if frame.StartType == "UNBOUNDED_PRECEDING" && frame.EndType == "UNBOUNDED_FOLLOWING" {
		return 0, len(rows), nil
	}
	var start, end int
	if frame.Mode == "RANGE" {
		values, err := windowIntegerRange(env.ctx, entry, over.OrderBy)
		if err != nil {
			return 0, 0, err
		}
		if values != nil {
			desc := over.OrderBy[0].Desc
			start = intRangeBound(values, current, desc, frame.StartType, frame.StartValue, true)
			end = intRangeBound(values, current, desc, frame.EndType, frame.EndValue, false)
		} else {
			start, err = rangeWindowBound(env, rows, current, over.OrderBy, frame.StartType, frame.StartValue, true)
			if err != nil {
				return 0, 0, err
			}
			end, err = rangeWindowBound(env, rows, current, over.OrderBy, frame.EndType, frame.EndValue, false)
			if err != nil {
				return 0, 0, err
			}
		}
	} else {
		start = rowsWindowBound(current, len(rows), frame.StartType, frame.StartValue, true)
		end = rowsWindowBound(current, len(rows), frame.EndType, frame.EndValue, false)
	}
	if start > end || start >= len(rows) || end < 0 {
		return 0, 0, nil
	}
	return start, end + 1, nil
}

func isOffsetWindowBound(kind string) bool {
	return kind == "OFFSET_PRECEDING" || kind == "OFFSET_FOLLOWING"
}

func rowsWindowBound(current, total int, kind string, offset int, start bool) int {
	switch kind {
	case "UNBOUNDED_PRECEDING":
		return 0
	case "UNBOUNDED_FOLLOWING":
		return total - 1
	case "OFFSET_PRECEDING":
		if offset > current {
			if start {
				return 0
			}
			return -1
		}
		return current - offset
	case "OFFSET_FOLLOWING":
		if offset >= total-current {
			if start {
				return total
			}
			return total - 1
		}
		return current + offset
	default:
		return current
	}
}

func rangeWindowBound(env ExecEnv, rows []Row, current int, order []OrderItem, kind string, offset int, start bool) (int, error) {
	switch kind {
	case "UNBOUNDED_PRECEDING":
		return 0, nil
	case "UNBOUNDED_FOLLOWING":
		return len(rows) - 1, nil
	}
	// Without an ordering all rows are peers. Offset frames require one key,
	// which is also validated by the parser before reaching execution.
	if len(order) == 0 {
		if start {
			return 0, nil
		}
		return len(rows) - 1, nil
	}
	if kind == "CURRENT" {
		return windowPeerBound(rows, current, order, start), nil
	}
	if len(order) != 1 {
		return 0, fmt.Errorf("RANGE offset requires exactly one ORDER BY column")
	}
	col := strings.ToLower(order[0].Col)
	value, _ := getValLower(rows[current], col)
	if value == nil {
		return windowPeerBound(rows, current, order, start), nil
	}
	number, ok := windowRangeNumber(value)
	if !ok {
		return 0, fmt.Errorf("RANGE offset requires a numeric ORDER BY value")
	}
	delta := new(big.Rat).SetInt64(int64(offset))
	if (kind == "OFFSET_PRECEDING") != order[0].Desc {
		delta.Neg(delta)
	}
	threshold := new(big.Rat).Add(number, delta)
	bound := len(rows)
	if !start {
		bound = -1
	}
	for i, row := range rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return 0, err
			}
		}
		value, _ := getValLower(row, col)
		if value == nil {
			continue
		}
		number, ok := windowRangeNumber(value)
		if !ok {
			return 0, fmt.Errorf("RANGE offset requires a numeric ORDER BY value")
		}
		cmp := number.Cmp(threshold)
		if order[0].Desc {
			cmp = -cmp
		}
		if start && cmp >= 0 {
			return i, nil
		}
		if !start && cmp <= 0 {
			bound = i
		}
	}
	return bound, nil
}

func windowPeerBound(rows []Row, current int, order []OrderItem, start bool) int {
	if !start {
		return lastWindowPeerIndex(rows, current, order)
	}
	for current > 0 && rowsOrderTie(rows[current-1], rows[current], order) {
		current--
	}
	return current
}

func windowRangeNumber(value any) (*big.Rat, bool) {
	switch v := value.(type) {
	case *big.Rat:
		if v == nil {
			return nil, false
		}
		return v, true
	case big.Rat:
		return &v, true
	}
	number, ok := ratFromNumeric(value)
	return number, ok && number != nil // NaN and infinities have no rational value.
}
