package engine

import (
	"fmt"
	"math/big"
	"strings"
)

func evalWindowFrameValue(env ExecEnv, ex *FuncCall, rows []Row, current int, last bool) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s requires one argument", ex.Name)
	}
	frame, err := windowFrameRows(env, rows, current, ex.Over)
	if err != nil || len(frame) == 0 {
		return nil, err
	}
	index := 0
	if last {
		index = len(frame) - 1
	}
	return evalExpr(env, ex.Args[0], frame[index])
}

func windowFrameRows(env ExecEnv, rows []Row, current int, over *OverClause) ([]Row, error) {
	if len(rows) == 0 || current < 0 || current >= len(rows) {
		return nil, nil
	}
	frame := over.Frame
	if frame == nil {
		if len(over.OrderBy) == 0 {
			return rows, nil
		}
		return rows[:lastWindowPeerIndex(rows, current, over.OrderBy)+1], nil
	}
	var start, end int
	if frame.Mode == "RANGE" {
		var err error
		start, err = rangeWindowBound(env, rows, current, over.OrderBy, frame.StartType, frame.StartValue, true)
		if err != nil {
			return nil, err
		}
		end, err = rangeWindowBound(env, rows, current, over.OrderBy, frame.EndType, frame.EndValue, false)
		if err != nil {
			return nil, err
		}
	} else {
		start = rowsWindowBound(current, len(rows), frame.StartType, frame.StartValue, true)
		end = rowsWindowBound(current, len(rows), frame.EndType, frame.EndValue, false)
	}
	if start > end || start >= len(rows) || end < 0 {
		return nil, nil
	}
	return rows[start : end+1], nil
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
