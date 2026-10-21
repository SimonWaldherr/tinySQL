package engine

import (
	"cmp"
	"strings"
)

// BETWEEN on a column parses as two comparisons. Fuse compatible bounds so a
// scan reads and checks the column's type once per row. Bound parameters are
// read on each call; cached plans must not retain their initial values.
func buildRawRangeFilter(colIndex map[string]int, leftExpr, rightExpr Expr, outside bool) func([]any) (bool, error) {
	left, lok := leftExpr.(*Binary)
	right, rok := rightExpr.(*Binary)
	if !lok || !rok {
		return nil
	}
	if outside {
		if left.Op != "<" || right.Op != ">" {
			return nil
		}
	} else if left.Op != ">=" || right.Op != "<=" {
		return nil
	}
	lref, lok := left.Left.(*VarRef)
	rref, rok := right.Left.(*VarRef)
	lo, lb := left.Right.(*Literal)
	hi, hb := right.Right.(*Literal)
	if !lok || !rok || !lb || !hb {
		return nil
	}
	column, lok := colIndex[strings.ToLower(lref.Name)]
	rcolumn, rok := colIndex[strings.ToLower(rref.Name)]
	if !lok || !rok || column != rcolumn {
		return nil
	}
	if lo.Parameter || hi.Parameter {
		if lo.Parameter && hi.Parameter {
			return buildBoundRangeFilter(column, lo, hi, outside)
		}
		return nil
	}
	if low, ok := rangeIntegerBound(lo.Val); ok {
		if high, ok := rangeIntegerBound(hi.Val); ok {
			return func(raw []any) (bool, error) {
				switch value := raw[column].(type) {
				case int:
					return rawRangeMatch(int64(value), low, high, outside), nil
				case int64:
					return rawRangeMatch(value, low, high, outside), nil
				case float64:
					return rawRangeMatch(value, float64(low), float64(high), outside), nil
				}
				return false, nil
			}
		}
	}
	switch low := lo.Val.(type) {
	case float64:
		if high, ok := hi.Val.(float64); ok {
			return func(raw []any) (bool, error) {
				value, ok := numericFast(raw[column])
				return ok && rawRangeMatch(value, low, high, outside), nil
			}
		}
	case string:
		if high, ok := hi.Val.(string); ok {
			return func(raw []any) (bool, error) {
				value, ok := raw[column].(string)
				return ok && rawRangeMatch(value, low, high, outside), nil
			}
		}
	}
	return nil
}

func buildBoundRangeFilter(column int, lo, hi *Literal, outside bool) func([]any) (bool, error) {
	lowerOp, upperOp := ">=", "<="
	if outside {
		lowerOp, upperOp = "<", ">"
	}
	lower := buildBoundLiteralFilter(column, lowerOp, lo)
	upper := buildBoundLiteralFilter(column, upperOp, hi)
	return func(raw []any) (bool, error) {
		value := raw[column]
		if lo.Val == nil {
			if outside {
				return upper(raw)
			}
			return false, nil
		}
		if hi.Val == nil {
			match, err := lower(raw)
			return outside && match, err
		}
		if low, ok := rangeIntegerBound(lo.Val); ok {
			if high, ok := rangeIntegerBound(hi.Val); ok {
				switch value := value.(type) {
				case int:
					return rawRangeMatch(int64(value), low, high, outside), nil
				case int64:
					return rawRangeMatch(value, low, high, outside), nil
				case float64:
					return rawRangeMatch(value, float64(low), float64(high), outside), nil
				}
			} else if high, ok := hi.Val.(float64); ok {
				if value, ok := rangeIntegerBound(value); ok {
					return rawSplitRangeMatch(value, low, float64(value), high, outside), nil
				}
				if value, ok := value.(float64); ok {
					return rawRangeMatch(value, float64(low), high, outside), nil
				}
			}
		}
		switch low := lo.Val.(type) {
		case float64:
			if high, ok := hi.Val.(float64); ok {
				if value, ok := numericFast(value); ok {
					return rawRangeMatch(value, low, high, outside), nil
				}
			} else if high, ok := rangeIntegerBound(hi.Val); ok {
				if value, ok := rangeIntegerBound(value); ok {
					return rawSplitRangeMatch(float64(value), low, value, high, outside), nil
				}
				if value, ok := value.(float64); ok {
					return rawRangeMatch(value, low, float64(high), outside), nil
				}
			}
		case string:
			if high, ok := hi.Val.(string); ok {
				if value, ok := value.(string); ok {
					return rawRangeMatch(value, low, high, outside), nil
				}
			}
		}
		// Preserve the existing comparator and its short-circuit order for
		// decimals, timestamps and incomparable types.
		match, err := lower(raw)
		if err != nil || match == outside {
			return match, err
		}
		return upper(raw)
	}
}

// Mixed integer/float bounds must widen only their own comparison. Widening
// both would lose integer precision above 2^53 on the other side of the range.
func rawSplitRangeMatch[L, H cmp.Ordered](lowValue, low L, highValue, high H, outside bool) bool {
	if outside {
		return lowValue < low || highValue > high
	}
	return lowValue >= low && highValue <= high
}

func rangeIntegerBound(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	}
	return 0, false
}

func rawRangeMatch[T cmp.Ordered](value, low, high T, outside bool) bool {
	if outside {
		// Keep the two comparisons: inverting a closed interval would make
		// NaN match NOT BETWEEN, unlike the original '< OR >' predicate.
		return value < low || value > high
	}
	return value >= low && value <= high
}
