// Package engine provides unboxed numeric evaluation for raw table rows.
//
// Predicates and expressions with numeric columns and arithmetic operators (+, -, *, /, %)
// can evaluate intermediate values without boxing each intermediate result into an any
// interface on the heap.
package engine

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

var errNumericFallback = errors.New("fallback to general evaluator")

type rawNum struct {
	intVal   int64
	floatVal float64
	isFloat  bool
	isNull   bool
}

func (n rawNum) toAny() any {
	if n.isNull {
		return nil
	}
	if n.isFloat {
		return n.floatVal
	}
	return intResult(n.intVal)
}

type rawNumericEvaluator func(raw []any) (rawNum, error)

func makeRawColNumericEvaluator(column int, colName string) rawNumericEvaluator {
	return func(raw []any) (rawNum, error) {
		if column < 0 || column >= len(raw) {
			return rawNum{}, fmt.Errorf("column %q is out of range", colName)
		}
		v := raw[column]
		switch x := v.(type) {
		case int:
			return rawNum{intVal: int64(x), isFloat: false}, nil
		case int64:
			return rawNum{intVal: x, isFloat: false}, nil
		case float64:
			return rawNum{floatVal: x, isFloat: true}, nil
		case nil:
			return rawNum{isNull: true}, nil
		default:
			return rawNum{}, errNumericFallback
		}
	}
}

func compileRawNumeric(columns map[string]int, expr Expr) rawNumericEvaluator {
	switch ex := expr.(type) {
	case *Literal:
		if ex.Parameter {
			return func(raw []any) (rawNum, error) {
				v := ex.Val
				switch x := v.(type) {
				case int:
					return rawNum{intVal: int64(x)}, nil
				case int64:
					return rawNum{intVal: x}, nil
				case float64:
					return rawNum{floatVal: x, isFloat: true}, nil
				case nil:
					return rawNum{isNull: true}, nil
				default:
					return rawNum{}, errNumericFallback
				}
			}
		}
		switch x := ex.Val.(type) {
		case int:
			num := rawNum{intVal: int64(x)}
			return func([]any) (rawNum, error) { return num, nil }
		case int64:
			num := rawNum{intVal: x}
			return func([]any) (rawNum, error) { return num, nil }
		case float64:
			num := rawNum{floatVal: x, isFloat: true}
			return func([]any) (rawNum, error) { return num, nil }
		case nil:
			num := rawNum{isNull: true}
			return func([]any) (rawNum, error) { return num, nil }
		default:
			return nil
		}
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		colIdx, ok := columns[key]
		if !ok {
			return nil
		}
		return makeRawColNumericEvaluator(colIdx, ex.Name)
	case *Unary:
		if ex.Op == "+" {
			return compileRawNumeric(columns, ex.Expr)
		}
		if ex.Op == "-" {
			child := compileRawNumeric(columns, ex.Expr)
			if child == nil {
				return nil
			}
			return func(raw []any) (rawNum, error) {
				v, err := child(raw)
				if err != nil || v.isNull {
					return v, err
				}
				if v.isFloat {
					return rawNum{floatVal: -v.floatVal, isFloat: true}, nil
				}
				if v.intVal == math.MinInt64 {
					return rawNum{floatVal: -float64(v.intVal), isFloat: true}, nil
				}
				return rawNum{intVal: -v.intVal, isFloat: false}, nil
			}
		}
		return nil
	case *Binary:
		if !isArithmeticOp(ex.Op) {
			return nil
		}
		left := compileRawNumeric(columns, ex.Left)
		right := compileRawNumeric(columns, ex.Right)
		if left == nil || right == nil {
			return nil
		}
		op := ex.Op
		return func(raw []any) (rawNum, error) {
			l, errL := left(raw)
			if errL != nil && !errors.Is(errL, errNumericFallback) {
				return rawNum{}, errL
			}
			r, errR := right(raw)
			if errR != nil && !errors.Is(errR, errNumericFallback) {
				return rawNum{}, errR
			}
			if errors.Is(errL, errNumericFallback) || errors.Is(errR, errNumericFallback) {
				return rawNum{}, errNumericFallback
			}
			if l.isNull || r.isNull {
				return rawNum{isNull: true}, nil
			}
			switch op {
			case "+":
				if !l.isFloat && !r.isFloat {
					sum, ok := addInt64(l.intVal, r.intVal)
					if ok {
						return rawNum{intVal: sum}, nil
					}
					return rawNum{floatVal: float64(l.intVal) + float64(r.intVal), isFloat: true}, nil
				}
				lf := l.floatVal
				if !l.isFloat {
					lf = float64(l.intVal)
				}
				rf := r.floatVal
				if !r.isFloat {
					rf = float64(r.intVal)
				}
				return rawNum{floatVal: lf + rf, isFloat: true}, nil
			case "-":
				if !l.isFloat && !r.isFloat {
					diff, ok := subInt64(l.intVal, r.intVal)
					if ok {
						return rawNum{intVal: diff}, nil
					}
					return rawNum{floatVal: float64(l.intVal) - float64(r.intVal), isFloat: true}, nil
				}
				lf := l.floatVal
				if !l.isFloat {
					lf = float64(l.intVal)
				}
				rf := r.floatVal
				if !r.isFloat {
					rf = float64(r.intVal)
				}
				return rawNum{floatVal: lf - rf, isFloat: true}, nil
			case "*":
				if !l.isFloat && !r.isFloat {
					prod, ok := mulInt64(l.intVal, r.intVal)
					if ok {
						return rawNum{intVal: prod}, nil
					}
					return rawNum{floatVal: float64(l.intVal) * float64(r.intVal), isFloat: true}, nil
				}
				lf := l.floatVal
				if !l.isFloat {
					lf = float64(l.intVal)
				}
				rf := r.floatVal
				if !r.isFloat {
					rf = float64(r.intVal)
				}
				return rawNum{floatVal: lf * rf, isFloat: true}, nil
			case "/":
				lf := l.floatVal
				if !l.isFloat {
					lf = float64(l.intVal)
				}
				rf := r.floatVal
				if !r.isFloat {
					rf = float64(r.intVal)
				}
				if rf == 0 {
					return rawNum{}, errors.New("division by zero")
				}
				return rawNum{floatVal: lf / rf, isFloat: true}, nil
			case "%":
				if !l.isFloat && !r.isFloat {
					if r.intVal == 0 {
						return rawNum{}, errors.New("division by zero")
					}
					return rawNum{intVal: l.intVal % r.intVal}, nil
				}
				lf := l.floatVal
				if !l.isFloat {
					lf = float64(l.intVal)
				}
				rf := r.floatVal
				if !r.isFloat {
					rf = float64(r.intVal)
				}
				if rf == 0 {
					return rawNum{}, errors.New("division by zero")
				}
				return rawNum{floatVal: math.Mod(lf, rf), isFloat: true}, nil
			default:
				return rawNum{}, fmt.Errorf("unknown arithmetic operator: %s", op)
			}
		}
	}
	return nil
}

func compareFloatUnboxed(ax, f float64) int {
	if ax < f {
		return -1
	}
	if ax > f {
		return 1
	}
	return 0
}

func compareInt64Unboxed(ax, b int64) int {
	if ax < b {
		return -1
	}
	if ax > b {
		return 1
	}
	return 0
}

func evalCmpResult(op string, cmp int) bool {
	switch op {
	case "=":
		return cmp == 0
	case "!=", "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

func compileRawNumericComparisonFilter(columns map[string]int, ex *Binary) func([]any) (bool, error) {
	if !isComparisonOp(ex.Op) {
		return nil
	}
	left := compileRawNumeric(columns, ex.Left)
	right := compileRawNumeric(columns, ex.Right)
	if left == nil || right == nil {
		return nil
	}
	op := ex.Op
	return func(raw []any) (bool, error) {
		l, errL := left(raw)
		if errL != nil && !errors.Is(errL, errNumericFallback) {
			return false, errL
		}
		r, errR := right(raw)
		if errR != nil && !errors.Is(errR, errNumericFallback) {
			return false, errR
		}
		if errors.Is(errL, errNumericFallback) || errors.Is(errR, errNumericFallback) {
			return evalComparisonFallback(columns, ex, raw)
		}
		if l.isNull || r.isNull {
			return false, nil
		}
		if !l.isFloat && !r.isFloat {
			cmp := compareInt64Unboxed(l.intVal, r.intVal)
			return evalCmpResult(op, cmp), nil
		}
		lf := l.floatVal
		if !l.isFloat {
			lf = float64(l.intVal)
		}
		rf := r.floatVal
		if !r.isFloat {
			rf = float64(r.intVal)
		}
		cmp := compareFloatUnboxed(lf, rf)
		return evalCmpResult(op, cmp), nil
	}
}

func compileRawNumericComparisonValue(columns map[string]int, ex *Binary) rawValueEvaluator {
	if !isComparisonOp(ex.Op) {
		return nil
	}
	left := compileRawNumeric(columns, ex.Left)
	right := compileRawNumeric(columns, ex.Right)
	if left == nil || right == nil {
		return nil
	}
	op := ex.Op
	return func(raw []any) (any, error) {
		l, errL := left(raw)
		if errL != nil && !errors.Is(errL, errNumericFallback) {
			return nil, errL
		}
		r, errR := right(raw)
		if errR != nil && !errors.Is(errR, errNumericFallback) {
			return nil, errR
		}
		if errors.Is(errL, errNumericFallback) || errors.Is(errR, errNumericFallback) {
			plan := &simpleSelectPlan{colIndex: columns, rowTextCols: rawRowTextColumns(columns)}
			return evalRawExpr(plan, raw, ex)
		}
		if l.isNull || r.isNull {
			return nil, nil
		}
		if !l.isFloat && !r.isFloat {
			cmp := compareInt64Unboxed(l.intVal, r.intVal)
			return evalCmpResult(op, cmp), nil
		}
		lf := l.floatVal
		if !l.isFloat {
			lf = float64(l.intVal)
		}
		rf := r.floatVal
		if !r.isFloat {
			rf = float64(r.intVal)
		}
		cmp := compareFloatUnboxed(lf, rf)
		return evalCmpResult(op, cmp), nil
	}
}

func evalComparisonFallback(columns map[string]int, ex *Binary, raw []any) (bool, error) {
	plan := &simpleSelectPlan{colIndex: columns, rowTextCols: rawRowTextColumns(columns)}
	v, err := evalRawExpr(plan, raw, ex)
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}

func buildRawFilterBetween(columns map[string]int, ex *BetweenExpr) func([]any) (bool, error) {
	target := compileRawNumeric(columns, ex.Expr)
	lo := compileRawNumeric(columns, ex.Lo)
	hi := compileRawNumeric(columns, ex.Hi)
	if target == nil || lo == nil || hi == nil {
		return nil
	}
	negate := ex.Negate
	return func(raw []any) (bool, error) {
		t, errT := target(raw)
		if errT != nil && !errors.Is(errT, errNumericFallback) {
			return false, errT
		}
		l, errL := lo(raw)
		if errL != nil && !errors.Is(errL, errNumericFallback) {
			return false, errL
		}
		h, errH := hi(raw)
		if errH != nil && !errors.Is(errH, errNumericFallback) {
			return false, errH
		}
		if errors.Is(errT, errNumericFallback) || errors.Is(errL, errNumericFallback) || errors.Is(errH, errNumericFallback) {
			return evalBetweenFallback(columns, ex, raw)
		}
		if t.isNull || l.isNull || h.isNull {
			return false, nil
		}

		if !t.isFloat && !l.isFloat && !h.isFloat {
			match := l.intVal <= t.intVal && t.intVal <= h.intVal
			if negate {
				return !match, nil
			}
			return match, nil
		}

		tf := t.floatVal
		if !t.isFloat {
			tf = float64(t.intVal)
		}
		lf := l.floatVal
		if !l.isFloat {
			lf = float64(l.intVal)
		}
		hf := h.floatVal
		if !h.isFloat {
			hf = float64(h.intVal)
		}

		if math.IsNaN(tf) || math.IsNaN(lf) || math.IsNaN(hf) {
			return evalBetweenFallback(columns, ex, raw)
		}

		match := lf <= tf && tf <= hf
		if negate {
			return !match, nil
		}
		return match, nil
	}
}

func evalBetweenFallback(columns map[string]int, ex *BetweenExpr, raw []any) (bool, error) {
	plan := &simpleSelectPlan{colIndex: columns, rowTextCols: rawRowTextColumns(columns)}
	v, err := evalRawExpr(plan, raw, ex)
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}
