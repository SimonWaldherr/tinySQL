package engine

import "strings"

type simpleHavingEval func(*simpleAggregateState) (any, error)

// compileSimpleHaving binds column and aggregate slots once per query, avoiding
// expression trees and row maps per group. Unsupported expressions fall back.
func compileSimpleHaving(plan *simpleAggregatePlan, e Expr) simpleHavingEval {
	switch ex := e.(type) {
	case *Literal:
		return func(*simpleAggregateState) (any, error) { return ex.Val, nil }
	case *VarRef:
		name := ex.Lower
		if name == "" {
			name = strings.ToLower(ex.Name)
		}
		col, ok := plan.colIndex[name]
		if !ok {
			return nil
		}
		for i, groupCol := range plan.groupCols {
			if groupCol == col {
				return func(s *simpleAggregateState) (any, error) { return s.groupValues[i], nil }
			}
		}
	case *FuncCall:
		i, ok := simpleAggregateProjectionForFunc(plan, ex)
		if ok {
			proj := plan.projs[i]
			return func(s *simpleAggregateState) (any, error) { return simpleAggregateProjectionValue(s, proj, i), nil }
		}
	case *Unary:
		inner := compileSimpleHaving(plan, ex.Expr)
		if inner == nil || (ex.Op != "+" && ex.Op != "-" && ex.Op != "NOT") {
			return nil
		}
		return func(s *simpleAggregateState) (any, error) {
			v, err := inner(s)
			if err != nil {
				return nil, err
			}
			return applyUnaryOp(ex.Op, v)
		}
	case *IsNull:
		inner := compileSimpleHaving(plan, ex.Expr)
		if inner == nil {
			return nil
		}
		return func(s *simpleAggregateState) (any, error) {
			v, err := inner(s)
			if err != nil {
				return nil, err
			}
			return isNull(v) != ex.Negate, nil
		}
	case *Binary:
		if ex.Op != "AND" && ex.Op != "OR" && !isComparisonOp(ex.Op) && !isArithmeticOp(ex.Op) {
			return nil
		}
		left, right := compileSimpleHaving(plan, ex.Left), compileSimpleHaving(plan, ex.Right)
		if left == nil || right == nil {
			return nil
		}
		return func(s *simpleAggregateState) (any, error) {
			lv, err := left(s)
			if err != nil {
				return nil, err
			}
			if ex.Op == "AND" && toTri(lv) == tvFalse {
				return false, nil
			}
			if ex.Op == "OR" && toTri(lv) == tvTrue {
				return true, nil
			}
			rv, err := right(s)
			if err != nil {
				return nil, err
			}
			switch ex.Op {
			case "AND":
				return triToValue(triAnd(toTri(lv), toTri(rv))), nil
			case "OR":
				return triToValue(triOr(toTri(lv), toTri(rv))), nil
			}
			if isArithmeticOp(ex.Op) {
				return evalArithmeticBinary(ex.Op, lv, rv)
			}
			return evalComparisonBinary(ex.Op, lv, rv)
		}
	}
	return nil
}
