package engine

import "strings"

type joinPredicate func(left, right Row) (int, error)
type joinOperand func(left, right Row) (any, error)

func compileAdditionalJoinPredicate(env ExecEnv, condition Expr, equality *joinEquality) joinPredicate {
	if equality != nil {
		return nil
	}
	return compileJoinPredicate(env, condition)
}

// compileJoinPredicate evaluates scalar ON expressions against both source
// rows so rejected pairs never need a merged Row. Functions and subqueries
// still receive the ordinary merged row through the general evaluator.
func compileJoinPredicate(env ExecEnv, condition Expr) joinPredicate {
	switch ex := condition.(type) {
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" {
			return compileJoinLogic(env, ex)
		}
		if !isComparisonOp(ex.Op) {
			return nil
		}
		left, right := compileJoinOperand(env, ex.Left), compileJoinOperand(env, ex.Right)
		if left == nil || right == nil {
			return nil
		}
		op := ex.Op
		return func(l, r Row) (int, error) {
			a, err := left(l, r)
			if err != nil {
				return tvFalse, err
			}
			b, err := right(l, r)
			if err != nil {
				return tvFalse, err
			}
			value, err := evalComparisonBinary(op, a, b)
			return toTri(value), err
		}
	case *Literal:
		return func(_, _ Row) (int, error) { return toTri(ex.Val), nil }
	}
	return nil
}

func compileJoinLogic(env ExecEnv, ex *Binary) joinPredicate {
	left, right := compileJoinPredicate(env, ex.Left), compileJoinPredicate(env, ex.Right)
	if left == nil || right == nil {
		return nil
	}
	stop, combine := tvFalse, triAnd
	if ex.Op == "OR" {
		stop, combine = tvTrue, triOr
	}
	return func(l, r Row) (int, error) {
		a, err := left(l, r)
		if err != nil || a == stop {
			return a, err
		}
		b, err := right(l, r)
		if err != nil {
			return tvFalse, err
		}
		return combine(a, b), nil
	}
}

func compileJoinOperand(env ExecEnv, operand Expr) joinOperand {
	switch ex := operand.(type) {
	case *Literal:
		return func(_, _ Row) (any, error) { return ex.Val, nil }
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		return func(l, r Row) (any, error) {
			if v, found := mergedJoinValue(l, r, key); found {
				return v, nil
			}
			// Keep fallback bindings and column suggestions when neither
			// source row contains this key.
			return evalVarRef(env, ex, mergeRows(l, r))
		}
	}
	return nil
}
