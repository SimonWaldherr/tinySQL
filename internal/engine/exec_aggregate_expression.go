package engine

import (
	"reflect"
	"strings"
)

// Only admit deterministic expressions whose raw evaluation depends on the row.
// Other expressions retain the general evaluator and its evaluation frequency.
func simpleAggregateGroupExpr(e Expr) bool {
	switch ex := e.(type) {
	case *Literal, *VarRef:
		return true
	case *Unary:
		return ex.Op == "NOT" && simpleAggregateGroupExpr(ex.Expr)
	case *Binary:
		return (isArithmeticOp(ex.Op) || isComparisonOp(ex.Op) || ex.Op == "AND" || ex.Op == "OR") && simpleAggregateGroupExpr(ex.Left) && simpleAggregateGroupExpr(ex.Right)
	case *IsNull:
		return simpleAggregateGroupExpr(ex.Expr)
	case *FuncCall:
		switch ex.Name {
		case "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DAYOFWEEK", "DAYOFYEAR", "WEEKOFYEAR", "QUARTER", "ISODOW", "DATE_TRUNC":
		default:
			return false
		}
		if ex.Over != nil || ex.Filter != nil || ex.Distinct || ex.Star {
			return false
		}
		for _, arg := range ex.Args {
			if !simpleAggregateGroupExpr(arg) {
				return false
			}
		}
		return true
	}
	return false
}

type groupsStateArgs struct {
	groups      *aggregateGroups
	plan        *simpleAggregatePlan
	rawPlan     *simpleSelectPlan
	raw, values []any
	cache       []map[string]any
}

func simpleAggregateGroupState(a groupsStateArgs) (*simpleAggregateState, error) {
	if len(a.plan.groupExprs) == 0 {
		return a.groups.state(a.raw, a.plan.groupCols), nil
	}
	for i, expr := range a.plan.groupExprs {
		if a.plan.groupCols[i] >= 0 {
			a.values[i] = a.raw[a.plan.groupCols[i]]
		} else {
			// Cache repeated textual timestamps for deterministic date parts.
			// Keep the cache query-local and bounded for high-cardinality data.
			var key string
			cacheable := false
			if call, ok := expr.(*FuncCall); ok && call.Name != "DATE_TRUNC" && len(call.Args) == 1 {
				if ref, ok := call.Args[0].(*VarRef); ok {
					if col, found := a.plan.colIndex[strings.ToLower(ref.Name)]; found && col >= 0 && col < len(a.raw) {
						key, cacheable = a.raw[col].(string)
					}
				}
			}
			if cacheable {
				if value, found := a.cache[i][key]; found {
					a.values[i] = value
					continue
				}
			}
			value, err := evalRawExpr(a.rawPlan, a.raw, expr)
			if err != nil {
				return nil, err
			}
			if cacheable && len(a.cache[i]) < 1024 {
				if a.cache[i] == nil {
					a.cache[i] = make(map[string]any)
				}
				a.cache[i][key] = value
			}
			a.values[i] = value
		}
	}
	return a.groups.state(a.values, a.plan.groupKeyCols), nil
}

// buildAggregateExpressionProjections keeps aggregate leaves in the normal
// accumulator slots and evaluates arithmetic once per surviving group.
func buildAggregateExpressionProjections(s *Select, colIndex map[string]int, groupCols []int, groupPositions map[int]int) ([]simpleAggregateProjection, []string, []simpleHavingEval, map[*FuncCall]int, bool) {
	plan := &simpleAggregatePlan{colIndex: colIndex, groupCols: groupCols, exprSlots: make(map[*FuncCall]int)}
	names := make([]string, len(s.Projs))
	evals := make([]simpleHavingEval, len(s.Projs))
	complex := make([]Expr, len(s.Projs))
	hasAgg := false
	var collect func(Expr) bool
	collect = func(e Expr) bool {
		switch ex := e.(type) {
		case *Literal, *VarRef:
			return true
		case *Unary:
			return (ex.Op == "+" || ex.Op == "-" || ex.Op == "NOT") && collect(ex.Expr)
		case *IsNull:
			return collect(ex.Expr)
		case *Binary:
			return (isArithmeticOp(ex.Op) || isComparisonOp(ex.Op)) && collect(ex.Left) && collect(ex.Right)
		case *FuncCall:
			if !isAggregateName(ex.Name) {
				return false
			}
			for _, arg := range ex.Args {
				if isAggregate(arg) {
					return false
				}
			}
			if ex.Filter != nil && isAggregate(ex.Filter) {
				return false
			}
			if i, ok := simpleAggregateProjectionForFunc(plan, ex); ok {
				plan.exprSlots[ex] = i
				hasAgg = true
				return true
			}
			proj, _, agg, ok, err := buildSimpleAggregateProjection(SelectItem{Expr: ex}, 0, colIndex, groupPositions)
			if err != nil || !ok || !agg {
				return false
			}
			plan.exprSlots[ex] = len(plan.projs)
			plan.projs = append(plan.projs, proj)
			hasAgg = true
			return true
		}
		return false
	}
	for i, item := range s.Projs {
		if item.Star {
			return nil, nil, nil, nil, false
		}
		names[i] = projName(item, i)
		matched := false
		for j, group := range s.GroupBy {
			if reflect.DeepEqual(item.Expr, group) {
				index := j
				evals[i] = func(state *simpleAggregateState) (any, error) { return state.groupValues[index], nil }
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		// A plain grouped column, or a single aggregate, can use its regular slot.
		proj, _, agg, ok, err := buildSimpleAggregateProjection(item, i, colIndex, groupPositions)
		if err != nil {
			return nil, nil, nil, nil, false
		}
		if ok {
			index := len(plan.projs)
			plan.projs = append(plan.projs, proj)
			evals[i] = func(state *simpleAggregateState) (any, error) {
				return simpleAggregateProjectionValue(state, proj, index), nil
			}
			hasAgg = hasAgg || agg
			continue
		}
		if !isAggregate(item.Expr) || !collect(item.Expr) {
			return nil, nil, nil, nil, false
		}
		complex[i] = item.Expr
	}
	if !hasAgg {
		return nil, nil, nil, nil, false
	}
	for i, expr := range complex {
		if expr != nil {
			evals[i] = compileSimpleHaving(plan, expr)
			if evals[i] == nil {
				return nil, nil, nil, nil, false
			}
		}
	}
	return plan.projs, names, evals, plan.exprSlots, true
}
