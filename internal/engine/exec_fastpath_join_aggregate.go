// Fast path for the single most common join+aggregate shape: a two-table
// equi-join (one "ON left.col = right.col" condition, no other join
// predicates) combined with a single-column GROUP BY whose key is a column
// from either input table. Without this, that shape falls through the plain
// join fast path (exec_fastpath_join.go excludes any GroupBy) and the plain
// aggregate fast path (exec_fastpath_aggregate.go excludes any Joins), so it
// always takes the generic Row-map join (exec_join.go) followed by
// processAggregateQuery (exec_group.go), which buffers every joined row per
// group and re-scans each group's buffer once per aggregate expression.
//
// This file adds a new, separate, narrowly-scoped fast path rather than
// loosening either existing eligibility check: it reuses the join mechanics
// (simpleJoinPlan, evalJoinRawExpr et al.) from exec_fastpath_join.go and the
// per-group accumulator (simpleAggregateState, accumulate/finalize helpers)
// from exec_fastpath_aggregate.go, mirroring executeSimpleAggregateFastPath's
// one-column strategy of using the grouped value itself as a map key. Any
// query shape outside this narrow case -- multi-column GROUP BY, non-equi
// joins, multiple join conditions, HAVING, more than two tables, ORDER BY,
// LIMIT/OFFSET, DISTINCT -- is rejected by simpleJoinAggregateEligibleSelect
// and falls through unchanged to the two existing fast paths (which already
// reject it) and ultimately the generic path.
package engine

import (
	"math/big"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// simpleJoinAggregatePlan combines a two-table equi-join plan with a single
// group-by column (resolved to a side+index, exactly like a direct-column
// simpleProjection) and the aggregate projections to accumulate per group.
type simpleJoinAggregatePlan struct {
	join       *simpleJoinPlan
	groupSide  int // 0 = left table column, 1 = right table column
	groupCol   int // column index within that side
	projs      []simpleAggregateProjection
	outputCols []string
}

func executeSimpleJoinAggregateFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleJoinAggregatePlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rightByKey, err := plan.join.rightRowsByKey()
	if err != nil {
		return nil, true, err
	}
	if simpleJoinAggregateCountStarOnly(plan) {
		return executeSimpleJoinCountStarFastPath(env, plan, rightByKey)
	}

	groups := make(map[string]*simpleAggregateState)
	order := make([]*simpleAggregateState, 0)
	keyBuf := make([]byte, 0, 32)
	for i, left := range plan.join.left.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		if plan.join.leftFilter != nil {
			match, err := plan.join.leftFilter(left)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}
		}
		leftKeyVal := left[plan.join.leftKey]
		if leftKeyVal == nil {
			continue
		}
		matches := rightByKey[comparableKeyPart(leftKeyVal)]
		for _, right := range matches {
			match, err := evalJoinRawWhere(plan.join, left, right)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}

			var groupValue any
			if plan.groupSide == 0 {
				groupValue = left[plan.groupCol]
			} else {
				groupValue = right[plan.groupCol]
			}
			keyBuf = writeSingleGroupKey(keyBuf[:0], groupValue)
			state, exists := groups[string(keyBuf)]
			if !exists {
				state = newSimpleAggregateState([]any{groupValue}, len(plan.projs))
				groups[string(keyBuf)] = state
				order = append(order, state)
			}
			if err := accumulateSimpleJoinAggregateState(plan.join, left, right, state, plan.projs); err != nil {
				return nil, true, err
			}
		}
	}

	return simpleAggregateResultSet(plan.projs, plan.outputCols, order), true, nil
}

// simpleJoinAggregateCountStarOnly identifies the particularly common
// `SELECT group_col, COUNT(*) ... GROUP BY group_col` shape. A regular
// simpleAggregateState is deliberately general (it can hold decimal SUM/AVG
// and MIN/MAX state), but needs five backing slices for each group. COUNT(*)
// needs only a group value and an integer, so keeping that state compact makes
// high-cardinality join/group queries substantially lighter on the allocator.
func simpleJoinAggregateCountStarOnly(plan *simpleJoinAggregatePlan) bool {
	if len(plan.projs) != 2 {
		return false
	}
	groupCols, countStars := 0, 0
	for _, proj := range plan.projs {
		switch proj.kind {
		case aggGroupCol:
			groupCols++
		case aggCount:
			if proj.arg != nil {
				return false
			}
			countStars++
		default:
			return false
		}
	}
	return groupCols == 1 && countStars == 1
}

type simpleJoinCountState struct {
	groupValue any
	count      int
}

func executeSimpleJoinCountStarFastPath(env ExecEnv, plan *simpleJoinAggregatePlan, rightByKey map[any][][]any) (*ResultSet, bool, error) {
	// Keep the compact states in one contiguous slice. The map only needs an
	// index, which avoids one heap object for every distinct group.
	groups := make(map[string]int)
	order := make([]simpleJoinCountState, 0)
	keyBuf := make([]byte, 0, 32)
	for i, left := range plan.join.left.Rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		if plan.join.leftFilter != nil {
			match, err := plan.join.leftFilter(left)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}
		}
		leftKey := left[plan.join.leftKey]
		if leftKey == nil {
			continue
		}
		for _, right := range rightByKey[comparableKeyPart(leftKey)] {
			match, err := evalJoinRawWhere(plan.join, left, right)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}
			groupValue := left[plan.groupCol]
			if plan.groupSide == 1 {
				groupValue = right[plan.groupCol]
			}
			keyBuf = writeSingleGroupKey(keyBuf[:0], groupValue)
			stateIndex, exists := groups[string(keyBuf)]
			if !exists {
				stateIndex = len(order)
				groups[string(keyBuf)] = stateIndex
				order = append(order, simpleJoinCountState{groupValue: groupValue})
			}
			order[stateIndex].count++
		}
	}

	rows := make([]Row, 0, len(order))
	for _, state := range order {
		row := make(Row, len(plan.projs))
		for _, proj := range plan.projs {
			if proj.kind == aggGroupCol {
				row[strings.ToLower(proj.name)] = state.groupValue
			} else {
				row[strings.ToLower(proj.name)] = state.count
			}
		}
		rows = append(rows, row)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: rows}, true, nil
}

// accumulateSimpleJoinAggregateState mirrors accumulateSimpleAggregateState
// (exec_fastpath_aggregate.go) exactly, but evaluates each aggregate's
// argument against a joined (left, right) row pair via evalJoinRawExpr
// instead of a single-table raw row via evalRawExpr. The two are kept as
// separate functions (rather than parameterizing one over an eval closure)
// so the already-optimized single-table accumulation loop is not made to pay
// for a per-row closure allocation it does not need.
func accumulateSimpleJoinAggregateState(join *simpleJoinPlan, left, right []any, state *simpleAggregateState, projs []simpleAggregateProjection) error {
	for i, proj := range projs {
		switch proj.kind {
		case aggGroupCol:
			continue
		case aggCount:
			if proj.arg == nil {
				state.counts[i]++
				continue
			}
			v, err := evalJoinRawExpr(join, left, right, proj.arg)
			if err != nil {
				return err
			}
			if v != nil {
				state.counts[i]++
			}
		case aggSum, aggAvg:
			v, err := evalJoinRawExpr(join, left, right, proj.arg)
			if err != nil {
				return err
			}
			if v == nil {
				continue
			}
			if f, ok := numeric(v); ok {
				if state.useRat != nil && state.useRat[i] {
					state.sumRat[i].Add(state.sumRat[i], new(big.Rat).SetFloat64(f))
				} else {
					state.sumFloat[i] += f
				}
				state.counts[i]++
				continue
			}
			if rv, ok := storage.DecimalFromAny(v); ok {
				if state.useRat == nil {
					state.useRat = make([]bool, len(projs))
					state.sumRat = make([]*big.Rat, len(projs))
				}
				if !state.useRat[i] {
					state.sumRat[i] = new(big.Rat)
					if state.counts[i] > 0 {
						state.sumRat[i].SetFloat64(state.sumFloat[i])
					}
					state.useRat[i] = true
				}
				state.sumRat[i].Add(state.sumRat[i], new(big.Rat).Set(rv))
				state.counts[i]++
			}
		case aggMin, aggMax:
			v, err := evalJoinRawExpr(join, left, right, proj.arg)
			if err != nil {
				return err
			}
			if v == nil {
				continue
			}
			if !state.haveMinMax[i] {
				state.minmax[i] = v
				state.haveMinMax[i] = true
				continue
			}
			cmp, err := compare(v, state.minmax[i])
			if err != nil {
				continue
			}
			if (proj.kind == aggMin && cmp < 0) || (proj.kind == aggMax && cmp > 0) {
				state.minmax[i] = v
			}
		}
	}
	return nil
}

func buildSimpleJoinAggregatePlan(env ExecEnv, s *Select) (*simpleJoinAggregatePlan, bool, error) {
	if !simpleJoinAggregateEligibleSelect(s) {
		return nil, false, nil
	}
	if isCatalogViewSource(env, s.From.Table) || isCatalogViewSource(env, s.Joins[0].Right.Table) {
		return nil, false, nil
	}
	if anyWindowInSelect(s.Projs) || !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}
	groupRef, ok := s.GroupBy[0].(*VarRef)
	if !ok {
		return nil, false, nil
	}

	left, right, err := loadSimpleJoinTables(env, s)
	if err != nil {
		return nil, true, err
	}
	cache := s.simpleJoinAggregatePlanCache
	// A bound parameter can be stored in an expression node while the parsed
	// statement is reused. Cache only parameter-independent plans, exactly as
	// the plain join fast path does.
	cacheable := cache != nil && !exprContainsBoundParameter(s.Where)
	if cacheable {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if cache.plan != nil && cache.left == left && cache.right == right {
			return cache.plan, true, nil
		}
	}

	leftIndex := simpleColumnIndex(left, aliasOr(s.From))
	rightIndex := simpleColumnIndex(right, aliasOr(s.Joins[0].Right))
	leftKey, rightKey, ok := simpleJoinKeys(s.Joins[0].On, leftIndex, rightIndex)
	if !ok {
		return nil, false, nil
	}
	if !simpleJoinExprResolvable(s.Where, leftIndex, rightIndex) {
		return nil, false, nil
	}

	groupSide, groupCol := resolveSimpleJoinProjectionRef(groupRef, leftIndex, rightIndex)
	if groupCol < 0 {
		return nil, false, nil
	}

	projs, outputCols, hasAgg, eligible := buildSimpleJoinAggregateProjections(s.Projs, leftIndex, rightIndex, groupSide, groupCol)
	if !eligible || !hasAgg {
		return nil, false, nil
	}

	joinPlan := &simpleJoinPlan{
		left:       left,
		right:      right,
		leftIndex:  leftIndex,
		rightIndex: rightIndex,
		leftKey:    leftKey,
		rightKey:   rightKey,
		where:      s.Where,
	}
	joinPlan.leftFilter, joinPlan.rightFilter, joinPlan.where = buildSimpleJoinFilters(s.Where, leftIndex, rightIndex)

	plan := &simpleJoinAggregatePlan{
		join:       joinPlan,
		groupSide:  groupSide,
		groupCol:   groupCol,
		projs:      projs,
		outputCols: outputCols,
	}
	if cacheable {
		cache.left = left
		cache.right = right
		cache.plan = plan
	}
	return plan, true, nil
}

// simpleJoinAggregateEligibleSelect mirrors simpleJoinSelectEligible exactly,
// except it requires exactly one GROUP BY column instead of none. It does not
// touch simpleJoinSelectEligible (which continues to require zero GROUP BY
// columns) or simpleAggregateEligibleSelect (which continues to require zero
// joins) -- this is an additional, separate eligibility gate for the one
// shape both of those deliberately exclude.
func simpleJoinAggregateEligibleSelect(s *Select) bool {
	return !s.Distinct && len(s.DistinctOn) <= 0 && len(s.CTEs) <= 0 &&
		s.Having == nil && s.Union == nil && len(s.OrderBy) <= 0 && s.Limit == nil && s.Offset == nil &&
		s.From.Table != "" && s.From.Subquery == nil && s.From.TableFunc == nil && len(s.Joins) == 1 &&
		s.Joins[0].Type == JoinInner && s.Joins[0].Right.Table != "" && s.Pivot == nil &&
		s.Joins[0].Right.Subquery == nil && s.Joins[0].Right.TableFunc == nil &&
		!isSQLiteSchemaTable(s.From.Table) && !isSQLiteSchemaTable(s.Joins[0].Right.Table) &&
		len(s.GroupBy) == 1
}

// buildSimpleJoinAggregateProjections accepts only a bare reference to the
// (single) GROUP BY column, or a supported aggregate function over a
// join-resolvable argument -- the same restriction buildSimpleAggregateProjection
// applies for the no-join case. Anything else (a differently-named bare
// column, a non-aggregate expression, COUNT(DISTINCT ...), a window
// function, ...) reports ineligible so the caller falls through to the
// generic path unchanged.
func buildSimpleJoinAggregateProjections(items []SelectItem, leftIndex, rightIndex map[string]int, groupSide, groupCol int) ([]simpleAggregateProjection, []string, bool, bool) {
	projs := make([]simpleAggregateProjection, 0, len(items))
	outputCols := make([]string, 0, len(items))
	hasAgg := false

	for i, it := range items {
		if it.Star {
			return nil, nil, false, false
		}
		name := projName(it, i)

		if ref, ok := it.Expr.(*VarRef); ok {
			side, colIdx := resolveSimpleJoinProjectionRef(ref, leftIndex, rightIndex)
			if colIdx < 0 || side != groupSide || colIdx != groupCol {
				return nil, nil, false, false
			}
			projs = append(projs, simpleAggregateProjection{name: name, kind: aggGroupCol, groupIndex: 0})
			outputCols = append(outputCols, name)
			continue
		}

		fc, ok := it.Expr.(*FuncCall)
		if !ok || fc.Distinct || fc.Over != nil {
			return nil, nil, false, false
		}
		var kind aggKind
		switch fc.Name {
		case "COUNT":
			kind = aggCount
			if fc.Star {
				if len(fc.Args) != 0 {
					return nil, nil, false, false
				}
			} else if len(fc.Args) != 1 {
				return nil, nil, false, false
			}
		case "SUM":
			kind = aggSum
		case "AVG":
			kind = aggAvg
		case "MIN":
			kind = aggMin
		case "MAX":
			kind = aggMax
		default:
			return nil, nil, false, false
		}
		if kind != aggCount && (fc.Star || len(fc.Args) != 1) {
			return nil, nil, false, false
		}

		var arg Expr
		if !fc.Star {
			arg = fc.Args[0]
			if !isSimpleRawExpr(arg) || !simpleJoinExprResolvable(arg, leftIndex, rightIndex) {
				return nil, nil, false, false
			}
		}
		projs = append(projs, simpleAggregateProjection{name: name, kind: kind, arg: arg})
		outputCols = append(outputCols, name)
		hasAgg = true
	}
	return projs, outputCols, hasAgg, true
}
