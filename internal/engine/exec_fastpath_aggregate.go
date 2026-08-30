// Fast path for grouped aggregation: accumulate COUNT/SUM/AVG/MIN/MAX directly
// off stored rows in a single scan, instead of buffering each group's rows and
// re-scanning them once per aggregate expression.
//
// This also covers the ungrouped case (no GROUP BY at all, e.g. "SELECT
// COUNT(*) FROM t" or "SELECT SUM(x), AVG(y) FROM t WHERE ..."), which used to
// be excluded here and fall all the way back to the general path: resolveFromClause
// materializes a dual-key Row map for every row of the table via rowsFromTable
// before aggregation even begins, for a query shape that is arguably the most
// common one in SQL. The accumulator machinery below already treats "zero
// group-by columns" as one implicit group (see executeSimpleMultiGroupAggregate),
// so the only genuinely new piece is synthesizing that one group's output row
// when no input row matched — a whole-table aggregate must still return exactly
// one row over zero matching rows (COUNT(*) = 0, SUM/AVG/MIN/MAX = NULL),
// whereas a real "GROUP BY x" correctly returns zero rows in that case. See the
// identical contract and its rationale in processAggregateQuery (exec_group.go).
package engine

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type simpleAggregatePlan struct {
	table      *storage.Table
	colIndex   map[string]int
	groupCols  []int
	where      Expr
	having     Expr
	orderBy    []OrderItem
	limit      *int
	offset     *int
	projs      []simpleAggregateProjection
	outputCols []string
}

// aggKind identifies which aggregate a simpleAggregateProjection computes in
// the raw fast path. Kept as a small enum (rather than a string) so the hot
// per-row switch in executeSimpleAggregateFastPath stays branch-cheap.
type aggKind byte

const (
	aggGroupCol aggKind = iota
	aggCount
	aggSum
	aggAvg
	aggMin
	aggMax
)

type simpleAggregateProjection struct {
	name       string
	kind       aggKind
	arg        Expr // nil for the group-by column and for COUNT(*)
	groupIndex int  // only used by aggGroupCol
}

// simpleAggregateState accumulates one group's aggregates directly (SUM as a
// running float/rational, MIN/MAX as a running best value) instead of
// buffering every matching row and re-scanning it once per aggregate
// expression, which is what the general (non-fast-path) GROUP BY evaluator
// does. sumRat/useRat mirror evalAggregateSumAvg's float->big.Rat promotion
// so SUM/AVG over DECIMAL/MONEY columns stays exact; they're left nil for
// groups that never see a decimal value, avoiding the allocation entirely
// for the common all-numeric case.
type simpleAggregateState struct {
	groupValues []any
	counts      []int // COUNT result, or non-null sample count for SUM/AVG
	sumFloat    []float64
	sumRat      []*big.Rat
	useRat      []bool
	minmax      []any
	haveMinMax  []bool
}

func executeSimpleAggregateFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleAggregatePlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rawPlan := &simpleSelectPlan{table: plan.table, colIndex: plan.colIndex, where: plan.where, filter: buildRawFilter(plan.colIndex, plan.where)}
	if len(plan.groupCols) == 1 {
		return executeSimpleSingleGroupAggregate(env, plan, rawPlan)
	}
	return executeSimpleMultiGroupAggregate(env, plan, rawPlan)
}

func executeSimpleSingleGroupAggregate(env ExecEnv, plan *simpleAggregatePlan, rawPlan *simpleSelectPlan) (*ResultSet, bool, error) {
	// A map[any] looks natural here, but converting a string group value from
	// a row into an interface makes it escape on every lookup.  On a large
	// text GROUP BY that turns one otherwise allocation-free scan into one
	// allocation per input row.  Use the same framed key representation as the
	// multi-column path instead: string(keyBuf) is allocation-free for a map
	// lookup, while a stable string is materialized only for a new group.
	groups := make(map[string]*simpleAggregateState)
	order := make([]*simpleAggregateState, 0)
	groupCol := plan.groupCols[0]
	keyBuf := make([]byte, 0, 32)
	for i, raw := range plan.table.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(rawPlan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}

		groupValue := raw[groupCol]
		keyBuf = writeSingleGroupKey(keyBuf[:0], groupValue)
		state, exists := groups[string(keyBuf)]
		if !exists {
			state = newSimpleAggregateState([]any{groupValue}, len(plan.projs))
			groups[string(keyBuf)] = state
			order = append(order, state)
		}
		if err := accumulateSimpleAggregateState(env, rawPlan, raw, state, plan.projs); err != nil {
			return nil, true, err
		}
	}
	rs, err := finalizeSimpleAggregateResultSet(env, plan, order)
	return rs, true, err
}

func executeSimpleMultiGroupAggregate(env ExecEnv, plan *simpleAggregatePlan, rawPlan *simpleSelectPlan) (*ResultSet, bool, error) {
	groups := make(map[string]*simpleAggregateState)
	order := make([]*simpleAggregateState, 0)
	// keyBuf is reused across rows via keyBuf[:0] (retaining its backing
	// array) rather than resetting a *strings.Builder to nil every row — see
	// the writeFmtKeyPart doc comment. Real GROUP BY workloads have far fewer
	// distinct groups than rows, so most rows hit the zero-allocation map
	// lookup below and only the first row of each group pays for a real
	// string allocation.
	keyBuf := make([]byte, 0, 64)
	for rowIdx, raw := range plan.table.Rows {
		if rowIdx&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(rawPlan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}

		keyBuf = keyBuf[:0]
		for i, groupCol := range plan.groupCols {
			if i > 0 {
				keyBuf = append(keyBuf, '\x1f')
			}
			keyBuf = writeFmtKeyPart(keyBuf, raw[groupCol])
		}
		state, exists := groups[string(keyBuf)]
		if !exists {
			key := string(keyBuf)
			values := make([]any, len(plan.groupCols))
			for i, groupCol := range plan.groupCols {
				values[i] = raw[groupCol]
			}
			state = newSimpleAggregateState(values, len(plan.projs))
			groups[key] = state
			order = append(order, state)
		}
		if err := accumulateSimpleAggregateState(env, rawPlan, raw, state, plan.projs); err != nil {
			return nil, true, err
		}
	}
	// A whole-table aggregate (no GROUP BY) always produces exactly one row,
	// even over zero matching input rows: see the package doc comment and
	// processAggregateQuery's identical synthesis (exec_group.go). A real
	// "GROUP BY x" correctly produces zero rows here instead, which is why
	// this is conditioned on zero group-by columns specifically, not just an
	// empty result.
	if len(plan.groupCols) == 0 && len(order) == 0 {
		order = append(order, newSimpleAggregateState(nil, len(plan.projs)))
	}
	rs, err := finalizeSimpleAggregateResultSet(env, plan, order)
	return rs, true, err
}

func newSimpleAggregateState(groupValues []any, projections int) *simpleAggregateState {
	return &simpleAggregateState{
		groupValues: groupValues,
		counts:      make([]int, projections),
		sumFloat:    make([]float64, projections),
		minmax:      make([]any, projections),
		haveMinMax:  make([]bool, projections),
	}
}

func accumulateSimpleAggregateState(env ExecEnv, rawPlan *simpleSelectPlan, raw []any, state *simpleAggregateState, projs []simpleAggregateProjection) error {
	for i, proj := range projs {
		switch proj.kind {
		case aggGroupCol:
			continue
		case aggCount:
			if proj.arg == nil {
				state.counts[i]++
				continue
			}
			v, err := evalRawExpr(rawPlan, raw, proj.arg)
			if err != nil {
				return err
			}
			if v != nil {
				state.counts[i]++
			}
		case aggSum, aggAvg:
			v, err := evalRawExpr(rawPlan, raw, proj.arg)
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
			v, err := evalRawExpr(rawPlan, raw, proj.arg)
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

// finalizeSimpleAggregateResultSet applies a supported HAVING expression to
// aggregate states before materializing result maps. This preserves the raw
// scan for common HAVING clauses such as COUNT(*) > 10 instead of falling
// back to rowsFromTable and allocating a Row map for every input row.
func finalizeSimpleAggregateResultSet(env ExecEnv, plan *simpleAggregatePlan, order []*simpleAggregateState) (*ResultSet, error) {
	if plan.having != nil {
		kept := order[:0]
		for i, state := range order {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
			}
			match, err := evalSimpleAggregateHaving(env, plan, state, plan.having)
			if err != nil {
				return nil, err
			}
			if match {
				kept = append(kept, state)
			}
		}
		order = kept
	}
	result := simpleAggregateResultSet(plan.projs, plan.outputCols, order)
	if len(plan.orderBy) > 0 {
		result.Rows = applySortOrderWithLimit(plan.orderBy, result.Rows, plan.limit, plan.offset)
	}
	result.Rows = applyOffsetLimit(&Select{Limit: plan.limit, Offset: plan.offset}, result.Rows)
	return result, nil
}

// simpleAggregateResultSet materializes one output Row per accumulated group.
// It takes projections/outputCols directly (rather than a *simpleAggregatePlan)
// so the join+aggregate fast path (exec_fastpath_join_aggregate.go) can share
// it too: that path accumulates simpleAggregateState the same way this file's
// single-table paths do, but its rows come from joined (left, right) pairs
// instead of plan.table.Rows, so it has no simpleAggregatePlan of its own.
func simpleAggregateResultSet(projs []simpleAggregateProjection, outputCols []string, order []*simpleAggregateState) *ResultSet {
	outRows := make([]Row, 0, len(order))
	for _, state := range order {
		out := make(Row, len(projs))
		for i, proj := range projs {
			putVal(out, proj.name, simpleAggregateProjectionValue(state, proj, i))
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: outputCols, Rows: outRows}
}

func simpleAggregateProjectionValue(state *simpleAggregateState, proj simpleAggregateProjection, i int) any {
	switch proj.kind {
	case aggGroupCol:
		return state.groupValues[proj.groupIndex]
	case aggCount:
		return state.counts[i]
	case aggSum:
		// Match evalAggregateSumAvg: SUM over no non-NULL input values is
		// NULL, not the float accumulator's zero value. In particular this
		// keeps the raw aggregate fast paths consistent with PIVOT and the
		// general aggregate evaluator.
		if state.counts[i] == 0 {
			return nil
		}
		if state.useRat != nil && state.useRat[i] {
			return state.sumRat[i]
		}
		return state.sumFloat[i]
	case aggAvg:
		if state.counts[i] == 0 {
			return nil
		}
		if state.useRat != nil && state.useRat[i] {
			return new(big.Rat).Quo(state.sumRat[i], big.NewRat(int64(state.counts[i]), 1))
		}
		return state.sumFloat[i] / float64(state.counts[i])
	case aggMin, aggMax:
		if state.haveMinMax[i] {
			return state.minmax[i]
		}
	}
	return nil
}

// simpleAggregateHavingSupported limits the HAVING fast path to expressions
// whose values are available from the aggregate state. Queries outside this
// subset continue through the general aggregate evaluator unchanged.
func simpleAggregateHavingSupported(plan *simpleAggregatePlan, e Expr) bool {
	switch ex := e.(type) {
	case *Literal:
		return true
	case *VarRef:
		_, ok := simpleAggregateGroupValue(plan, nil, ex)
		return ok
	case *FuncCall:
		_, ok := simpleAggregateProjectionForFunc(plan, ex)
		return ok
	case *Unary:
		return (ex.Op == "+" || ex.Op == "-" || ex.Op == "NOT") && simpleAggregateHavingSupported(plan, ex.Expr)
	case *Binary:
		return (ex.Op == "AND" || ex.Op == "OR" || isComparisonOp(ex.Op) || isArithmeticOp(ex.Op)) &&
			simpleAggregateHavingSupported(plan, ex.Left) && simpleAggregateHavingSupported(plan, ex.Right)
	case *IsNull:
		return simpleAggregateHavingSupported(plan, ex.Expr)
	default:
		return false
	}
}

// evalSimpleAggregateHaving first binds grouped-column and aggregate values
// from state into literals, then delegates the SQL operators to evalExpr. The
// latter keeps NULL and three-valued-logic behavior identical to the general
// aggregate path without materializing source rows as Row maps.
func evalSimpleAggregateHaving(env ExecEnv, plan *simpleAggregatePlan, state *simpleAggregateState, e Expr) (bool, error) {
	bound, ok, err := bindSimpleAggregateHaving(plan, state, e)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("unsupported HAVING expression in simple aggregate plan")
	}
	v, err := evalExpr(env, bound, Row{})
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}

func bindSimpleAggregateHaving(plan *simpleAggregatePlan, state *simpleAggregateState, e Expr) (Expr, bool, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex, true, nil
	case *VarRef:
		value, ok := simpleAggregateGroupValue(plan, state, ex)
		if !ok {
			return nil, false, nil
		}
		return &Literal{Val: value}, true, nil
	case *FuncCall:
		idx, ok := simpleAggregateProjectionForFunc(plan, ex)
		if !ok {
			return nil, false, nil
		}
		return &Literal{Val: simpleAggregateProjectionValue(state, plan.projs[idx], idx)}, true, nil
	case *Unary:
		inner, ok, err := bindSimpleAggregateHaving(plan, state, ex.Expr)
		if err != nil || !ok {
			return nil, ok, err
		}
		return &Unary{Op: ex.Op, Expr: inner}, true, nil
	case *Binary:
		left, ok, err := bindSimpleAggregateHaving(plan, state, ex.Left)
		if err != nil || !ok {
			return nil, ok, err
		}
		right, ok, err := bindSimpleAggregateHaving(plan, state, ex.Right)
		if err != nil || !ok {
			return nil, ok, err
		}
		return &Binary{Op: ex.Op, Left: left, Right: right}, true, nil
	case *IsNull:
		inner, ok, err := bindSimpleAggregateHaving(plan, state, ex.Expr)
		if err != nil || !ok {
			return nil, ok, err
		}
		return &IsNull{Expr: inner, Negate: ex.Negate}, true, nil
	default:
		return nil, false, nil
	}
}

func simpleAggregateGroupValue(plan *simpleAggregatePlan, state *simpleAggregateState, ref *VarRef) (any, bool) {
	name := ref.Lower
	if name == "" {
		name = strings.ToLower(ref.Name)
	}
	col, ok := plan.colIndex[name]
	if !ok {
		return nil, false
	}
	for i, groupCol := range plan.groupCols {
		if groupCol == col {
			if state == nil {
				return nil, true
			}
			return state.groupValues[i], true
		}
	}
	return nil, false
}

func simpleAggregateProjectionForFunc(plan *simpleAggregatePlan, fc *FuncCall) (int, bool) {
	if fc == nil || fc.Distinct || fc.Over != nil {
		return 0, false
	}
	var kind aggKind
	switch fc.Name {
	case "COUNT":
		kind = aggCount
		if fc.Star {
			if len(fc.Args) != 0 {
				return 0, false
			}
		} else if len(fc.Args) != 1 {
			return 0, false
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
		return 0, false
	}
	if kind != aggCount && (fc.Star || len(fc.Args) != 1) {
		return 0, false
	}
	var arg Expr
	if !fc.Star {
		arg = fc.Args[0]
	}
	for i, proj := range plan.projs {
		if proj.kind == kind && simpleAggregateArgumentsEqual(proj.arg, arg) {
			return i, true
		}
	}
	return 0, false
}

// simpleAggregateArgumentsEqual deliberately accepts only the direct column
// and literal expressions that dominate HAVING clauses. More complex
// aggregate arguments safely use the established general path.
func simpleAggregateArgumentsEqual(left, right Expr) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	switch l := left.(type) {
	case *VarRef:
		r, ok := right.(*VarRef)
		return ok && strings.EqualFold(l.Name, r.Name)
	case *Literal:
		r, ok := right.(*Literal)
		return ok && l.Parameter == r.Parameter && rawEqual(l.Val, r.Val)
	default:
		return false
	}
}

func buildSimpleAggregatePlan(env ExecEnv, s *Select) (*simpleAggregatePlan, bool, error) {
	if !simpleAggregateEligibleSelect(s) {
		return nil, false, nil
	}
	if !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	table, err := env.db.Get(env.tenant, s.From.Table)
	if err != nil {
		schema, name := splitObjectName(s.From.Table)
		if mv, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
			table, err = ensureMaterializedViewCache(env, s.From.Table, mv)
			if err != nil {
				return nil, true, err
			}
		} else if isCatalogViewSource(env, s.From.Table) {
			return nil, false, nil
		} else {
			return nil, true, err
		}
	}
	colIndex := simpleColumnIndex(table, aliasOr(s.From))
	groupCols := make([]int, len(s.GroupBy))
	groupPositions := make(map[int]int, len(s.GroupBy))
	for i, groupExpr := range s.GroupBy {
		groupRef, ok := groupExpr.(*VarRef)
		if !ok {
			return nil, false, nil
		}
		groupCol, ok := colIndex[strings.ToLower(groupRef.Name)]
		if !ok {
			return nil, true, unknownColumnErr(groupRef.Name, columnSuggestion(groupRef.Name, colIndex))
		}
		groupCols[i] = groupCol
		if _, exists := groupPositions[groupCol]; !exists {
			groupPositions[groupCol] = i
		}
	}

	projs, outputCols, hasAgg, eligible, err := buildSimpleAggregateProjections(s, colIndex, groupPositions)
	if err != nil {
		return nil, true, err
	}
	if !eligible || !hasAgg {
		return nil, false, nil
	}
	plan := &simpleAggregatePlan{
		table:      table,
		colIndex:   colIndex,
		groupCols:  groupCols,
		where:      s.Where,
		having:     s.Having,
		orderBy:    s.OrderBy,
		limit:      s.Limit,
		offset:     s.Offset,
		projs:      projs,
		outputCols: outputCols,
	}
	if plan.having != nil && !simpleAggregateHavingSupported(plan, plan.having) {
		return nil, false, nil
	}
	return plan, true, nil
}

func simpleAggregateEligibleSelect(s *Select) bool {
	// len(s.GroupBy) > 0 admits a normal GROUP BY query; anyAggInSelect admits
	// the ungrouped whole-table aggregate case ("SELECT COUNT(*) FROM t", no
	// GROUP BY at all). buildSimpleAggregatePlan still requires hasAgg==true
	// regardless of which of these let a query through, so this is purely a
	// cheap pre-filter to skip attempting to build a plan for an ordinary
	// non-aggregate SELECT.
	if s.Distinct || len(s.DistinctOn) > 0 || len(s.CTEs) > 0 || len(s.Joins) > 0 ||
		s.Union != nil ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil ||
		(len(s.GroupBy) == 0 && !anyAggInSelect(s.Projs)) ||
		s.Pivot != nil || isSQLiteSchemaTable(s.From.Table) {
		return false
	}
	return !isCatalogOrSysTableRef(s.From.Table)
}

func buildSimpleAggregateProjections(s *Select, colIndex map[string]int, groupPositions map[int]int) ([]simpleAggregateProjection, []string, bool, bool, error) {
	projs := make([]simpleAggregateProjection, 0, len(s.Projs))
	outputCols := make([]string, 0, len(s.Projs))
	hasAgg := false

	for i, it := range s.Projs {
		proj, name, isAgg, eligible, err := buildSimpleAggregateProjection(it, i, colIndex, groupPositions)
		if err != nil {
			return nil, nil, false, false, err
		}
		if !eligible {
			return nil, nil, false, false, nil
		}
		if isAgg {
			hasAgg = true
		}
		projs = append(projs, proj)
		outputCols = append(outputCols, name)
	}
	return projs, outputCols, hasAgg, true, nil
}

// simpleAggFuncKinds maps the aggregate function names supported by the raw
// GROUP BY fast path (executeSimpleAggregateFastPath) to their aggKind.
// SUM/AVG/MIN/MAX join COUNT here so simple single-table GROUP BY queries
// using any of these no longer fall back to the general row-map evaluator.
var simpleAggFuncKinds = map[string]aggKind{
	"SUM": aggSum,
	"AVG": aggAvg,
	"MIN": aggMin,
	"MAX": aggMax,
}

func buildSimpleAggregateProjection(it SelectItem, idx int, colIndex map[string]int, groupPositions map[int]int) (simpleAggregateProjection, string, bool, bool, error) {
	if it.Star {
		return simpleAggregateProjection{}, "", false, false, nil
	}
	name := projName(it, idx)
	if ref, ok := it.Expr.(*VarRef); ok {
		refCol, ok := colIndex[strings.ToLower(ref.Name)]
		if !ok {
			return simpleAggregateProjection{}, "", false, false, unknownColumnErr(ref.Name, columnSuggestion(ref.Name, colIndex))
		}
		groupIndex, grouped := groupPositions[refCol]
		if !grouped {
			return simpleAggregateProjection{}, "", false, false, nil
		}
		return simpleAggregateProjection{name: name, kind: aggGroupCol, groupIndex: groupIndex}, name, false, true, nil
	}

	fc, ok := it.Expr.(*FuncCall)
	if !ok || fc.Distinct || fc.Over != nil {
		return simpleAggregateProjection{}, "", false, false, nil
	}

	if fc.Name == "COUNT" {
		if fc.Star {
			return simpleAggregateProjection{name: name, kind: aggCount}, name, true, true, nil
		}
		if len(fc.Args) != 1 || !isSimpleRawExpr(fc.Args[0]) {
			return simpleAggregateProjection{}, "", false, false, nil
		}
		return simpleAggregateProjection{name: name, kind: aggCount, arg: fc.Args[0]}, name, true, true, nil
	}

	if kind, ok := simpleAggFuncKinds[fc.Name]; ok {
		if fc.Star || len(fc.Args) != 1 || !isSimpleRawExpr(fc.Args[0]) {
			return simpleAggregateProjection{}, "", false, false, nil
		}
		return simpleAggregateProjection{name: name, kind: kind, arg: fc.Args[0]}, name, true, true, nil
	}

	return simpleAggregateProjection{}, "", false, false, nil
}
