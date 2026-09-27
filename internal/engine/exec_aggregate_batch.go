package engine

import "fmt"

// Batches are query-local scratch, never a second copy of a table. Keeping the
// source as row IDs lets filtering precede column extraction and bounds memory
// by batch size and the number of referenced aggregate columns.
const aggregateBatchSize = 256
const aggregateBatchMinRows = 2048

type aggregateBatchColumn struct {
	column  int
	numeric bool
	values  [aggregateBatchSize]float64 // REAL cells
	ints    [aggregateBatchSize]int64   // integer cells, marked in isInt
	valid   [aggregateBatchSize / 64]uint64
	isInt   [aggregateBatchSize / 64]uint64
	// hasInt/hasFloat describe the current batch's non-NULL cells, so the
	// common homogeneous batches take a loop without per-cell type checks.
	hasInt, hasFloat bool
}

type aggregateBatchOp struct {
	kind       aggKind
	projection int
	column     int // -1 for COUNT(*)
	copyFrom   int // canonical projection + 1; zero means accumulate this op
}

// aggregateBatchEligible admits only pure, direct-column COUNT/SUM/AVG operations.
// MIN/MAX preserve the original input type; decimals require exact rational
// promotion. Those semantics remain in the scalar implementation. Runtime
// nonnumeric values also switch to scalar accumulation, without rereading or
// reevaluating the predicate of a batch.
func aggregateBatchEligible(plan *simpleAggregatePlan, source *simpleSelectPlan) bool {
	n := len(plan.table.Rows)
	if source.rowIDs != nil {
		n = len(source.rowIDs)
	}
	if n < aggregateBatchMinRows {
		return false
	}
	// Validate the whole shape before allocating any batch scratch.
	hasNumeric := false
	for _, proj := range plan.projs {
		switch proj.kind {
		case aggGroupCol:
			continue
		case aggCount:
			if proj.arg != nil && proj.argColumn == 0 {
				return false
			}
		case aggSum, aggAvg:
			if proj.argColumn == 0 {
				return false
			}
			hasNumeric = true
		default:
			return false
		}
	}
	return hasNumeric
}

func buildAggregateBatch(plan *simpleAggregatePlan, source *simpleSelectPlan) ([]aggregateBatchColumn, []aggregateBatchOp, bool) {
	if !aggregateBatchEligible(plan, source) {
		return nil, nil, false
	}
	// Resolve/deduplicate column numbers first so append never copies populated
	// vectors. SUM(x), AVG(x), COUNT(x) share one extraction and validity bitmap.
	columnIDs := make([]int, 0, len(plan.projs))
	ops := make([]aggregateBatchOp, 0, len(plan.projs))
	for i, proj := range plan.projs {
		if proj.kind == aggGroupCol {
			continue
		}
		col := -1
		if proj.argColumn > 0 {
			for j, id := range columnIDs {
				if id == proj.argColumn-1 {
					col = j
					break
				}
			}
			if col < 0 {
				col = len(columnIDs)
				columnIDs = append(columnIDs, proj.argColumn-1)
			}
		}
		ops = append(ops, aggregateBatchOp{kind: proj.kind, projection: i, column: col})
	}
	cols := make([]aggregateBatchColumn, len(columnIDs))
	for i, id := range columnIDs {
		cols[i].column = id
	}
	for _, op := range ops {
		if op.kind == aggSum || op.kind == aggAvg {
			cols[op.column].numeric = true
		}
	}
	// Numeric SUM/AVG/COUNT of the same column have identical sample counts,
	// and SUM/AVG have identical running sums. Accumulate one representative
	// while batches remain numeric. COUNT(*) duplicates can share too.
	for i, op := range ops {
		for _, candidate := range ops {
			if candidate.column != op.column {
				continue
			}
			if op.column >= 0 && cols[op.column].numeric && candidate.kind == aggCount {
				continue // a count cannot supply the sum needed by SUM/AVG
			}
			if candidate.projection != op.projection {
				ops[i].copyFrom = candidate.projection + 1
			}
			break
		}
	}
	return cols, ops, true
}

func executeAggregateBatches(env ExecEnv, plan *simpleAggregatePlan, source *simpleSelectPlan, cols []aggregateBatchColumn, ops []aggregateBatchOp) (*ResultSet, bool, error) {
	rows := plan.table.Rows
	n := len(rows)
	if source.rowIDs != nil {
		n = len(source.rowIDs)
	}
	var selected [aggregateBatchSize]int
	var states [aggregateBatchSize]*simpleAggregateState
	groups := make(map[string]*simpleAggregateState)
	order := make([]*simpleAggregateState, 0)
	key := make([]byte, 0, 64)
	var whole *simpleAggregateState
	if len(plan.groupCols) == 0 {
		whole = newSimpleAggregateState(nil, len(plan.projs))
		order = append(order, whole)
	}
	// Once a runtime value needs scalar handling, retain that path for the
	// rest of the query: a state may now contain an exact rational accumulator.
	scalar := false
	for start := 0; start < n; start += aggregateBatchSize {
		count := 0
		for i := start; i < min(start+aggregateBatchSize, n); i++ {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, true, err
				}
			}
			id := i
			if source.rowIDs != nil {
				id = source.rowIDs[i]
			}
			if id < 0 || id >= len(rows) {
				return nil, true, fmt.Errorf("index %q returned invalid row id %d", source.indexName, id)
			}
			match, err := evalRawWhere(source, rows[id])
			if err != nil {
				return nil, true, err
			}
			if match {
				selected[count] = id
				count++
			}
		}
		if count == 0 {
			continue
		}
		if !scalar {
			scalar = !extractAggregateColumns(rows, selected[:count], cols)
			if scalar {
				// Materialize shared states before the first scalar row. COUNT(x)
				// and SUM(x) can diverge on nonnumeric values, and decimal promotion
				// must see each aggregate's complete preceding numeric history.
				copyAggregateBatchStates(order, ops)
			}
		}
		if whole == nil {
			for i, id := range selected[:count] {
				raw := rows[id]
				key = key[:0]
				if len(plan.groupCols) == 1 {
					key = writeSingleGroupKey(key, raw[plan.groupCols[0]])
				} else {
					for j, col := range plan.groupCols {
						if j > 0 {
							key = append(key, '\x1f')
						}
						key = writeFmtKeyPart(key, raw[col])
					}
				}
				state := groups[string(key)]
				if state == nil {
					values := make([]any, len(plan.groupCols))
					for j, col := range plan.groupCols {
						values[j] = raw[col]
					}
					state = newSimpleAggregateState(values, len(plan.projs))
					groups[string(key)] = state
					order = append(order, state)
				}
				states[i] = state
			}
		}
		if scalar {
			for i, id := range selected[:count] {
				state := whole
				if state == nil {
					state = states[i]
				}
				if err := accumulateSimpleAggregateState(env, source, rows[id], state, plan.projs); err != nil {
					return nil, true, err
				}
			}
			continue
		}
		for _, op := range ops {
			if op.copyFrom != 0 {
				continue
			}
			if whole != nil {
				accumulateWholeBatch(whole, count, cols, op)
			} else {
				accumulateGroupedBatch(states[:count], cols, op)
			}
		}
	}
	if !scalar {
		copyAggregateBatchStates(order, ops)
	}
	rs, err := finalizeSimpleAggregateResultSet(env, plan, order)
	return rs, true, err
}

func copyAggregateBatchStates(states []*simpleAggregateState, ops []aggregateBatchOp) {
	for _, op := range ops {
		if op.copyFrom == 0 {
			continue
		}
		for _, state := range states {
			state.counts[op.projection] = state.counts[op.copyFrom-1]
			if op.kind != aggCount {
				state.sums[op.projection] = state.sums[op.copyFrom-1]
			}
		}
	}
}

func extractAggregateColumns(rows [][]any, selected []int, cols []aggregateBatchColumn) bool {
	for c := range cols {
		col := &cols[c]
		clear(col.valid[:])
		clear(col.isInt[:])
		col.hasInt, col.hasFloat = false, false
		for i, id := range selected {
			if col.column >= len(rows[id]) {
				return false // scalar evaluation reports the usual column error
			}
			v := rows[id][col.column]
			if v == nil {
				continue
			}
			bit := uint64(1) << uint(i%64)
			col.valid[i/64] |= bit
			if !col.numeric {
				continue
			}
			switch x := v.(type) {
			case int:
				col.ints[i] = int64(x)
				col.isInt[i/64] |= bit
				col.hasInt = true
			case int64:
				col.ints[i] = x
				col.isInt[i/64] |= bit
				col.hasInt = true
			case float64:
				col.values[i] = x
				col.hasFloat = true
			default:
				return false
			}
		}
	}
	return true
}

func accumulateWholeBatch(state *simpleAggregateState, n int, cols []aggregateBatchColumn, op aggregateBatchOp) {
	p := op.projection
	if op.column < 0 {
		state.counts[p] += n
		return
	}
	col := &cols[op.column]
	count, acc := state.counts[p], &state.sums[p]
	switch {
	case op.kind == aggCount || (!col.hasInt && !col.hasFloat):
		for i := 0; i < n; i++ {
			if col.valid[i/64]&(uint64(1)<<uint(i%64)) != 0 {
				count++
			}
		}
	case !col.hasInt:
		// Keep the original addition order, including across batch boundaries.
		// Partial batch sums would change floating-point rounding.
		acc.addFloat(0) // switch to REAL; adding zero leaves the sum unchanged
		sum := acc.floatSum
		for i := 0; i < n; i++ {
			if col.valid[i/64]&(uint64(1)<<uint(i%64)) == 0 {
				continue
			}
			count++
			sum += col.values[i]
		}
		acc.floatSum = sum
	default:
		for i := 0; i < n; i++ {
			bit := uint64(1) << uint(i%64)
			if col.valid[i/64]&bit == 0 {
				continue
			}
			count++
			if col.isInt[i/64]&bit != 0 {
				acc.addInt(col.ints[i])
			} else {
				acc.addFloat(col.values[i])
			}
		}
	}
	state.counts[p] = count
}

func accumulateGroupedBatch(states []*simpleAggregateState, cols []aggregateBatchColumn, op aggregateBatchOp) {
	p := op.projection
	if op.column < 0 {
		for _, state := range states {
			state.counts[p]++
		}
		return
	}
	col := &cols[op.column]
	for i, state := range states {
		bit := uint64(1) << uint(i%64)
		if col.valid[i/64]&bit == 0 {
			continue
		}
		state.counts[p]++
		if op.kind == aggCount {
			continue
		}
		if col.isInt[i/64]&bit != 0 {
			state.sums[p].addInt(col.ints[i])
		} else {
			state.sums[p].addFloat(col.values[i])
		}
	}
}
