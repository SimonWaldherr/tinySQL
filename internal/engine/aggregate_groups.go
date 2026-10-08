package engine

import "math"

// aggregateGroups belongs to one execution. Single-column keys avoid text
// formatting, while composite and unusual values retain the framed encoding
// used by the general GROUP BY evaluator. Separate key kinds preserve the
// engine's distinction between int, int64, float64, bool and NULL groups.
type aggregateGroups struct {
	text    map[string]*simpleAggregateState
	numbers [3]map[uint64]*simpleAggregateState // int, int64, float64
	fixed   [3]*simpleAggregateState            // NULL, false, true
	other   map[string]*simpleAggregateState
	key     []byte
	order   []*simpleAggregateState
	whole   *simpleAggregateState
	arena   aggregateStateArena
}

func newAggregateGroups(projs []simpleAggregateProjection, groupCount int) aggregateGroups {
	groups := aggregateGroups{arena: aggregateStateArena{projections: len(projs), groupCount: groupCount}}
	for _, proj := range projs {
		switch proj.kind {
		case aggSum, aggAvg:
			groups.arena.needSums = true
		case aggMin, aggMax:
			groups.arena.needMinMax = true
		}
	}
	return groups
}

func (g *aggregateGroups) state(raw []any, cols []int) *simpleAggregateState {
	if len(cols) == 0 {
		if g.whole == nil {
			g.whole = g.newState(raw, cols)
		}
		return g.whole
	}
	if len(cols) == 1 {
		value := raw[cols[0]]
		if text, ok := value.(string); ok {
			if state := g.text[text]; state != nil {
				return state
			}
			if g.text == nil {
				g.text = make(map[string]*simpleAggregateState)
			}
			state := g.newState(raw, cols)
			g.text[text] = state
			return state
		}
		var kind int
		var key uint64
		switch value := value.(type) {
		case nil:
			return g.fixedState(0, raw, cols)
		case int:
			key = uint64(value)
		case int64:
			kind, key = 1, uint64(value)
		case float64:
			kind, key = 2, math.Float64bits(value)
			// AppendFloat groups every NaN spelling together but distinguishes
			// negative zero from positive zero. Match both behaviors exactly.
			if math.IsNaN(value) {
				key = 0x7ff8000000000000
			}
		case bool:
			if value {
				return g.fixedState(2, raw, cols)
			}
			return g.fixedState(1, raw, cols)
		default:
			return g.encodedState(raw, cols)
		}
		if state := g.numbers[kind][key]; state != nil {
			return state
		}
		if g.numbers[kind] == nil {
			g.numbers[kind] = make(map[uint64]*simpleAggregateState)
		}
		state := g.newState(raw, cols)
		g.numbers[kind][key] = state
		return state
	}
	return g.encodedState(raw, cols)
}

func (g *aggregateGroups) fixedState(i int, raw []any, cols []int) *simpleAggregateState {
	if g.fixed[i] == nil {
		g.fixed[i] = g.newState(raw, cols)
	}
	return g.fixed[i]
}

func (g *aggregateGroups) encodedState(raw []any, cols []int) *simpleAggregateState {
	key := g.key[:0]
	for i, col := range cols {
		if i != 0 {
			key = append(key, '\x1f')
		}
		key = writeFmtKeyPart(key, raw[col])
	}
	g.key = key
	if state := g.other[string(key)]; state != nil {
		return state
	}
	return g.newEncodedState(key, raw, cols)
}

func (g *aggregateGroups) newEncodedState(key []byte, raw []any, cols []int) *simpleAggregateState {
	if g.other == nil {
		g.other = make(map[string]*simpleAggregateState)
	}
	state := g.newState(raw, cols)
	g.other[string(key)] = state
	return state
}

func (g *aggregateGroups) newState(raw []any, cols []int) *simpleAggregateState {
	state := g.arena.next()
	for i, col := range cols {
		state.groupValues[i] = raw[col]
	}
	g.order = append(g.order, state)
	return state
}

// Small, growing blocks remove per-group slice allocations without reserving
// space for every input row. States and their capacity-limited slices never
// move or overlap; later blocks cannot overwrite earlier groups or results.
// Decimal and DISTINCT storage remains lazy on each state as before.
type aggregateStateArena struct {
	projections, groupCount int
	needSums, needMinMax    bool
	remaining               []simpleAggregateState
	blockSize               int
}

func (a *aggregateStateArena) next() *simpleAggregateState {
	if len(a.remaining) == 0 {
		a.blockSize = min(64, max(1, a.blockSize*2))
		a.remaining = make([]simpleAggregateState, a.blockSize)
		counts := make([]int, a.blockSize*a.projections)
		values := make([]any, a.blockSize*a.groupCount)
		var sums []sumAccumulator
		var minmax []any
		var haveMinMax []bool
		if a.needSums {
			sums = make([]sumAccumulator, a.blockSize*a.projections)
		}
		if a.needMinMax {
			minmax = make([]any, a.blockSize*a.projections)
			haveMinMax = make([]bool, a.blockSize*a.projections)
		}
		for i := range a.remaining {
			state := &a.remaining[i]
			start, end := i*a.projections, (i+1)*a.projections
			state.counts = counts[start:end:end]
			if a.needSums {
				state.sums = sums[start:end:end]
			}
			if a.needMinMax {
				state.minmax = minmax[start:end:end]
				state.haveMinMax = haveMinMax[start:end:end]
			}
			start, end = i*a.groupCount, (i+1)*a.groupCount
			state.groupValues = values[start:end:end]
		}
	}
	state := &a.remaining[0]
	a.remaining = a.remaining[1:]
	return state
}
