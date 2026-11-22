package engine

import "math"

// aggregateGroups belongs to one execution. Single-column keys avoid text
// formatting, as do common pairs of text and integer values. Other composite
// and unusual values retain the framed encoding used by the general GROUP BY
// evaluator.
// Separate key kinds preserve the distinction between int, int64, float64,
// bool and NULL groups.
type aggregateGroups struct {
	text             map[string]*simpleAggregateState
	textPairs        map[[2]string]*simpleAggregateState
	intPairs         map[[2]int]*simpleAggregateState
	textIntegerPairs [4]map[textIntegerGroupKey]*simpleAggregateState // text first/second, int/int64
	numbers          [3]map[uint64]*simpleAggregateState              // int, int64, float64
	fixed            [3]*simpleAggregateState                         // NULL, false, true
	other            map[string]*simpleAggregateState
	key              []byte
	order            []*simpleAggregateState
	whole            *simpleAggregateState
	arena            aggregateStateArena
}

type textIntegerGroupKey struct {
	text    string
	integer int64
}

func newAggregateGroups(projs []simpleAggregateProjection, groupCount int) aggregateGroups {
	groups := aggregateGroups{arena: aggregateStateArena{projections: len(projs), groupCount: groupCount}}
	sumStart, sumEnd := len(projs), 0
	for i, proj := range projs {
		switch proj.kind {
		case aggSum, aggAvg:
			sumStart = min(sumStart, i)
			sumEnd = i + 1
		case aggMin, aggMax:
			groups.arena.needMinMax = true
		}
	}
	if sumEnd > sumStart {
		// Keep projection-relative indexing while omitting unused slots before
		// the first and after the last numeric accumulator.
		groups.arena.sumStart = sumStart
		groups.arena.sumCount = sumEnd - sumStart
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
	if len(cols) == 2 {
		if state := g.pairState(raw, cols); state != nil {
			return state
		}
	}
	return g.encodedState(raw, cols)
}

// pairState shares direct key lookups across scalar and batched aggregation.
// Unhandled pairs use the caller's framed encoding buffer.
func (g *aggregateGroups) pairState(raw []any, cols []int) *simpleAggregateState {
	leftValue, rightValue := raw[cols[0]], raw[cols[1]]
	left, leftText := leftValue.(string)
	right, rightText := rightValue.(string)
	switch {
	case leftText && rightText:
		return g.textPairState(left, right, raw, cols)
	case leftText:
		return g.textIntegerPairState(left, rightValue, false, raw, cols)
	case rightText:
		return g.textIntegerPairState(right, leftValue, true, raw, cols)
	}
	leftInt, leftInteger := leftValue.(int)
	rightInt, rightInteger := rightValue.(int)
	if leftInteger && rightInteger {
		return g.intPairState(leftInt, rightInt, raw, cols)
	}
	return nil
}

// A mixed dimension key keeps its column order and the canonical distinction
// between int and int64 without formatting the integer and copying the text.
func (g *aggregateGroups) textIntegerPairState(text string, value any, textSecond bool, raw []any, cols []int) *simpleAggregateState {
	key := textIntegerGroupKey{text: text}
	kind := 0
	if textSecond {
		kind = 2
	}
	switch number := value.(type) {
	case int:
		key.integer = int64(number)
	case int64:
		key.integer = number
		kind++
	default:
		return nil
	}
	pairs := g.textIntegerPairs[kind]
	if state := pairs[key]; state != nil {
		return state
	}
	if pairs == nil {
		pairs = make(map[textIntegerGroupKey]*simpleAggregateState)
		g.textIntegerPairs[kind] = pairs
	}
	state := g.newState(raw, cols)
	pairs[key] = state
	return state
}

// Integer dimension pairs can be hashed without formatting a composite key.
// Restrict this map to int values: int64, float64 and NULL belong to separate
// groups under the canonical encoding, even when numerically equal to an int.
func (g *aggregateGroups) intPairState(left, right int, raw []any, cols []int) *simpleAggregateState {
	key := [2]int{left, right}
	if state := g.intPairs[key]; state != nil {
		return state
	}
	if g.intPairs == nil {
		g.intPairs = make(map[[2]int]*simpleAggregateState)
	}
	state := g.newState(raw, cols)
	g.intPairs[key] = state
	return state
}

func (g *aggregateGroups) textPairState(left, right string, raw []any, cols []int) *simpleAggregateState {
	key := [2]string{left, right}
	if state := g.textPairs[key]; state != nil {
		return state
	}
	if g.textPairs == nil {
		g.textPairs = make(map[[2]string]*simpleAggregateState)
	}
	state := g.newState(raw, cols)
	g.textPairs[key] = state
	return state
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
	sumStart, sumCount      int
	needMinMax              bool
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
		if a.sumCount > 0 {
			sums = make([]sumAccumulator, a.blockSize*a.sumCount)
		}
		if a.needMinMax {
			minmax = make([]any, a.blockSize*a.projections)
		}
		for i := range a.remaining {
			state := &a.remaining[i]
			start, end := i*a.projections, (i+1)*a.projections
			state.counts = counts[start:end:end]
			if a.sumCount > 0 {
				first, last := i*a.sumCount, (i+1)*a.sumCount
				state.sums = sums[first:last:last]
				state.sumStart = a.sumStart
			}
			if a.needMinMax {
				state.minmax = minmax[start:end:end]
			}
			start, end = i*a.groupCount, (i+1)*a.groupCount
			state.groupValues = values[start:end:end]
		}
	}
	state := &a.remaining[0]
	a.remaining = a.remaining[1:]
	return state
}
