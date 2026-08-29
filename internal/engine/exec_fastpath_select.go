// Fast path for a single-table SELECT: an unfiltered scan, a filtered scan, and
// an ordered scan that degenerates to a bounded top-N heap when LIMIT is
// present. The float-keyed variant keeps the sort key unboxed.
package engine

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeSimpleSelectFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleSelectPlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}
	if s.Distinct {
		// DISTINCT combined with ORDER BY keeps the general path: the ordered
		// fast path sorts raw source rows and projects afterwards, whereas
		// DISTINCT has to collapse duplicates on the *projected* values first,
		// and the two cannot simply be composed in that order.
		//
		// A select list that maps two projections onto one output name is
		// likewise handed back (see distinctProjectionsSafe).
		if len(plan.orderBy) > 0 || !distinctProjectionsSafe(plan.projs) {
			return nil, false, nil
		}
		return executeSimpleSelectDistinctFastPath(env, plan)
	}
	if len(plan.orderBy) > 0 {
		return executeSimpleSelectOrderedFastPath(env, plan)
	}
	if plan.where == nil && plan.rowIDs == nil {
		return executeSimpleSelectUnfilteredFastPath(env, plan)
	}

	outRows := make([]Row, 0, simpleSelectInitialCap(plan))
	stopAfter := -1
	if plan.limit != nil {
		stopAfter = *plan.limit
		if plan.offset != nil {
			stopAfter += *plan.offset
		}
	}

	rows := simplePlanRows(plan)
	rowCount := len(rows)
	if plan.rowIDs != nil {
		rowCount = len(plan.rowIDs)
	}
	for i := 0; i < rowCount; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(rows) {
			return nil, true, fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
		}
		raw := rows[rowID]
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match := plan.filterFullyCovered
		if !match {
			var err error
			match, err = evalRawWhere(plan, raw)
			if err != nil {
				return nil, true, err
			}
		}
		if !match {
			continue
		}
		out, err := projectRawRow(plan, raw)
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
		if stopAfter >= 0 && len(outRows) >= stopAfter {
			break
		}
	}

	outRows = applyOffsetLimit(&Select{Limit: plan.limit, Offset: plan.offset}, outRows)
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

// executeSimpleSelectUnfilteredFastPath applies LIMIT/OFFSET before row
// projection for an unfiltered table scan. SQL applies OFFSET after filtering,
// so this shortcut is deliberately restricted to a scan with no WHERE clause
// and no index RowID set. In the common pagination shape this avoids building
// and then discarding one Row map for every skipped row.
// executeSimpleSelectDistinctFastPath runs SELECT DISTINCT without an ORDER BY
// directly over raw rows.
//
// DISTINCT previously disqualified the raw fast path outright
// (simpleSelectEligible), so the general path materialized a Row map for every
// *source* row and only then collapsed duplicates in distinctRows. For the
// shape DISTINCT is normally written for — few distinct values over many rows,
// e.g. SELECT DISTINCT status FROM events — that is one map allocation per
// scanned row to produce a handful of output rows. Hashing the projected values
// first means a map is built only for a row that actually survives, so the
// allocation count follows the size of the *result* rather than the table.
//
// The dedup key is byte-identical to distinctRows' (see appendDistinctKey), and
// first-occurrence-wins ordering is preserved, so the returned rows match the
// general path exactly.
//
// LIMIT/OFFSET are applied here against distinct rows, because a fast path
// returns straight to executeSelect's caller and the general tail never runs.
// Counting distinct rows (not scanned rows) is what makes that correct, and it
// also lets a LIMIT stop the scan as soon as enough distinct rows exist.
func executeSimpleSelectDistinctFastPath(env ExecEnv, plan *simpleSelectPlan) (*ResultSet, bool, error) {
	if plan.limit != nil && *plan.limit == 0 {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}

	rows := simplePlanRows(plan)
	rowCount := len(rows)
	if plan.rowIDs != nil {
		rowCount = len(plan.rowIDs)
	}

	offset := 0
	if plan.offset != nil && *plan.offset > 0 {
		offset = *plan.offset
	}
	stopAfter := -1
	if plan.limit != nil {
		stopAfter = offset + *plan.limit
	}

	seen := make(map[string]struct{})
	vals := make([]any, len(plan.projs))
	// key is reused across rows; seen[string(key)] is a zero-allocation lookup,
	// so a string is only materialized for a genuinely new row — the same
	// trick distinctRows uses.
	key := make([]byte, 0, 64)
	var outRows []Row
	distinctCount := 0

	for i := 0; i < rowCount; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(rows) {
			return nil, true, fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
		}
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		raw := rows[rowID]

		match := plan.filterFullyCovered
		if !match {
			var err error
			match, err = evalRawWhere(plan, raw)
			if err != nil {
				return nil, true, err
			}
		}
		if !match {
			continue
		}

		if err := projectRawValues(plan, raw, vals); err != nil {
			return nil, true, err
		}
		key = appendDistinctKey(key[:0], vals)
		if _, dup := seen[string(key)]; dup {
			continue
		}
		seen[string(key)] = struct{}{}

		distinctCount++
		if distinctCount <= offset {
			continue
		}
		outRows = append(outRows, rowFromProjectedValues(plan, vals))
		if stopAfter >= 0 && distinctCount >= stopAfter {
			break
		}
	}

	if outRows == nil {
		outRows = []Row{}
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

func executeSimpleSelectUnfilteredFastPath(env ExecEnv, plan *simpleSelectPlan) (*ResultSet, bool, error) {
	rows := simplePlanRows(plan)
	// The generic scan checks the context on its first source row, even when
	// LIMIT/OFFSET would ultimately discard all rows. Preserve that
	// cancellation behavior before returning an empty page.
	if len(rows) > 0 {
		if err := checkCtx(env.ctx); err != nil {
			return nil, true, err
		}
	}
	if plan.limit != nil && *plan.limit == 0 {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}

	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start >= len(rows) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	end := len(rows)
	if plan.limit != nil && *plan.limit < end-start {
		end = start + *plan.limit
	}

	outRows := make([]Row, 0, end-start)
	for i, raw := range rows[start:end] {
		// The first source row was checked above. Continue at the same 64-row
		// cadence as the generic scan without checking it twice.
		if i > 0 && i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		out, err := projectRawRow(plan, raw)
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

type orderedRawRow struct {
	raw  []any
	key  any
	keys []any
}

// floatOrderedRawRow is the narrow top-N representation for ORDER BY on a
// physical FLOAT/FLOAT64 column. Keeping the sort key unboxed avoids the
// generic compare/type-switch path for every heap comparison. It is used only
// after simpleFloatOrderColumn has verified every source value is float64;
// all nullable, mixed-type, indexed, or filtered queries retain the general
// orderedRawRow implementation below.
type floatOrderedRawRow struct {
	raw []any
	key float64
}

// simpleFloatOrderColumn recognizes the common pagination shape
//
//	SELECT ... FROM t ORDER BY float_column [ASC|DESC] LIMIT n [OFFSET m]
//
// without a WHERE clause or index RowID set. The schema test avoids an extra
// table scan for non-float ORDER BY queries; the caller still checks the raw
// values, because direct storage callers may bypass SQL coercion.
func simpleFloatOrderColumn(plan *simpleSelectPlan) (int, bool) {
	if plan == nil || plan.table == nil || plan.where != nil || plan.rowIDs != nil || plan.limit == nil ||
		len(plan.orderBy) != 1 || len(plan.orderExprs) != 1 {
		return 0, false
	}
	ref, ok := plan.orderExprs[0].(*VarRef)
	if !ok {
		return 0, false
	}
	idx, ok := plan.colIndex[strings.ToLower(ref.Name)]
	if !ok || idx < 0 || idx >= len(plan.table.Cols) {
		return 0, false
	}
	switch plan.table.Cols[idx].Type {
	case storage.Float64Type, storage.FloatType:
		return idx, true
	default:
		return 0, false
	}
}

// executeSimpleSelectFloatOrderedTopN performs the verified float-only
// variant of the ordered raw fast path. Its ordering intentionally uses the
// same < and > comparisons as compareFloat, including treating NaN as equal
// to every other value, so it preserves the generic comparator's behavior.
func executeSimpleSelectFloatOrderedTopN(env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, column, keepCount int) (*ResultSet, error) {
	topRows := floatOrderedRawRowHeap{
		desc:  plan.orderBy[0].Desc,
		items: make([]floatOrderedRawRow, 0, simpleSelectInitialCap(plan)),
	}
	for i, raw := range sourceRows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
		}
		topRows.pushBounded(floatOrderedRawRow{raw: raw, key: raw[column].(float64)}, keepCount)
	}

	rows := topRows.items
	sort.Stable(floatOrderedRawRowsAsc{desc: topRows.desc, items: rows})
	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(rows) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, nil
	}
	rows = rows[start:]
	if *plan.limit < len(rows) {
		rows = rows[:*plan.limit]
	}

	outRows := make([]Row, 0, len(rows))
	for _, item := range rows {
		out, err := projectRawRow(plan, item.raw)
		if err != nil {
			return nil, err
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, nil
}

type rawFloatScorer func([]any) (float64, error)

// simpleFloatOrderScorer compiles numeric full-text/vector scoring expressions
// used by ORDER BY ... LIMIT into an unboxed float64 evaluator. The generic
// expression path returns `any`, which makes every candidate score escape to
// the heap even though the bounded top-N heap only needs a number.
func simpleFloatOrderScorer(plan *simpleSelectPlan) (rawFloatScorer, bool) {
	if plan == nil || plan.limit == nil || len(plan.orderExprs) != 1 {
		return nil, false
	}
	call, ok := plan.orderExprs[0].(*FuncCall)
	if !ok || len(call.Args) < 2 {
		return nil, false
	}
	ref, ok := call.Args[0].(*VarRef)
	if !ok {
		return nil, false
	}
	col, ok := plan.colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil, false
	}

	switch call.Name {
	case "FTS_RANK", "BM25":
		query, ok := call.Args[1].(*Literal)
		if !ok {
			return nil, false
		}
		queryText, ok := query.Val.(string)
		if !ok {
			return nil, false
		}
		node := parseCachedFTSQuery(queryText)
		terms, supported := ftsLiteralORTerms(node)
		if !supported {
			return nil, false
		}
		counts := make([]int, len(terms))
		return func(raw []any) (float64, error) {
			if col >= len(raw) || raw[col] == nil {
				return 0, nil
			}
			return ftsLiteralTermsRank(ftsValueToString(raw[col]), terms, counts), nil
		}, true

	case "VEC_DOT", "VEC_COSINE_SIMILARITY", "VEC_COSINE_DISTANCE",
		"VEC_L2_DISTANCE", "VEC_MANHATTAN_DISTANCE", "VEC_HAMMING_DISTANCE":
		query, ok := call.Args[1].(*Literal)
		if !ok {
			return nil, false
		}
		queryVec, ok := query.Val.([]float64)
		if !ok {
			return nil, false
		}
		name := call.Name
		var queryPositive []bool
		if name == "VEC_HAMMING_DISTANCE" {
			queryPositive = make([]bool, len(queryVec))
			for i, value := range queryVec {
				queryPositive[i] = value > 0
			}
		}
		return func(raw []any) (float64, error) {
			if col >= len(raw) {
				return 0, fmt.Errorf("column %q is out of range", ref.Name)
			}
			vec, err := vecFromValue(raw[col])
			if err != nil {
				return 0, fmt.Errorf("%s arg1: %w", name, err)
			}
			if len(vec) != len(queryVec) {
				return 0, fmt.Errorf("%s: dimension mismatch %d vs %d", name, len(vec), len(queryVec))
			}
			switch name {
			case "VEC_DOT":
				return search.VectorDot(vec, queryVec), nil
			case "VEC_COSINE_SIMILARITY", "VEC_COSINE_DISTANCE":
				sim, err := cosineSimilarity(vec, queryVec)
				if err != nil {
					return 0, fmt.Errorf("%s: %w", name, err)
				}
				if name == "VEC_COSINE_DISTANCE" {
					return 1 - sim, nil
				}
				return sim, nil
			case "VEC_L2_DISTANCE":
				return math.Sqrt(search.VectorL2Squared(vec, queryVec)), nil
			case "VEC_MANHATTAN_DISTANCE":
				return search.VectorL1Distance(vec, queryVec), nil
			default:
				count := 0
				for i, positive := range queryPositive {
					if (vec[i] > 0) != positive {
						count++
					}
				}
				return float64(count), nil
			}
		}, true
	}
	return nil, false
}

func executeSimpleSelectFloatScoredTopN(env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, rowCount, keepCount int, score rawFloatScorer) (*ResultSet, error) {
	topRows := floatOrderedRawRowHeap{
		desc:  plan.orderBy[0].Desc,
		items: make([]floatOrderedRawRow, 0, simpleSelectInitialCap(plan)),
	}
	for i := 0; i < rowCount; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(sourceRows) {
			return nil, fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
		}
		raw := sourceRows[rowID]
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
		}
		match, err := evalRawWhere(plan, raw)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		key, err := score(raw)
		if err != nil {
			return nil, err
		}
		topRows.pushBounded(floatOrderedRawRow{raw: raw, key: key}, keepCount)
	}

	rows := topRows.items
	sort.Stable(floatOrderedRawRowsAsc{desc: topRows.desc, items: rows})
	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(rows) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, nil
	}
	rows = rows[start:]
	if *plan.limit < len(rows) {
		rows = rows[:*plan.limit]
	}
	outRows := make([]Row, 0, len(rows))
	for _, item := range rows {
		out, err := projectRawRow(plan, item.raw)
		if err != nil {
			return nil, err
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, nil
}

type floatOrderedRawRowHeap struct {
	desc  bool
	items []floatOrderedRawRow
}

func (h floatOrderedRawRowHeap) less(i, j int) bool {
	// The heap root is the worst retained row, hence the reversed comparison.
	// Direct float comparisons — semantically identical to the generic
	// comparator (NaN ranks equal to everything, ties are not less) but small
	// enough to inline into every heap level of every pushed row.
	a, b := h.items[i].key, h.items[j].key
	if a < b {
		return h.desc
	}
	if a > b {
		return !h.desc
	}
	return false
}

func (h floatOrderedRawRowHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *floatOrderedRawRowHeap) pushBounded(item floatOrderedRawRow, keepCount int) {
	if len(h.items) < keepCount {
		h.items = append(h.items, item)
		for j := len(h.items) - 1; j > 0; {
			i := (j - 1) / 2
			if !h.less(j, i) {
				break
			}
			h.swap(i, j)
			j = i
		}
		return
	}
	// Insert only when item is strictly better than the current worst (the
	// root); ties — including NaN, which compares equal to everything — keep
	// the incumbent. Direct comparisons for the same inlining reason as less.
	if h.desc {
		if !(item.key > h.items[0].key) {
			return
		}
	} else {
		if !(item.key < h.items[0].key) {
			return
		}
	}
	h.items[0] = item
	for i, n := 0, len(h.items); ; {
		left := 2*i + 1
		if left >= n {
			return
		}
		child := left
		if right := left + 1; right < n && h.less(right, left) {
			child = right
		}
		if !h.less(child, i) {
			return
		}
		h.swap(i, child)
		i = child
	}
}

// floatOrderedRawRowsAsc adapts a []floatOrderedRawRow slice to sort.Interface
// with a concrete Swap, avoiding the reflect.Swapper-based Swap that
// sort.Slice/SliceStable builds for element types it doesn't special-case
// (see orderedRawRowsAsc below for the same fix on the more expensive
// generic path).
type floatOrderedRawRowsAsc struct {
	desc  bool
	items []floatOrderedRawRow
}

func (s floatOrderedRawRowsAsc) Len() int { return len(s.items) }
func (s floatOrderedRawRowsAsc) Less(i, j int) bool {
	// Direct float comparisons; NaN ranks equal to everything and ties are
	// not less, so sort.Stable keeps their existing order.
	a, b := s.items[i].key, s.items[j].key
	if a < b {
		return !s.desc
	}
	if a > b {
		return s.desc
	}
	return false
}
func (s floatOrderedRawRowsAsc) Swap(i, j int) { s.items[i], s.items[j] = s.items[j], s.items[i] }

func executeSimpleSelectOrderedFastPath(env ExecEnv, plan *simpleSelectPlan) (*ResultSet, bool, error) {
	if plan.limit != nil && *plan.limit == 0 {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	sourceRows := simplePlanRows(plan)
	rowCount := len(sourceRows)
	if plan.rowIDs != nil {
		rowCount = len(plan.rowIDs)
	}

	keepCount := -1
	if plan.limit != nil {
		keepCount = *plan.limit
		if plan.offset != nil {
			keepCount += *plan.offset
		}
		if keepCount > rowCount {
			keepCount = rowCount
		}
	}
	if scorer, ok := simpleFloatOrderScorer(plan); ok && keepCount > 0 {
		rs, err := executeSimpleSelectFloatScoredTopN(env, plan, sourceRows, rowCount, keepCount, scorer)
		return rs, true, err
	}
	if column, ok := simpleFloatOrderColumn(plan); ok {
		// Verify storage values before entering the unboxed path. SQL INSERT
		// coercion makes this true for normal FLOAT/FLOAT64 tables; direct
		// storage callers can still provide NULL or a different Go type, which
		// must follow generic SQL ordering instead.
		allFloat64 := true
		for i, raw := range sourceRows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, true, err
				}
			}
			if column >= len(raw) {
				allFloat64 = false
				break
			}
			if _, ok := raw[column].(float64); !ok {
				allFloat64 = false
				break
			}
		}
		if allFloat64 {
			rs, err := executeSimpleSelectFloatOrderedTopN(env, plan, sourceRows, column, keepCount)
			return rs, true, err
		}
	}

	rows := make([]orderedRawRow, 0, simpleSelectInitialCap(plan))
	// keyArena backs the per-row keys slices of multi-column orders in a few
	// large chunks. Freshly packed keys sort measurably faster than reads
	// scattered across per-row allocations or the raw rows themselves, and
	// the chunks cost a handful of allocations instead of one per row.
	var keyArena []any
	var topRows orderedRawRowHeap
	useTopN := keepCount > 0
	if useTopN {
		topRows = orderedRawRowHeap{
			plan:  plan,
			items: make([]orderedRawRow, 0, simpleSelectInitialCap(plan)),
		}
	}
	for i := 0; i < rowCount; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(sourceRows) {
			return nil, true, fmt.Errorf("index %q returned invalid row id %d", plan.indexName, rowID)
		}
		raw := sourceRows[rowID]
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(plan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}
		var item orderedRawRow
		switch {
		case plan.orderCols != nil:
			// Every ORDER BY term is a direct column reference: read sort
			// keys straight out of the raw row instead of the per-term
			// evalRawExpr map lookups.
			for k, col := range plan.orderCols {
				if col >= len(raw) {
					// Same error evalRawExpr raises for a ragged raw row;
					// orderCols non-nil guarantees every term is a *VarRef.
					return nil, true, fmt.Errorf("column %q is out of range", plan.orderExprs[k].(*VarRef).Name)
				}
			}
			if len(plan.orderCols) == 1 {
				item = orderedRawRow{raw: raw, key: raw[plan.orderCols[0]]}
			} else {
				var keys []any
				keyArena, keys = reserveKeySlots(keyArena, len(plan.orderCols))
				for i, col := range plan.orderCols {
					keys[i] = raw[col]
				}
				item = orderedRawRow{raw: raw, keys: keys}
			}
		case len(plan.orderExprs) == 1:
			key, err := evalRawExpr(plan, raw, plan.orderExprs[0])
			if err != nil {
				return nil, true, err
			}
			item = orderedRawRow{raw: raw, key: key}
		default:
			var keys []any
			keyArena, keys = reserveKeySlots(keyArena, len(plan.orderExprs))
			for i, expr := range plan.orderExprs {
				v, err := evalRawExpr(plan, raw, expr)
				if err != nil {
					return nil, true, err
				}
				keys[i] = v
			}
			item = orderedRawRow{raw: raw, keys: keys}
		}
		if useTopN {
			topRows.pushBounded(item, keepCount)
		} else {
			rows = append(rows, item)
		}
	}
	if useTopN {
		rows = topRows.items
	}

	// Sort a permutation of positions instead of the items themselves: a swap
	// moves 4 bytes instead of a whole orderedRawRow, which is what dominates
	// large sorts. perm starts as the identity permutation, so its values are
	// pre-sort positions; breaking comparator ties on them makes the unstable
	// (pdqsort) sort.Sort reproduce exactly the order sort.Stable produced
	// here before — scan order for full sorts, heap order for top-N — without
	// symMerge's O(n log² n) element moves.
	perm := make([]int32, len(rows))
	for i := range perm {
		perm[i] = int32(i)
	}
	sort.Sort(orderedRawRowsPermAsc{plan: plan, items: rows, perm: perm})

	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(perm) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	perm = perm[start:]
	if plan.limit != nil && *plan.limit < len(perm) {
		perm = perm[:*plan.limit]
	}

	outRows := make([]Row, 0, len(perm))
	for _, p := range perm {
		out, err := projectRawRow(plan, rows[p].raw)
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

// rawKeyArenaChunkRows sizes keyArena chunks in rows' worth of keys. Chunking
// keeps allocation proportional to matched rows — a selective WHERE must not
// pre-pay key storage for the whole table — while keeping each chunk's keys
// densely packed for the sort comparator.
const rawKeyArenaChunkRows = 4096

// reserveKeySlots carves a length-k keys slice out of arena, starting a fresh
// chunk when the current one is full. Full chunks stay referenced by the item
// key slices already pointing into them; the capped three-index views can
// never overlap.
func reserveKeySlots(arena []any, k int) (newArena, keys []any) {
	if len(arena)+k > cap(arena) {
		arena = make([]any, 0, rawKeyArenaChunkRows*k)
	}
	start := len(arena)
	arena = arena[: start+k : cap(arena)]
	return arena, arena[start : start+k : start+k]
}

// orderedRawRowsPermAsc sorts a permutation of positions into items rather
// than items itself. orderedRawRow is a 64-byte, pointer-heavy struct, so
// swapping elements directly is the dominant sort cost at scale; swapping
// int32 positions moves 4 bytes. perm must start as the identity permutation:
// its values are pre-sort positions, and the comparator breaks key ties on
// them, which both pins a total order (so the pdqsort result is unique) and
// reproduces stable-sort semantics. int32 suffices because these are indexes
// into an in-memory row slice.
type orderedRawRowsPermAsc struct {
	plan  *simpleSelectPlan
	items []orderedRawRow
	perm  []int32
}

func (s orderedRawRowsPermAsc) Len() int { return len(s.perm) }
func (s orderedRawRowsPermAsc) Less(i, j int) bool {
	pi, pj := s.perm[i], s.perm[j]
	if cmp := compareOrderedRawRows(s.plan, s.items[pi], s.items[pj]); cmp != 0 {
		return cmp < 0
	}
	return pi < pj
}
func (s orderedRawRowsPermAsc) Swap(i, j int) { s.perm[i], s.perm[j] = s.perm[j], s.perm[i] }

type orderedRawRowHeap struct {
	plan  *simpleSelectPlan
	items []orderedRawRow
}

func (h orderedRawRowHeap) Len() int { return len(h.items) }

func (h orderedRawRowHeap) Less(i, j int) bool {
	return compareOrderedRawRows(h.plan, h.items[i], h.items[j]) > 0
}

func (h orderedRawRowHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// orderedRawRowHeapPush and its Fix-path counterpart below replicate
// container/heap's up/down algorithm directly on the concrete type instead of
// going through heap.Interface — same rationale and pattern as
// orderedValueRowHeapPush above: heap.Push boxes each orderedRawRow into an
// `any`, costing one allocation per row while the top-N heap fills.
func orderedRawRowHeapPush(h *orderedRawRowHeap, v orderedRawRow) {
	h.items = append(h.items, v)
	orderedRawRowHeapUp(*h, len(h.items)-1)
}

func orderedRawRowHeapUp(h orderedRawRowHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func orderedRawRowHeapDown(h orderedRawRowHeap, i0 int) {
	n := h.Len()
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

func (h *orderedRawRowHeap) pushBounded(item orderedRawRow, keepCount int) {
	if keepCount <= 0 {
		return
	}
	if len(h.items) < keepCount {
		orderedRawRowHeapPush(h, item)
		return
	}
	if compareOrderedRawRows(h.plan, h.items[0], item) > 0 {
		h.items[0] = item
		orderedRawRowHeapDown(*h, 0)
	}
}

func compareOrderedRawRows(plan *simpleSelectPlan, a, b orderedRawRow) int {
	if len(plan.orderBy) == 1 {
		return compareOrderedValue(a.key, b.key, plan.orderBy[0].Desc)
	}
	for i, oi := range plan.orderBy {
		cmp := compareOrderedValue(a.keys[i], b.keys[i], oi.Desc)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareOrderedValue(a, b any, desc bool) int {
	cmp := compareForOrder(a, b, desc)
	switch {
	case cmp == 0:
		return 0
	case desc && cmp > 0:
		return -1
	case desc:
		return 1
	case cmp < 0:
		return -1
	default:
		return 1
	}
}
