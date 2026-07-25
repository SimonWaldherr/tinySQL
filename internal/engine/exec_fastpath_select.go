// Fast path for a single-table SELECT: an unfiltered scan, a filtered scan, and
// an ordered scan that degenerates to a bounded top-N heap when LIMIT is
// present. The float-keyed variant keeps the sort key unboxed.
package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeSimpleSelectFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleSelectPlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
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
	sort.SliceStable(rows, func(i, j int) bool {
		return compareOrderedFloat(rows[i].key, rows[j].key, topRows.desc) < 0
	})
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
	return compareOrderedFloat(h.items[i].key, h.items[j].key, h.desc) > 0
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
	if compareOrderedFloat(h.items[0].key, item.key, h.desc) <= 0 {
		return
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

func compareOrderedFloat(a, b float64, desc bool) int {
	cmp := 0
	if a < b {
		cmp = -1
	} else if a > b {
		cmp = 1
	}
	if desc {
		return -cmp
	}
	return cmp
}

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
		if len(plan.orderExprs) == 1 {
			key, err := evalRawExpr(plan, raw, plan.orderExprs[0])
			if err != nil {
				return nil, true, err
			}
			item := orderedRawRow{raw: raw, key: key}
			if useTopN {
				topRows.pushBounded(item, keepCount)
			} else {
				rows = append(rows, item)
			}
			continue
		}
		keys := make([]any, len(plan.orderExprs))
		for i, expr := range plan.orderExprs {
			v, err := evalRawExpr(plan, raw, expr)
			if err != nil {
				return nil, true, err
			}
			keys[i] = v
		}
		item := orderedRawRow{raw: raw, keys: keys}
		if useTopN {
			topRows.pushBounded(item, keepCount)
		} else {
			rows = append(rows, item)
		}
	}
	if useTopN {
		rows = topRows.items
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return compareOrderedRawRows(plan, rows[i], rows[j]) < 0
	})

	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(rows) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	rows = rows[start:]
	if plan.limit != nil && *plan.limit < len(rows) {
		rows = rows[:*plan.limit]
	}

	outRows := make([]Row, 0, len(rows))
	for _, item := range rows {
		out, err := projectRawRow(plan, item.raw)
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

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
