// Typed single-column ORDER BY for the raw fast path. The generic ordered path
// boxes every key and compares it through interface type switches. For a column
// whose stored values share one Go type (int, int64, float64 or string) the same
// comparisons can run on unboxed keys. Results are identical: ties keep scan
// order, NULLs follow compareOrderedValueNulls, and the descending flag applies
// to keys only. A column with mixed types, a NaN, or any other value sends the
// query back to the generic path, so this file never decides ordering rules.
package engine

import (
	"math"
	"slices"
)

// typedOrderKey lists the stored Go types this path sorts without boxing.
// Each is a type the INSERT path and the storage layer produce for a column.
type typedOrderKey interface {
	int | int64 | float64 | string
}

type typedOrderKind uint8

const (
	typedOrderNone typedOrderKind = iota
	typedOrderInt
	typedOrderInt64
	typedOrderFloat
	typedOrderString
)

// typedOrderItem is one matching candidate. pos is its index among the scanned
// candidates and breaks ties, as the generic path's idx does. row is the index
// into the source rows used for projection. A NULL key sets null and leaves key
// at its zero value.
type typedOrderItem[K typedOrderKey] struct {
	key  K
	pos  int32
	row  int32
	null bool
}

// simpleTypedOrderColumn reports the raw column of a single ORDER BY term that
// names a column directly. Multi-column and expression orders use other paths.
func simpleTypedOrderColumn(plan *simpleSelectPlan) (int, bool) {
	if len(plan.orderBy) != 1 || len(plan.orderCols) != 1 {
		return 0, false
	}
	return plan.orderCols[0], true
}

// executeTypedOrderedSelect sorts the candidates by one column's typed keys. It
// reports handled=false, without error, whenever the generic path must decide,
// and it may be called only after the float and scorer paths declined.
func executeTypedOrderedSelect(env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, column, keepCount int) (*ResultSet, bool, error) {
	candidates := len(sourceRows)
	if plan.rowIDs != nil {
		candidates = len(plan.rowIDs)
	}
	// Positions and row indexes are stored as int32, as the permutation sort does.
	if len(sourceRows) > math.MaxInt32 {
		return nil, false, nil
	}
	kind, err := typedOrderKeyKind(env, plan, sourceRows, column, candidates)
	if err != nil {
		return nil, true, err
	}
	switch kind {
	case typedOrderInt:
		return runTypedOrderedSelect[int](env, plan, sourceRows, column, keepCount, candidates)
	case typedOrderInt64:
		return runTypedOrderedSelect[int64](env, plan, sourceRows, column, keepCount, candidates)
	case typedOrderFloat:
		return runTypedOrderedSelect[float64](env, plan, sourceRows, column, keepCount, candidates)
	case typedOrderString:
		return runTypedOrderedSelect[string](env, plan, sourceRows, column, keepCount, candidates)
	}
	return nil, false, nil
}

// typedOrderKeyKind validates the sort column of every candidate. It returns the
// single Go type shared by all non-NULL keys. It returns typedOrderNone when the
// keys mix types, contain NaN, hold an unsupported type or are all NULL, or when
// a source row is missing the column or a row id is invalid. The generic path
// then reports the same error it always did.
func typedOrderKeyKind(env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, column, candidates int) (typedOrderKind, error) {
	kind := typedOrderNone
	for i := 0; i < candidates; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(sourceRows) {
			return typedOrderNone, nil
		}
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return typedOrderNone, err
			}
		}
		raw := sourceRows[rowID]
		if column >= len(raw) {
			return typedOrderNone, nil
		}
		var current typedOrderKind
		switch value := raw[column].(type) {
		case nil:
			continue
		case int:
			current = typedOrderInt
		case int64:
			current = typedOrderInt64
		case float64:
			// NaN is not totally ordered; the generic comparison decides it.
			if math.IsNaN(value) {
				return typedOrderNone, nil
			}
			current = typedOrderFloat
		case string:
			current = typedOrderString
		default:
			return typedOrderNone, nil
		}
		if kind != typedOrderNone && kind != current {
			return typedOrderNone, nil
		}
		kind = current
	}
	return kind, nil
}

// runTypedOrderedSelect scans the candidates with the plan's filter, keeps their
// unboxed keys, orders them and projects the requested page. Its phase names and
// row counts match the generic ordered path, so query profiles do not change.
func runTypedOrderedSelect[K typedOrderKey](env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, column, keepCount, candidates int) (*ResultSet, bool, error) {
	// Keep the bounded heap only where it can discard candidates. Retaining
	// nearly every candidate is cheaper to sort once, as the generic path decides.
	useTopN := keepCount > 0 && keepCount <= candidates-candidates/10

	desc := plan.orderBy[0].Desc
	nullsFirst := plan.orderBy[0].Nulls == NullsFirst

	var scanPhase *queryPhase
	if env.profile != nil {
		operation := "SCAN/FILTER"
		detail := "collect raw ordering keys"
		if useTopN {
			operation = "SCAN/FILTER/TOP-N"
			detail = "retain bounded ordering candidates"
		}
		_, scanPhase = startQueryPhase(env, operation, detail)
		defer scanPhase.finish(-1)
	}

	var top typedOrderHeap[K]
	var items []typedOrderItem[K]
	if useTopN {
		top = typedOrderHeap[K]{
			items:      make([]typedOrderItem[K], 0, keepCount),
			desc:       desc,
			nullsFirst: nullsFirst,
		}
	} else {
		items = make([]typedOrderItem[K], 0, candidates)
	}

	for i := 0; i < candidates; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		raw := sourceRows[rowID]
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		var match bool
		var err error
		if plan.filter != nil {
			match, err = plan.filter(raw)
		} else {
			match, err = evalRawWhere(plan, raw)
		}
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}
		item := typedOrderItem[K]{pos: int32(i), row: int32(rowID)}
		if value := raw[column]; value == nil {
			item.null = true
		} else {
			// Validation accepted every candidate's type, so the assertion holds.
			key, ok := value.(K)
			if !ok {
				return nil, false, nil
			}
			item.key = key
		}
		if useTopN {
			top.pushBounded(item, keepCount)
		} else {
			items = append(items, item)
		}
	}
	if useTopN {
		items = top.items
	}
	scanPhase.finish(int64(len(items)))

	var sortPhase *queryPhase
	if env.profile != nil {
		_, sortPhase = startQueryPhase(env, "SORT", "order raw candidates")
	}
	slices.SortFunc(items, func(a, b typedOrderItem[K]) int {
		return typedOrderSortCmp(a, b, desc, nullsFirst)
	})
	sortPhase.finish(int64(len(items)))

	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(items) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	items = items[start:]
	if plan.limit != nil && *plan.limit < len(items) {
		items = items[:*plan.limit]
	}

	var projectPhase *queryPhase
	if env.profile != nil {
		_, projectPhase = startQueryPhase(env, "PROJECT", "materialize ordered page")
		defer projectPhase.finish(-1)
	}
	outRows := make([]Row, 0, len(items))
	for _, item := range items {
		out, err := projectRawRow(plan, sourceRows[item.row])
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
	}
	projectPhase.finish(int64(len(outRows)))
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

// typedNullWorse orders a pair in which at least one key is NULL. NULL placement
// depends only on NULLS FIRST, not on the direction, and two NULL keys tie.
func typedNullWorse(aNull, bNull bool, aPos, bPos int32, nullsFirst bool) bool {
	switch {
	case aNull && bNull:
	case aNull:
		return !nullsFirst
	default:
		return nullsFirst
	}
	return aPos > bPos
}

// typedItemWorse reports whether a sorts after b in the output order. Scan
// positions are unique, so the order is strict and total. The non-NULL branch is
// small enough to inline into every heap comparison.
func typedItemWorse[K typedOrderKey](a, b *typedOrderItem[K], desc, nullsFirst bool) bool {
	if a.null || b.null {
		return typedNullWorse(a.null, b.null, a.pos, b.pos, nullsFirst)
	}
	if a.key != b.key {
		// Descending keys reverse the comparison; equal keys keep scan order.
		return (a.key < b.key) == desc
	}
	return a.pos > b.pos
}

// typedOrderSortCmp is the three-way form of typedItemWorse used for the final
// ordering. It returns 0 only for the same candidate.
func typedOrderSortCmp[K typedOrderKey](a, b typedOrderItem[K], desc, nullsFirst bool) int {
	if a.pos == b.pos {
		return 0
	}
	if typedItemWorse(&a, &b, desc, nullsFirst) {
		return 1
	}
	return -1
}

// typedOrderHeap keeps the best keep candidates. Its root is the worst retained
// candidate, so a new candidate replaces it only when the candidate sorts first.
type typedOrderHeap[K typedOrderKey] struct {
	items      []typedOrderItem[K]
	desc       bool
	nullsFirst bool
}

func (h *typedOrderHeap[K]) pushBounded(item typedOrderItem[K], keep int) {
	if len(h.items) < keep {
		h.items = append(h.items, item)
		h.siftUp(len(h.items)-1, item)
		return
	}
	if typedItemWorse(&h.items[0], &item, h.desc, h.nullsFirst) {
		h.replaceRoot(item)
	}
}

// siftUp places item at position i, moving it toward the root while it sorts
// after its parent. Every element above i is already in heap order.
func (h *typedOrderHeap[K]) siftUp(i int, item typedOrderItem[K]) {
	for i > 0 {
		parent := (i - 1) / 2
		if !typedItemWorse(&item, &h.items[parent], h.desc, h.nullsFirst) {
			break
		}
		h.items[i] = h.items[parent]
		i = parent
	}
	h.items[i] = item
}

// replaceRoot removes the worst candidate and inserts item in its place. The
// hole sinks along the worse child with one comparison per level, and item then
// rises from the leaf it reaches. A new best candidate usually stays there, so
// this takes about half the comparisons of a standard sift-down.
func (h *typedOrderHeap[K]) replaceRoot(item typedOrderItem[K]) {
	n := len(h.items)
	i := 0
	for {
		child := 2*i + 1
		if child >= n {
			break
		}
		if right := child + 1; right < n && typedItemWorse(&h.items[right], &h.items[child], h.desc, h.nullsFirst) {
			child = right
		}
		h.items[i] = h.items[child]
		i = child
	}
	h.siftUp(i, item)
}
