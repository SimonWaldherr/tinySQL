// ORDER BY. Sorting either sorts the whole result or, when a LIMIT bounds it,
// keeps only the rows that can still make the cut in a bounded heap. The heap
// implementations are written against their concrete element type rather than
// container/heap, whose any-typed interface costs one allocation per row.
package engine

import (
	"sort"
	"strings"
)

// applySortOrder applies ORDER BY sorting to rows. Sort keys are extracted
// once per row up front (same helper as applySortOrderWithLimit's TopN path)
// instead of re-looking them up from the row map on every comparator call —
// a map lookup per column per comparison adds up fast under sort.SliceStable's
// O(n log n) comparisons.
func applySortOrder(orderBy []OrderItem, outRows []Row) []Row {
	if len(orderBy) == 0 || len(outRows) <= 1 {
		return outRows
	}
	lcOrdCols := make([]string, len(orderBy))
	for idx, oi := range orderBy {
		lcOrdCols[idx] = strings.ToLower(oi.Col)
	}
	items := make([]orderedValueRow, len(outRows))
	for i, row := range outRows {
		items[i] = buildOrderByValues(row, lcOrdCols)
	}
	sort.Stable(orderedValueRowsAsc{orderBy: orderBy, items: items})
	for i, item := range items {
		outRows[i] = item.row
	}
	return outRows
}

type orderedValueRow struct {
	row  Row
	keys []any
}

type orderedValueRowHeap struct {
	orderBy []OrderItem
	items   []orderedValueRow
}

func (h orderedValueRowHeap) Len() int { return len(h.items) }

func (h orderedValueRowHeap) Less(i, j int) bool {
	return compareOrderedValueRows(h.orderBy, h.items[i], h.items[j]) > 0
}

func (h orderedValueRowHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// orderedValueRowHeapPush/pushBounded's Fix path replicate container/heap's
// up/down algorithm directly on the concrete type instead of going through
// heap.Interface. heap.Push/Fix take/operate via an `any`-boxing Push method,
// which forces every orderedValueRow (a Row map plus a keys slice) to be
// heap-allocated just to box it into the interface — on an ORDER BY ... LIMIT
// N query this is up to N allocations purely from filling the top-N heap,
// mirroring the same fix already applied to vecScoredHeap (vector_search.go)
// and vecMinScoredHeap (vector_index.go).
func orderedValueRowHeapPush(h *orderedValueRowHeap, v orderedValueRow) {
	h.items = append(h.items, v)
	orderedValueRowHeapUp(*h, len(h.items)-1)
}

func orderedValueRowHeapUp(h orderedValueRowHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func orderedValueRowHeapDown(h orderedValueRowHeap, i0 int) {
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

func (h *orderedValueRowHeap) pushBounded(item orderedValueRow, keepCount int) {
	if keepCount <= 0 {
		return
	}
	if len(h.items) < keepCount {
		orderedValueRowHeapPush(h, item)
		return
	}
	if compareOrderedValueRows(h.orderBy, h.items[0], item) > 0 {
		h.items[0] = item
		orderedValueRowHeapDown(*h, 0)
	}
}

// orderedValueRowsAsc adapts a []orderedValueRow slice to sort.Interface with
// a concrete Swap. sort.Slice/SliceStable instead build their Swap from
// reflect.Swapper, which falls back to a generic reflect-driven memmove for
// any element type it doesn't special-case (anything above 16 bytes, or
// smaller but containing pointers) — orderedValueRow (a Row map pointer plus
// a keys slice) hits that slow path on every swap. A concrete Swap is a
// plain two-field assignment, same fix already applied to the heap types
// below via orderedValueRowHeapPush/Fix instead of container/heap.
type orderedValueRowsAsc struct {
	orderBy []OrderItem
	items   []orderedValueRow
}

func (s orderedValueRowsAsc) Len() int { return len(s.items) }
func (s orderedValueRowsAsc) Less(i, j int) bool {
	return compareOrderedValueRows(s.orderBy, s.items[i], s.items[j]) < 0
}
func (s orderedValueRowsAsc) Swap(i, j int) { s.items[i], s.items[j] = s.items[j], s.items[i] }

func compareOrderedValueRows(orderBy []OrderItem, a, b orderedValueRow) int {
	for i, oi := range orderBy {
		cmp := compareOrderedValue(a.keys[i], b.keys[i], oi.Desc)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func buildOrderByValues(row Row, lcOrdCols []string) orderedValueRow {
	keys := make([]any, len(lcOrdCols))
	for i, col := range lcOrdCols {
		keys[i] = row[col]
	}
	return orderedValueRow{row: row, keys: keys}
}

func applySortOrderWithLimit(orderBy []OrderItem, outRows []Row, limit, offset *int) []Row {
	if len(orderBy) == 0 || len(outRows) <= 1 {
		return outRows
	}
	if limit != nil && *limit <= 0 {
		return []Row{}
	}
	if limit == nil && offset == nil {
		return applySortOrder(orderBy, outRows)
	}

	lcOrdCols := make([]string, len(orderBy))
	for idx, oi := range orderBy {
		lcOrdCols[idx] = strings.ToLower(oi.Col)
	}

	keepCount := len(outRows)
	if limit != nil {
		keepCount = *limit
		if offset != nil {
			keepCount += *offset
		}
		if keepCount > len(outRows) {
			keepCount = len(outRows)
		}
	}
	if keepCount <= 0 {
		return []Row{}
	}

	items := make([]orderedValueRow, 0, min(cap(outRows), keepCount))
	var topRows orderedValueRowHeap
	useTopN := limit != nil && keepCount > 0 && keepCount < len(outRows)
	if useTopN {
		topRows = orderedValueRowHeap{
			orderBy: orderBy,
			items:   make([]orderedValueRow, 0, min(cap(outRows), keepCount)),
		}
	}

	for _, row := range outRows {
		item := buildOrderByValues(row, lcOrdCols)
		if useTopN {
			topRows.pushBounded(item, keepCount)
		} else {
			items = append(items, item)
		}
	}
	if useTopN {
		items = topRows.items
	}

	sort.Stable(orderedValueRowsAsc{orderBy: orderBy, items: items})

	sorted := make([]Row, len(items))
	for i, item := range items {
		sorted[i] = item.row
	}
	return sorted
}
