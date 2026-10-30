package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

const (
	defaultResultPageSize = 100
	maxResultPageSize     = 1000
)

type resultPage struct {
	Rows         []tinysql.Row
	TotalRows    int
	FilteredRows int
	Offset       int
	Limit        int
}

// resultPager retains the expensive filter/sort index while the user moves
// through pages of the same result view. Only the small visible row slice is
// rebuilt for each page request.
type resultPager struct {
	result        *tinysql.ResultSet
	filterText    string
	sortColumn    string
	sortDirection string
	rowIndexes    []int
}

func (pager *resultPager) reset() {
	pager.result = nil
	pager.rowIndexes = nil
}

func (pager *resultPager) page(result *tinysql.ResultSet, offset, limit int, filterText, sortColumn, sortDirection string) resultPage {
	normalizedFilter := strings.ToLower(strings.TrimSpace(filterText))
	normalizedDirection := "asc"
	if strings.EqualFold(sortDirection, "desc") {
		normalizedDirection = "desc"
	}
	if pager.result != result || pager.filterText != normalizedFilter || pager.sortColumn != sortColumn || pager.sortDirection != normalizedDirection {
		pager.result = result
		pager.filterText = normalizedFilter
		pager.sortColumn = sortColumn
		pager.sortDirection = normalizedDirection
		pager.rowIndexes = buildResultRowIndexes(result, normalizedFilter, sortColumn, normalizedDirection)
	}
	return sliceResultPage(result, pager.rowIndexes, offset, limit)
}

// buildResultPage keeps large query results inside Go/WASM and only moves the
// visible slice across the relatively expensive syscall/js boundary.
func buildResultPage(result *tinysql.ResultSet, offset, limit int, filterText, sortColumn, sortDirection string) resultPage {
	filterNeedle := strings.ToLower(strings.TrimSpace(filterText))
	rowIndexes := buildResultRowIndexes(result, filterNeedle, sortColumn, sortDirection)
	return sliceResultPage(result, rowIndexes, offset, limit)
}

// A nil index means all rows in their original order. An empty, non-nil index
// means the filter matched no rows.
func buildResultRowIndexes(result *tinysql.ResultSet, filterNeedle, sortColumn, sortDirection string) []int {
	if result == nil {
		return nil
	}
	sortRows := sortColumn != "" && resultHasColumn(result, sortColumn)
	if filterNeedle == "" && !sortRows {
		return nil
	}

	var filterColumns []string
	if filterNeedle != "" {
		// Result rows use lower-case keys. Normalize each column once per view,
		// instead of once per cell in every row.
		filterColumns = make([]string, len(result.Cols))
		for i, column := range result.Cols {
			filterColumns[i] = strings.ToLower(column)
		}
	}
	rowIndexes := make([]int, 0, len(result.Rows))
	for index, row := range result.Rows {
		if filterNeedle == "" || resultRowContains(row, filterColumns, filterNeedle) {
			rowIndexes = append(rowIndexes, index)
		}
	}
	if len(rowIndexes) < 2 {
		return rowIndexes
	}

	if sortRows {
		descending := strings.EqualFold(sortDirection, "desc")
		sortKey := strings.ToLower(sortColumn)
		// When most rows match, sorting integer indexes is cheaper than moving
		// the larger keys on every swap. Preserve that fast path for broad or
		// absent filters.
		if len(rowIndexes) >= len(result.Rows)/2 {
			sortValues := make([]resultViewSortValue, len(result.Rows))
			for index, row := range result.Rows {
				sortValues[index] = newResultViewSortValue(row[sortKey])
			}
			sort.SliceStable(rowIndexes, func(i, j int) bool {
				comparison := compareResultViewSortValues(sortValues[rowIndexes[i]], sortValues[rowIndexes[j]])
				if descending {
					return comparison > 0
				}
				return comparison < 0
			})
			return rowIndexes
		}
		// Convert only matching rows. A selective filter can leave very few
		// visible rows out of a large result, and converting the other values
		// needlessly consumes CPU and memory in WASM.
		sortValues := make([]resultViewSortValue, len(rowIndexes))
		for i, index := range rowIndexes {
			sortValues[i] = newResultViewSortValue(result.Rows[index][sortKey])
		}
		sort.Stable(resultViewSorter{indexes: rowIndexes, values: sortValues, descending: descending})
	}
	return rowIndexes
}

// Keep sort keys aligned with their row indexes without copying row maps.
type resultViewSorter struct {
	indexes    []int
	values     []resultViewSortValue
	descending bool
}

func (s resultViewSorter) Len() int { return len(s.indexes) }
func (s resultViewSorter) Less(i, j int) bool {
	comparison := compareResultViewSortValues(s.values[i], s.values[j])
	if s.descending {
		return comparison > 0
	}
	return comparison < 0
}
func (s resultViewSorter) Swap(i, j int) {
	s.indexes[i], s.indexes[j] = s.indexes[j], s.indexes[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type resultViewSortValue struct {
	isNil    bool
	isNumber bool
	number   float64
	text     string
}

func newResultViewSortValue(value any) resultViewSortValue {
	if value == nil {
		return resultViewSortValue{isNil: true}
	}
	number, isNumber := resultViewNumber(value)
	text, isString := value.(string)
	if !isString {
		text = fmt.Sprint(value)
	}
	return resultViewSortValue{
		isNumber: isNumber,
		number:   number,
		text:     strings.ToLower(text),
	}
}

func compareResultViewSortValues(left, right resultViewSortValue) int {
	if left.isNil && right.isNil {
		return 0
	}
	if left.isNil {
		return 1
	}
	if right.isNil {
		return -1
	}
	if left.isNumber && right.isNumber {
		switch {
		case left.number < right.number:
			return -1
		case left.number > right.number:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(left.text, right.text)
}

func sliceResultPage(result *tinysql.ResultSet, rowIndexes []int, offset, limit int) resultPage {
	if limit <= 0 {
		limit = defaultResultPageSize
	}
	if limit > maxResultPageSize {
		limit = maxResultPageSize
	}
	if offset < 0 {
		offset = 0
	}
	if result == nil {
		return resultPage{Rows: []tinysql.Row{}, Limit: limit}
	}

	filteredRows := len(rowIndexes)
	if rowIndexes == nil {
		filteredRows = len(result.Rows)
	}
	if offset > filteredRows {
		offset = filteredRows
	}
	end := offset + limit
	if end > filteredRows {
		end = filteredRows
	}

	rows := make([]tinysql.Row, 0, end-offset)
	if rowIndexes == nil {
		rows = append(rows, result.Rows[offset:end]...)
	} else {
		for _, index := range rowIndexes[offset:end] {
			rows = append(rows, result.Rows[index])
		}
	}
	return resultPage{
		Rows:         rows,
		TotalRows:    len(result.Rows),
		FilteredRows: filteredRows,
		Offset:       offset,
		Limit:        limit,
	}
}

// columns are normalized lower-case result-row keys.
func resultRowContains(row tinysql.Row, columns []string, needle string) bool {
	for _, column := range columns {
		value, ok := row[column]
		if !ok || value == nil {
			continue
		}
		// The filter scans every visible cell of every row (debounced in the
		// UI precisely because it's expensive), and text columns are the
		// common case. fmt.Sprint's default formatting of a bare string is
		// the string itself unchanged, so bypassing its reflection-based
		// dispatch for the already-a-string case is byte-for-byte identical
		// to what it would have produced, just without the interface-type
		// switch and formatter machinery. Other types still go through
		// fmt.Sprint, unchanged.
		text, isString := value.(string)
		if !isString {
			text = fmt.Sprint(value)
		}
		if resultContainsFolded(text, needle) {
			return true
		}
	}
	return false
}

// resultContainsFolded implements the existing Lower(text)-then-Contains
// semantics without allocating a lower-case copy for short ASCII values.
// Long inputs use strings.Contains' efficient implementation, and Unicode is
// always handled by Go's case mapping (which can change encoded length).
func resultContainsFolded(text, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(text) > 128 || len(needle) > 16 {
		return strings.Contains(strings.ToLower(text), needle)
	}
	for i := 0; i < len(needle); i++ {
		if needle[i] >= 0x80 {
			return strings.Contains(strings.ToLower(text), needle)
		}
	}
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c >= 0x80 {
			return strings.Contains(strings.ToLower(text), needle)
		}
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != needle[0] || i+len(needle) > len(text) {
			continue
		}
		matched := true
		for j := 1; j < len(needle); j++ {
			c = text[i+j]
			if c >= 0x80 {
				return strings.Contains(strings.ToLower(text), needle)
			}
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != needle[j] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func resultHasColumn(result *tinysql.ResultSet, column string) bool {
	for _, candidate := range result.Cols {
		if candidate == column {
			return true
		}
	}
	return false
}

func compareResultViewValues(left, right any) int {
	return compareResultViewSortValues(newResultViewSortValue(left), newResultViewSortValue(right))
}

func resultViewNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(number), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
