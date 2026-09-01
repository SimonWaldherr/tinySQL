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

func buildResultRowIndexes(result *tinysql.ResultSet, filterNeedle, sortColumn, sortDirection string) []int {
	if result == nil {
		return nil
	}

	rowIndexes := make([]int, 0, len(result.Rows))
	for index, row := range result.Rows {
		if filterNeedle == "" || resultRowContains(row, result.Cols, filterNeedle) {
			rowIndexes = append(rowIndexes, index)
		}
	}

	if sortColumn != "" && resultHasColumn(result, sortColumn) {
		descending := strings.EqualFold(sortDirection, "desc")
		// sort.Slice calls its comparison function O(n log n) times. Convert a
		// value to its numeric/text comparison representation once per row rather
		// than repeatedly parsing and stringifying it in WASM for every compare.
		sortValues := make([]resultViewSortValue, len(result.Rows))
		for index, row := range result.Rows {
			value, _ := tinysql.GetVal(row, sortColumn)
			sortValues[index] = newResultViewSortValue(value)
		}
		sort.SliceStable(rowIndexes, func(i, j int) bool {
			comparison := compareResultViewSortValues(sortValues[rowIndexes[i]], sortValues[rowIndexes[j]])
			if descending {
				return comparison > 0
			}
			return comparison < 0
		})
	}
	return rowIndexes
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
	return resultViewSortValue{
		isNumber: isNumber,
		number:   number,
		text:     strings.ToLower(fmt.Sprint(value)),
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
	if offset > filteredRows {
		offset = filteredRows
	}
	end := offset + limit
	if end > filteredRows {
		end = filteredRows
	}

	rows := make([]tinysql.Row, 0, end-offset)
	for _, index := range rowIndexes[offset:end] {
		rows = append(rows, result.Rows[index])
	}
	return resultPage{
		Rows:         rows,
		TotalRows:    len(result.Rows),
		FilteredRows: filteredRows,
		Offset:       offset,
		Limit:        limit,
	}
}

func resultRowContains(row tinysql.Row, columns []string, needle string) bool {
	for _, column := range columns {
		value, ok := tinysql.GetVal(row, column)
		if ok && value != nil && strings.Contains(strings.ToLower(fmt.Sprint(value)), needle) {
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
