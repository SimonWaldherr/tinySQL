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

// buildResultPage keeps large query results inside Go/WASM and only moves the
// visible slice across the relatively expensive syscall/js boundary.
func buildResultPage(result *tinysql.ResultSet, offset, limit int, filterText, sortColumn, sortDirection string) resultPage {
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

	filterNeedle := strings.ToLower(strings.TrimSpace(filterText))
	rowIndexes := make([]int, 0, len(result.Rows))
	for index, row := range result.Rows {
		if filterNeedle == "" || resultRowContains(row, result.Cols, filterNeedle) {
			rowIndexes = append(rowIndexes, index)
		}
	}

	if sortColumn != "" && resultHasColumn(result, sortColumn) {
		descending := strings.EqualFold(sortDirection, "desc")
		sort.SliceStable(rowIndexes, func(i, j int) bool {
			left, _ := tinysql.GetVal(result.Rows[rowIndexes[i]], sortColumn)
			right, _ := tinysql.GetVal(result.Rows[rowIndexes[j]], sortColumn)
			comparison := compareResultViewValues(left, right)
			if descending {
				return comparison > 0
			}
			return comparison < 0
		})
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
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return 1
	}
	if right == nil {
		return -1
	}
	if leftNumber, ok := resultViewNumber(left); ok {
		if rightNumber, ok := resultViewNumber(right); ok {
			switch {
			case leftNumber < rightNumber:
				return -1
			case leftNumber > rightNumber:
				return 1
			default:
				return 0
			}
		}
	}
	return strings.Compare(strings.ToLower(fmt.Sprint(left)), strings.ToLower(fmt.Sprint(right)))
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
