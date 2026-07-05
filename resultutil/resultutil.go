// Package resultutil contains small helpers for adapting tinySQL ResultSets to
// UI/API-friendly string matrices and compact result summaries.
package resultutil

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

const (
	defaultMaxRows      = 20
	defaultMaxExamples  = 3
	defaultMaxTopValues = 8
)

// SummaryOptions controls how much raw data and profiling detail is included.
type SummaryOptions struct {
	MaxRows      int
	MaxExamples  int
	MaxTopValues int
}

// Summary is a compact, JSON-friendly representation of a query result.
type Summary struct {
	Columns    []string        `json:"columns,omitempty"`
	Rows       [][]string      `json:"rows,omitempty"`
	TotalRows  int             `json:"total_rows"`
	Summarized bool            `json:"summarized"`
	Profile    []ColumnProfile `json:"profile,omitempty"`
}

// ColumnProfile describes one result column across all rows.
type ColumnProfile struct {
	Name      string       `json:"name"`
	NonEmpty  int          `json:"non_empty"`
	Nulls     int          `json:"nulls"`
	Distinct  int          `json:"distinct"`
	TopValues []ValueCount `json:"top_values,omitempty"`
	Numeric   bool         `json:"numeric"`
	Min       string       `json:"min,omitempty"`
	Max       string       `json:"max,omitempty"`
	Sum       string       `json:"sum,omitempty"`
	Avg       string       `json:"avg,omitempty"`
	Examples  []string     `json:"examples,omitempty"`
}

// ValueCount is one value frequency entry in a ColumnProfile.
type ValueCount struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// ResultSetToStringMatrix converts a tinySQL ResultSet to column names and
// string rows while preserving ResultSet column order.
func ResultSetToStringMatrix(rs *tinysql.ResultSet) ([]string, [][]string) {
	if rs == nil {
		return nil, nil
	}
	columns := append([]string(nil), rs.Cols...)
	rows := make([][]string, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		out := make([]string, len(columns))
		for i, col := range columns {
			if v, ok := tinysql.GetVal(row, col); ok {
				out[i] = ValueToString(v)
			}
		}
		rows = append(rows, out)
	}
	return columns, rows
}

// StringMatrixToResultSet converts string rows back to a tinySQL ResultSet.
func StringMatrixToResultSet(columns []string, rows [][]string) *tinysql.ResultSet {
	rs := &tinysql.ResultSet{Cols: append([]string(nil), columns...)}
	rs.Rows = make([]tinysql.Row, 0, len(rows))
	for _, row := range rows {
		out := make(tinysql.Row, len(columns))
		for i, col := range columns {
			value := ""
			if i < len(row) {
				value = row[i]
			}
			out[strings.ToLower(col)] = value
		}
		rs.Rows = append(rs.Rows, out)
	}
	return rs
}

// SummarizeMatrix returns raw rows when the result is small and a profiled,
// bounded sample when the result is too large to pass around in full.
func SummarizeMatrix(columns []string, rows [][]string, opts SummaryOptions) *Summary {
	opts = opts.withDefaults()
	ctx := &Summary{
		Columns:   append([]string(nil), columns...),
		TotalRows: len(rows),
	}
	if len(rows) <= opts.MaxRows {
		ctx.Rows = cloneRows(rows)
		return ctx
	}

	ctx.Summarized = true
	ctx.Rows = cloneRows(rows[:opts.MaxRows])
	ctx.Profile = ProfileColumns(columns, rows, opts)
	return ctx
}

// SummarizeResultSet summarizes a tinySQL ResultSet.
func SummarizeResultSet(rs *tinysql.ResultSet, opts SummaryOptions) *Summary {
	columns, rows := ResultSetToStringMatrix(rs)
	return SummarizeMatrix(columns, rows, opts)
}

// ProfileColumns profiles string matrix columns across all supplied rows.
func ProfileColumns(columns []string, rows [][]string, opts SummaryOptions) []ColumnProfile {
	opts = opts.withDefaults()
	profiles := make([]ColumnProfile, len(columns))
	for colIdx, colName := range columns {
		counts := make(map[string]int)
		exampleSeen := make(map[string]bool)
		profile := ColumnProfile{Name: colName}
		numericCount := 0
		var minVal, maxVal, sum float64
		minSet := false

		for _, row := range rows {
			value := ""
			if colIdx < len(row) {
				value = strings.TrimSpace(row[colIdx])
			}
			if value == "" {
				profile.Nulls++
				continue
			}
			profile.NonEmpty++
			counts[value]++
			if len(profile.Examples) < opts.MaxExamples && !exampleSeen[value] {
				profile.Examples = append(profile.Examples, value)
				exampleSeen[value] = true
			}
			if f, ok := parseNumericString(value); ok {
				if !minSet {
					minVal, maxVal = f, f
					minSet = true
				}
				if f < minVal {
					minVal = f
				}
				if f > maxVal {
					maxVal = f
				}
				sum += f
				numericCount++
			}
		}

		profile.Distinct = len(counts)
		profile.TopValues = TopValues(counts, opts.MaxTopValues)
		if profile.NonEmpty > 0 && numericCount == profile.NonEmpty {
			profile.Numeric = true
			profile.Min = formatFloat(minVal)
			profile.Max = formatFloat(maxVal)
			profile.Sum = formatFloat(sum)
			profile.Avg = formatFloat(sum / float64(numericCount))
		}
		profiles[colIdx] = profile
	}
	return profiles
}

// TopValues returns the most common values, ordered by count desc then value asc.
func TopValues(counts map[string]int, limit int) []ValueCount {
	if limit <= 0 {
		limit = defaultMaxTopValues
	}
	values := make([]ValueCount, 0, len(counts))
	for value, count := range counts {
		values = append(values, ValueCount{Value: value, Count: count})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Count == values[j].Count {
			return values[i].Value < values[j].Value
		}
		return values[i].Count > values[j].Count
	})
	if len(values) > limit {
		values = values[:limit]
	}
	return values
}

// ValueToString converts database values to display/API strings.
func ValueToString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func (opts SummaryOptions) withDefaults() SummaryOptions {
	if opts.MaxRows <= 0 {
		opts.MaxRows = defaultMaxRows
	}
	if opts.MaxExamples <= 0 {
		opts.MaxExamples = defaultMaxExamples
	}
	if opts.MaxTopValues <= 0 {
		opts.MaxTopValues = defaultMaxTopValues
	}
	return opts
}

func cloneRows(rows [][]string) [][]string {
	out := make([][]string, len(rows))
	for i, row := range rows {
		out[i] = append([]string(nil), row...)
	}
	return out
}

func parseNumericString(value string) (float64, bool) {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
