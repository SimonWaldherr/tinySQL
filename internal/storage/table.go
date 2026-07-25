// Table: the rows of one table plus the schema and statistics describing them,
// and the dirty tracking that lets the write-ahead log write a delta instead of
// a whole table.
package storage

import (
	"fmt"
	"strings"
	"time"
)

// Table stores rows along with column metadata and indexes.
type Table struct {
	Name string
	Cols []Column
	Rows [][]any
	// Indexes contains materialized secondary and composite indexes keyed by
	// lower-case SQL index name. Unlike catalog metadata these entries are
	// used by the executor and persisted with table snapshots.
	Indexes map[string]*SecondaryIndex
	IsTemp  bool
	colPos  map[string]int
	Version int
	// Stats is populated by ANALYZE and persisted with the table. DML marks it
	// stale rather than trying to estimate distinct values incrementally.
	Stats *TableStats
	// dirtyFrom tracks the first row index modified since the last
	// WAL checkpoint. -1 means no dirty rows (full table must be logged).
	// For append-only workloads (INSERT without UPDATE/DELETE), this
	// enables the WAL to log only new rows instead of the entire table.
	dirtyFrom int
	// dirtyRows lists the rows an UPDATE replaced in place since the last
	// ResetDirty, letting the WAL log those rows instead of the whole table.
	// It is only trusted while dirtyRowsState is dirtyRowsExact; anything that
	// adds, removes or reorders rows takes the state to dirtyRowsUnknown
	// through MarkDirtyFrom, and the WAL falls back to a full-table record.
	// The fallback is the safe direction: a missed MarkRowUpdated costs one
	// oversized record, never a lost change.
	dirtyRows      []int
	dirtyRowsState dirtyRowsState
}

// dirtyRowsState distinguishes "no in-place updates recorded yet" from "the
// recorded list is complete" and from "row positions moved, so no list can
// describe this change".
type dirtyRowsState uint8

const (
	dirtyRowsNone dirtyRowsState = iota
	dirtyRowsExact
	dirtyRowsUnknown
)

// ColumnStats summarizes one column as of TableStats.AnalyzedAt. Min and Max
// are display values for introspection; the planner currently uses row and
// distinct counts, which remain meaningful across all supported column types.
type ColumnStats struct {
	NullCount     int
	DistinctCount int
	Min           string
	Max           string
	HasMinMax     bool
}

// TableStats is the persisted result of ANALYZE for one table.
type TableStats struct {
	RowCount   int
	Columns    map[string]ColumnStats // lower-cased column name → statistics
	AnalyzedAt time.Time
	Stale      bool
}

// NewTable creates a new Table with case-insensitive column lookup indices.
func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int, len(cols))
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp, dirtyFrom: -1, Indexes: make(map[string]*SecondaryIndex)}
}

// Analyze computes exact cardinality, null and simple range summaries for the
// current table contents. The first statistics implementation scans all rows
// deliberately: transparent and correct inputs are more useful than a sampled
// model whose accuracy would need separate policy and tuning.
func (t *Table) Analyze() *TableStats {
	stats := &TableStats{
		RowCount:   len(t.Rows),
		Columns:    make(map[string]ColumnStats, len(t.Cols)),
		AnalyzedAt: time.Now().UTC(),
	}
	for colIdx, column := range t.Cols {
		columnStats := ColumnStats{}
		distinct := make(map[string]struct{})
		var minValue, maxValue any
		for _, row := range t.Rows {
			if colIdx >= len(row) || row[colIdx] == nil {
				columnStats.NullCount++
				continue
			}
			value := row[colIdx]
			distinct[string(CanonicalIndexKey([]any{value}))] = struct{}{}
			if !columnStats.HasMinMax || statsLess(value, minValue) {
				minValue = value
			}
			if !columnStats.HasMinMax || statsLess(maxValue, value) {
				maxValue = value
			}
			columnStats.HasMinMax = true
		}
		columnStats.DistinctCount = len(distinct)
		if columnStats.HasMinMax {
			columnStats.Min = fmt.Sprint(minValue)
			columnStats.Max = fmt.Sprint(maxValue)
		}
		stats.Columns[strings.ToLower(column.Name)] = columnStats
	}
	t.Stats = stats
	return cloneTableStats(stats)
}

// InvalidateStats marks the previous ANALYZE result stale after a mutation.
// RowCount remains useful for observability while distinct/range values are
// excluded from planner decisions until ANALYZE is run again.
func (t *Table) InvalidateStats() {
	if t.Stats == nil {
		return
	}
	t.Stats.RowCount = len(t.Rows)
	t.Stats.Stale = true
}

// Statistics returns a defensive copy of the latest ANALYZE result.
func (t *Table) Statistics() *TableStats { return cloneTableStats(t.Stats) }

func cloneTableStats(stats *TableStats) *TableStats {
	if stats == nil {
		return nil
	}
	copy := *stats
	copy.Columns = make(map[string]ColumnStats, len(stats.Columns))
	for name, column := range stats.Columns {
		copy.Columns[name] = column
	}
	return &copy
}

func statsLess(left, right any) bool {
	if right == nil {
		return true
	}
	leftNumber, leftIsNumber := statsNumber(left)
	rightNumber, rightIsNumber := statsNumber(right)
	if leftIsNumber && rightIsNumber {
		return leftNumber < rightNumber
	}
	if leftTime, ok := left.(time.Time); ok {
		if rightTime, ok := right.(time.Time); ok {
			return leftTime.Before(rightTime)
		}
	}
	if leftDuration, ok := left.(time.Duration); ok {
		if rightDuration, ok := right.(time.Duration); ok {
			return leftDuration < rightDuration
		}
	}
	return fmt.Sprint(left) < fmt.Sprint(right)
}

func statsNumber(value any) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

// MarkDirtyFrom records the first row index that was modified. If an earlier
// index is already set, it is kept. Use -1 for non-append mutations (UPDATE,
// DELETE) to force a full-table WAL entry.
//
// The -1 (full-table) sentinel is sticky: once a mutation within a transaction
// forces a full-table entry, a later append-only INSERT must not downgrade it
// to a delta, or the earlier UPDATE/DELETE would be lost on WAL recovery.
// Any call also gives up on the in-place row list: an append shifts nothing but
// adds rows the list cannot describe, and an explicit -1 from a caller that is
// not reporting individual rows means the shape of the change is unknown.
func (t *Table) MarkDirtyFrom(idx int) {
	t.dirtyRows = nil
	t.dirtyRowsState = dirtyRowsUnknown
	if idx < 0 {
		t.dirtyFrom = -1
		return
	}
	if t.dirtyFrom < 0 {
		return // full-table WAL entry already forced; keep the sentinel
	}
	if t.dirtyFrom <= idx {
		return // already tracking earlier rows
	}
	t.dirtyFrom = idx
}

// MarkRowUpdated records that row idx was replaced in place, so the WAL can log
// that row instead of the entire table. UPDATE is the only shape this fits: it
// changes row contents without changing how many rows there are or where they
// sit.
//
// Callers must report every row they change. Reporting none is safe — the WAL
// writes the whole table, as it always did. Reporting some but not all is not,
// so a mutation path that cannot enumerate its rows must call MarkDirtyFrom(-1)
// instead, which permanently gives up the list for this dirty window.
func (t *Table) MarkRowUpdated(idx int) {
	t.dirtyFrom = -1
	if t.dirtyRowsState == dirtyRowsUnknown || idx < 0 {
		return
	}
	t.dirtyRowsState = dirtyRowsExact
	t.dirtyRows = append(t.dirtyRows, idx)
}

// DirtyFrom returns the first dirty row index, or -1 if non-append-only.
func (t *Table) DirtyFrom() int { return t.dirtyFrom }

// DirtyRows returns the in-place updated row indices and whether that list is
// known to be complete. A false second result means the caller must treat the
// whole table as changed.
func (t *Table) DirtyRows() ([]int, bool) {
	if t.dirtyRowsState != dirtyRowsExact || len(t.dirtyRows) == 0 {
		return nil, false
	}
	return t.dirtyRows, true
}

// ResetDirty marks the table as clean (called after WAL checkpoint).
func (t *Table) ResetDirty() {
	t.dirtyFrom = len(t.Rows)
	t.dirtyRows = nil
	t.dirtyRowsState = dirtyRowsNone
}

// ColIndex returns the zero-based index of the named column.
func (t *Table) ColIndex(name string) (int, error) {
	i, ok := t.colPos[strings.ToLower(name)]
	if !ok {
		return -1, fmt.Errorf("unknown column %q on table %q", name, t.Name)
	}
	return i, nil
}
