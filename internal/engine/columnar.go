package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ColumnarResultSet stores one positional value slice per output column.
// Values[column][row] addresses a cell; NULL is nil. Cols preserves the same
// names and order as ResultSet. RowCount is explicit for zero-column results.
// The slices belong to the caller and are never pooled or reused by execution.
// As with ResultSet, nested cell values (e.g. BLOBs) are not deep-copied.
// This is a compact result format, not an Arrow buffer or a disk storage mode.
type ColumnarResultSet struct {
	Cols     []string
	Values   [][]any
	RowCount int
}

// ExecuteColumnar executes a SELECT with the usual authorization, statement
// locking and SQL semantics. Simple unordered scans project directly into
// columns without allocating Row maps. Other SELECT shapes use the existing
// executor and transpose its final result, so their intermediate memory use is
// unchanged. Non-SELECT statements are rejected without executing them.
func ExecuteColumnar(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ColumnarResultSet, error) {
	if _, ok := stmt.(*Select); !ok {
		return nil, fmt.Errorf("columnar results require a SELECT statement")
	}
	result := &ColumnarResultSet{}
	if _, err := executeStatementWithColumns(ctx, db, tenant, stmt, result); err != nil {
		return nil, err
	}
	return result, nil
}

func columnarFromRows(rs *ResultSet) ColumnarResultSet {
	result := newColumnarResult(rs.Cols, len(rs.Rows))
	for _, row := range rs.Rows {
		for c, name := range rs.Cols {
			v, _ := getVal(row, name)
			result.Values[c] = append(result.Values[c], v)
		}
	}
	result.RowCount = len(rs.Rows)
	return result
}

func newColumnarResult(cols []string, capacity int) ColumnarResultSet {
	result := ColumnarResultSet{Cols: append([]string(nil), cols...), Values: make([][]any, len(cols))}
	for c := range result.Values {
		result.Values[c] = make([]any, 0, capacity)
	}
	return result
}

func executeSimpleColumnarSelect(env ExecEnv, s *Select, out *ColumnarResultSet) (bool, error) {
	if !streamableSimpleSelect(s) {
		return false, nil
	}
	plan, handled, err := buildSimpleSelectPlan(env, s)
	if !handled || err != nil {
		return handled, err
	}
	// Colliding aliases have Row-map overwrite semantics. Use the existing
	// materializer for those queries so both APIs expose identical values.
	if !distinctProjectionsSafe(plan.projs) {
		return false, nil
	}
	for i, p := range plan.projs {
		for j, other := range plan.projs {
			if i != j && p.key == other.altKey {
				return false, nil
			}
		}
	}
	if plan.limit != nil && *plan.limit == 0 {
		*out = newColumnarResult(plan.outputCols, 0)
		return true, nil
	}
	rows := simplePlanRows(plan)
	n := len(rows)
	if plan.rowIDs != nil {
		n = len(plan.rowIDs)
	}
	capacity := simpleSelectInitialCap(plan)
	if plan.limit != nil && *plan.limit >= 0 {
		capacity = min(capacity, *plan.limit)
	}
	capacityHint := capacity
	if plan.where != nil && !plan.filterFullyCovered {
		capacity = min(capacity, 64)
	}
	*out = newColumnarResult(plan.outputCols, capacity)
	offset := 0
	if plan.offset != nil && *plan.offset > 0 {
		offset = *plan.offset
	}
	start := 0
	if plan.where == nil && plan.rowIDs == nil {
		// Match the unfiltered Row path: skipped projections are not evaluated.
		if n > 0 {
			if err := checkCtx(env.ctx); err != nil {
				return true, err
			}
		}
		start, offset = min(offset, n), 0
	}
	var skippedValues []any
	if offset > 0 {
		skippedValues = make([]any, len(plan.projs))
	}
	for i := start; i < n; i++ {
		if (i-start)&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return true, err
			}
		}
		id := i
		if plan.rowIDs != nil {
			id = plan.rowIDs[i]
		}
		if id < 0 || id >= len(rows) {
			return true, fmt.Errorf("index %q returned invalid row id %d", plan.indexName, id)
		}
		match := plan.where == nil || plan.filterFullyCovered
		if !match {
			match, err = evalRawWhere(plan, rows[id])
			if err != nil {
				return true, err
			}
		}
		if !match {
			continue
		}
		if offset > 0 {
			if err := projectRawValues(plan, rows[id], skippedValues); err != nil {
				return true, err
			}
			offset--
			continue
		}
		// Explicit geometric growth avoids repeatedly copying large interface
		// arrays under append's more conservative growth rule. Bound the target
		// by remaining candidates and LIMIT; a sparse tail cannot produce more
		// rows than remain in the source. All columns always have equal length.
		if len(out.Values) > 0 && len(out.Values[0]) == cap(out.Values[0]) {
			additional := min(max(out.RowCount, 64), n-i)
			if plan.limit != nil {
				additional = min(additional, *plan.limit-out.RowCount)
			}
			for c := range out.Values {
				out.Values[c] = slices.Grow(out.Values[c], additional)
			}
		}
		if err := appendColumnarRow(plan, rows[id], out); err != nil {
			return true, err
		}
		out.RowCount++
		if out.RowCount == 64 && i-start < 128 && capacityHint > 64 {
			for c := range out.Values {
				out.Values[c] = slices.Grow(out.Values[c], capacityHint-len(out.Values[c]))
			}
		}
		if plan.limit != nil && out.RowCount >= *plan.limit {
			break
		}
	}
	return true, nil
}

// Evaluate projections in the same order as projectRawValues/projectRawRow,
// but write directly into the caller-owned columns without a scratch value
// slice and a second pass. A partial row on error is never returned to callers.
func appendColumnarRow(plan *simpleSelectPlan, raw []any, out *ColumnarResultSet) error {
	for c, p := range plan.projs {
		var v any
		if p.colIdx >= 0 {
			v = raw[p.colIdx]
		} else {
			var err error
			v, err = evalRawExpr(plan, raw, p.expr)
			if err != nil {
				return err
			}
		}
		out.Values[c] = append(out.Values[c], v)
	}
	return nil
}
