// UNION, UNION ALL, EXCEPT and INTERSECT.
package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
)

// setOperationContextBatchRows bounds the amount of pointer copying a compound
// SELECT can do without observing a cancelled request. It matches the cadence
// used by the set-operation materializers below.
const setOperationContextBatchRows = 64

func processUnionClauses(env ExecEnv, union *UnionClause, leftRows []Row, leftCols []string, orderAliases map[string]string) ([]Row, []string, error) {
	resultRows := leftRows
	resultCols := leftCols
	var unionDistinct *setOperationDistinctAccumulator

	for current := union; current != nil; current = current.Next {
		// Adjacent UNION terms can share their membership set. Other set
		// operations change membership or multiplicity and end that run.
		if current.Type != UnionDistinct {
			unionDistinct = nil
		}
		// Execute the right-hand SELECT
		rightResult, err := executeSelect(env, current.Right)
		if err != nil {
			return nil, nil, err
		}

		// Validate column compatibility
		if len(rightResult.Cols) != len(resultCols) {
			return nil, nil, fmt.Errorf("UNION: column count mismatch between queries (%d vs %d)",
				len(resultCols), len(rightResult.Cols))
		}

		addCompoundOrderAliases(orderAliases, rightResult.Cols, resultCols)

		switch current.Type {
		case UnionAll:
			resultRows, err = appendUnionAllRows(env.ctx, resultRows, rightResult, resultCols)

		case UnionDistinct:
			// Hash the accumulated left result only once per UNION run.
			// Rehashing it for every term makes a growing chain quadratic.
			if unionDistinct == nil {
				unionDistinct = newSetOperationDistinctAccumulator(resultCols, len(resultRows)+len(rightResult.Rows))
				if err := unionDistinct.appendRows(env.ctx, resultRows); err != nil {
					return nil, nil, err
				}
			}
			err = unionDistinct.appendTerm(env.ctx, rightResult, resultCols)
			resultRows = unionDistinct.rows

		case Except, Intersect:
			var rightRows []Row
			rightRows, err = alignSetOperationRowsWithContext(env.ctx, rightResult.Rows, rightResult.Cols, resultCols)
			if err != nil {
				return nil, nil, err
			}
			if current.Type == Except {
				resultRows, err = exceptRowsWithContext(env.ctx, resultRows, rightRows, resultCols)
			} else {
				resultRows, err = intersectRowsWithContext(env.ctx, resultRows, rightRows, resultCols)
			}
		}
		if err != nil {
			return nil, nil, err
		}
	}

	return resultRows, resultCols, nil
}

// Appending renamed rows directly avoids an intermediate slice proportional
// to the right-hand term. Every row still contributes to UNION ALL.
func appendUnionAllRows(ctx context.Context, dst []Row, term *ResultSet, targetCols []string) ([]Row, error) {
	if sameSetOperationColumns(term.Cols, targetCols) {
		return appendRowsWithContext(ctx, dst, term.Rows)
	}
	if len(term.Rows) == 0 {
		return dst, nil
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	dst = slices.Grow(dst, len(term.Rows))
	sourceKeys, targetKeys := setOperationColumnKeys(term.Cols), setOperationColumnKeys(targetCols)
	for i, row := range term.Rows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		dst = append(dst, copySetOperationRow(row, sourceKeys, targetKeys))
	}
	return dst, nil
}

// appendRowsWithContext combines compound-select terms in small batches.
// A single append of a very large right-hand term can copy millions of row
// pointers before the engine gets another chance to observe cancellation.
// The row maps themselves are not copied.
func appendRowsWithContext(ctx context.Context, dst, src []Row) ([]Row, error) {
	for start := 0; start < len(src); start += setOperationContextBatchRows {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		end := start + setOperationContextBatchRows
		if end > len(src) {
			end = len(src)
		}
		dst = append(dst, src[start:end]...)
	}
	return dst, nil
}

// addCompoundOrderAliases records aliases from every SELECT term so a trailing
// ORDER BY may refer to the matching result expression of a later term, as
// SQLite permits.  Earlier terms win on ambiguous aliases, matching the
// left-to-right lookup order for compound SELECTs.
func addCompoundOrderAliases(aliases map[string]string, sourceCols, targetCols []string) {
	if aliases == nil {
		return
	}
	for i, source := range sourceCols {
		key := strings.ToLower(source)
		if _, exists := aliases[key]; !exists {
			aliases[key] = targetCols[i]
		}
	}
}

// alignSetOperationRows renames a SELECT term's result maps to the output
// columns of the left-most term, preserving values by ordinal position.  A
// no-op when the names already match keeps the common same-alias path free of
// extra row-map allocations.
func alignSetOperationRows(rows []Row, sourceCols, targetCols []string) []Row {
	aligned, _ := alignSetOperationRowsWithContext(context.Background(), rows, sourceCols, targetCols)
	return aligned
}

// alignSetOperationRowsWithContext is the execution-path counterpart of
// alignSetOperationRows. Compound SELECT terms can contain millions of rows;
// checking every small batch lets a request deadline stop the otherwise pure
// in-memory remapping work before it allocates another complete result slice.
func alignSetOperationRowsWithContext(ctx context.Context, rows []Row, sourceCols, targetCols []string) ([]Row, error) {
	if sameSetOperationColumns(sourceCols, targetCols) {
		return rows, nil
	}

	aligned := make([]Row, len(rows))
	if len(rows) == 0 {
		return aligned, nil
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	// Output names are fixed for the whole term. Normalize them once rather
	// than allocating the same lower-case aliases for every copied cell.
	sourceKeys := setOperationColumnKeys(sourceCols)
	targetKeys := setOperationColumnKeys(targetCols)
	for rowIdx, row := range rows {
		if rowIdx&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		aligned[rowIdx] = copySetOperationRow(row, sourceKeys, targetKeys)
	}
	return aligned, nil
}

func copySetOperationRow(row Row, sourceKeys, targetKeys []string) Row {
	out := make(Row, len(targetKeys))
	for i, source := range sourceKeys {
		out[targetKeys[i]] = row[source]
	}
	return out
}

func sameSetOperationColumns(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !strings.EqualFold(left[i], right[i]) {
			return false
		}
	}
	return true
}

func exceptRows(leftRows, rightRows []Row, cols []string) []Row {
	result, _ := exceptRowsWithContext(context.Background(), leftRows, rightRows, cols)
	return result
}

func exceptRowsWithContext(ctx context.Context, leftRows, rightRows []Row, cols []string) ([]Row, error) {
	columnKeys := setOperationColumnKeys(cols)
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]struct{}, len(rightRows))
	buf := make([]byte, 0, 64)
	for i, r := range rightRows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		buf = appendSetOperationSignature(buf[:0], r, columnKeys)
		rightSet[string(buf)] = struct{}{}
	}

	// EXCEPT, unlike UNION ALL, is a set operation: remove right-hand matches
	// and collapse duplicate left-hand rows.
	result := make([]Row, 0, len(leftRows))
	seen := make(map[string]struct{}, len(leftRows))
	for i, l := range leftRows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		buf = appendSetOperationSignature(buf[:0], l, columnKeys)
		if _, found := rightSet[string(buf)]; found {
			continue
		}
		if _, found := seen[string(buf)]; found {
			continue
		}
		seen[string(buf)] = struct{}{}
		result = append(result, l)
	}
	return result, nil
}

func intersectRows(leftRows, rightRows []Row, cols []string) []Row {
	result, _ := intersectRowsWithContext(context.Background(), leftRows, rightRows, cols)
	return result
}

func intersectRowsWithContext(ctx context.Context, leftRows, rightRows []Row, cols []string) ([]Row, error) {
	columnKeys := setOperationColumnKeys(cols)
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]struct{}, len(rightRows))
	buf := make([]byte, 0, 64)
	for i, r := range rightRows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		buf = appendSetOperationSignature(buf[:0], r, columnKeys)
		rightSet[string(buf)] = struct{}{}
	}

	// Consume each right-hand key on its first left match. This also removes
	// duplicate left rows, without retaining a second membership set.
	result := make([]Row, 0, min(len(leftRows), len(rightSet)))
	for i, l := range leftRows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		buf = appendSetOperationSignature(buf[:0], l, columnKeys)
		if _, found := rightSet[string(buf)]; !found {
			continue
		}
		delete(rightSet, string(buf))
		result = append(result, l)
	}
	return result, nil
}

// distinctSetOperationRows implements UNION's duplicate elimination. It is
// deliberately separate from SELECT DISTINCT: compound SELECT comparisons use
// SQLite's numeric equality, so INTEGER 1 and REAL 1.0 represent the same set
// member even though their Go representations differ.
func distinctSetOperationRows(rows []Row, cols []string) []Row {
	result, _ := distinctSetOperationRowsWithContext(context.Background(), rows, cols)
	return result
}

func distinctSetOperationRowsWithContext(ctx context.Context, rows []Row, cols []string) ([]Row, error) {
	accumulator := newSetOperationDistinctAccumulator(cols, len(rows))
	if err := accumulator.appendRows(ctx, rows); err != nil {
		return nil, err
	}
	return accumulator.rows, nil
}

// setOperationDistinctAccumulator preserves first-occurrence order and the
// compound SELECT equality rules while accepting multiple terms incrementally.
type setOperationDistinctAccumulator struct {
	columnKeys []string
	seen       map[string]struct{}
	rows       []Row
	key        []byte
}

func newSetOperationDistinctAccumulator(cols []string, capacity int) *setOperationDistinctAccumulator {
	return &setOperationDistinctAccumulator{
		columnKeys: setOperationColumnKeys(cols),
		seen:       make(map[string]struct{}, capacity/2),
		rows:       make([]Row, 0, capacity),
		key:        make([]byte, 0, 64),
	}
}

func (a *setOperationDistinctAccumulator) appendRows(ctx context.Context, rows []Row) error {
	return a.appendWithKeys(ctx, rows, a.columnKeys, false)
}

func (a *setOperationDistinctAccumulator) appendTerm(ctx context.Context, term *ResultSet, targetCols []string) error {
	if len(term.Rows) == 0 || sameSetOperationColumns(term.Cols, targetCols) {
		return a.appendRows(ctx, term.Rows)
	}
	sourceKeys := setOperationColumnKeys(term.Cols)
	if len(sourceKeys) > 1 {
		// The last source column wins if multiple output names lower to the
		// same key. Bind signature positions as though the row was remapped.
		bindings := make(map[string]string, len(sourceKeys))
		for i, key := range a.columnKeys {
			bindings[key] = sourceKeys[i]
		}
		for i, key := range a.columnKeys {
			sourceKeys[i] = bindings[key]
		}
	}
	return a.appendWithKeys(ctx, term.Rows, sourceKeys, true)
}

func (a *setOperationDistinctAccumulator) appendWithKeys(ctx context.Context, rows []Row, sourceKeys []string, rename bool) error {
	for i, row := range rows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return err
			}
		}
		a.key = appendSetOperationSignature(a.key[:0], row, sourceKeys)
		if _, found := a.seen[string(a.key)]; found {
			continue
		}
		a.seen[string(a.key)] = struct{}{}
		if rename {
			// A discarded duplicate needs neither a result map nor alias copies.
			row = copySetOperationRow(row, sourceKeys, a.columnKeys)
		}
		a.rows = append(a.rows, row)
	}
	return nil
}

func setOperationColumnKeys(cols []string) []string {
	keys := make([]string, len(cols))
	for i, col := range cols {
		keys[i] = strings.ToLower(col)
	}
	return keys
}

// appendSetOperationSignature appends a stable key using the equality rules
// used by compound SELECTs. The caller owns and reuses buf across rows, so a
// row that is discarded after a map lookup does not allocate a temporary key.
// Numeric values intentionally share one representation: tinySQL's regular
// comparisons consider int, int64, and float64 equal when they have the same
// numeric value, as does SQLite for UNION/EXCEPT/INTERSECT.
func appendSetOperationSignature(buf []byte, row Row, columnKeys []string) []byte {
	for i, col := range columnKeys {
		if i > 0 {
			buf = append(buf, '|')
		}
		value := row[col]
		if number, ok := numeric(value); ok {
			// Collapse signed zero and NaN payloads as the textual key did.
			bits := math.Float64bits(number)
			switch {
			case number == 0:
				bits = 0
			case math.IsNaN(number):
				bits = math.Float64bits(math.NaN())
			}
			buf = binary.LittleEndian.AppendUint64(append(buf, 'D'), bits)
			continue
		}
		buf = writeFmtKeyPart(buf, value)
	}
	return buf
}

func rowSignature(row Row, cols []string) string {
	buf := make([]byte, 0, 64)
	for i, col := range cols {
		if i > 0 {
			buf = append(buf, '|')
		}
		val, _ := getVal(row, col)
		buf = writeFmtKeyPart(buf, val)
	}
	return string(buf)
}
