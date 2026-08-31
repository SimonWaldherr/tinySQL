// UNION, UNION ALL, EXCEPT and INTERSECT.
package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// setOperationContextBatchRows bounds the amount of pointer copying a compound
// SELECT can do without observing a cancelled request. It matches the cadence
// used by the set-operation materializers below.
const setOperationContextBatchRows = 64

func processUnionClauses(env ExecEnv, union *UnionClause, leftRows []Row, leftCols []string, orderAliases map[string]string) ([]Row, []string, error) {
	resultRows := leftRows
	resultCols := leftCols

	current := union
	for current != nil {
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

		// SQL set operations line up result values by position, while the
		// internal Row representation is keyed by output name.  The left-most
		// SELECT defines the names exposed by the compound result, so remap each
		// right-hand row before appending it or comparing set membership.  Without
		// this, e.g. `SELECT 1 AS a UNION SELECT 1 AS b` compared `a` to a
		// missing key on the right and returned an incorrect NULL-valued row.
		rightRows, err := alignSetOperationRowsWithContext(env.ctx, rightResult.Rows, rightResult.Cols, resultCols)
		if err != nil {
			return nil, nil, err
		}
		addCompoundOrderAliases(orderAliases, rightResult.Cols, resultCols)

		// Process the union based on type
		switch current.Type {
		case UnionAll:
			// UNION ALL: Just append all rows
			resultRows, err = appendRowsWithContext(env.ctx, resultRows, rightRows)
			if err != nil {
				return nil, nil, err
			}

		case UnionDistinct:
			// UNION: Append and then remove duplicates
			resultRows, err = appendRowsWithContext(env.ctx, resultRows, rightRows)
			if err != nil {
				return nil, nil, err
			}
			resultRows, err = distinctSetOperationRowsWithContext(env.ctx, resultRows, resultCols)
			if err != nil {
				return nil, nil, err
			}

		case Except:
			// EXCEPT: Remove rows that exist in the right result
			resultRows, err = exceptRowsWithContext(env.ctx, resultRows, rightRows, resultCols)
			if err != nil {
				return nil, nil, err
			}

		case Intersect:
			// INTERSECT: Keep only rows that exist in both results
			resultRows, err = intersectRowsWithContext(env.ctx, resultRows, rightRows, resultCols)
			if err != nil {
				return nil, nil, err
			}
		}

		current = current.Next
	}

	return resultRows, resultCols, nil
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
	for rowIdx, row := range rows {
		if rowIdx&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		out := make(Row, len(targetCols))
		for colIdx, sourceCol := range sourceCols {
			value, _ := getVal(row, sourceCol)
			putVal(out, targetCols[colIdx], value)
		}
		aligned[rowIdx] = out
	}
	return aligned, nil
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

	// Keep only left rows that are also in the right set
	result := make([]Row, 0, len(leftRows))
	seen := make(map[string]struct{}, len(leftRows))
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
		if _, found := seen[string(buf)]; found {
			continue
		}
		seen[string(buf)] = struct{}{}
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
	columnKeys := setOperationColumnKeys(cols)
	seen := make(map[string]struct{}, len(rows)/2)
	result := make([]Row, 0, len(rows))
	buf := make([]byte, 0, 64)
	for i, row := range rows {
		if i&(setOperationContextBatchRows-1) == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		buf = appendSetOperationSignature(buf[:0], row, columnKeys)
		if _, found := seen[string(buf)]; found {
			continue
		}
		seen[string(buf)] = struct{}{}
		result = append(result, row)
	}
	return result, nil
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
			// Normalize negative zero as compare treats -0.0 and 0.0 as equal.
			if number == 0 {
				number = 0
			}
			buf = append(buf, 'N')
			buf = strconv.AppendFloat(buf, number, 'g', -1, 64)
			buf = append(buf, ';')
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
