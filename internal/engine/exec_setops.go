// UNION, UNION ALL, EXCEPT and INTERSECT.
package engine

import (
	"fmt"
)

func processUnionClauses(env ExecEnv, union *UnionClause, leftRows []Row, leftCols []string) ([]Row, []string, error) {
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

		// Process the union based on type
		switch current.Type {
		case UnionAll:
			// UNION ALL: Just append all rows
			resultRows = append(resultRows, rightResult.Rows...)

		case UnionDistinct:
			// UNION: Append and then remove duplicates
			resultRows = append(resultRows, rightResult.Rows...)
			resultRows = distinctRows(resultRows, resultCols)

		case Except:
			// EXCEPT: Remove rows that exist in the right result
			resultRows = exceptRows(resultRows, rightResult.Rows, resultCols)

		case Intersect:
			// INTERSECT: Keep only rows that exist in both results
			resultRows = intersectRows(resultRows, rightResult.Rows, resultCols)
		}

		current = current.Next
	}

	return resultRows, resultCols, nil
}

func exceptRows(leftRows, rightRows []Row, cols []string) []Row {
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]bool)
	for _, r := range rightRows {
		key := rowSignature(r, cols)
		rightSet[key] = true
	}

	// Keep only left rows that are not in the right set
	var result []Row
	for _, l := range leftRows {
		key := rowSignature(l, cols)
		if !rightSet[key] {
			result = append(result, l)
		}
	}
	return result
}

func intersectRows(leftRows, rightRows []Row, cols []string) []Row {
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]bool)
	for _, r := range rightRows {
		key := rowSignature(r, cols)
		rightSet[key] = true
	}

	// Keep only left rows that are also in the right set
	var result []Row
	seen := make(map[string]bool)
	for _, l := range leftRows {
		key := rowSignature(l, cols)
		if rightSet[key] && !seen[key] {
			result = append(result, l)
			seen[key] = true
		}
	}
	return result
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
