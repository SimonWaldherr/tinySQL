package engine

import "strings"

// Performance optimizations for tinySQL

// HashJoinOptimizer provides hash-based join optimizations
type HashJoinOptimizer struct {
	env ExecEnv
}

// OptimizedJoinType defines the type of join operation for optimizations
type OptimizedJoinType int

const (
	OptimizedJoinTypeInner OptimizedJoinType = iota
	OptimizedJoinTypeLeft
	OptimizedJoinTypeRight
)

// JoinCondition represents a parsed join condition
type JoinCondition struct {
	LeftColumn  string
	RightColumn string
	Operator    string
	IsEquiJoin  bool
}

// OptimizedJoinProcessor handles optimized join operations
func (h *HashJoinOptimizer) ProcessOptimizedJoin(leftRows, rightRows []Row, condition Expr, joinType OptimizedJoinType) ([]Row, error) {
	if condition == nil {
		// Cross join - use existing nested loop
		return h.processCrossJoin(leftRows, rightRows, joinType)
	}

	// Try to extract equi-join condition
	joinCond := h.extractJoinCondition(condition)
	if joinCond != nil && joinCond.IsEquiJoin {
		return h.processHashJoin(leftRows, rightRows, joinCond, joinType)
	}

	// Fall back to nested loop for complex conditions
	return h.processNestedLoopJoin(leftRows, rightRows, condition, joinType)
}

// extractJoinCondition tries to extract a simple equi-join condition
func (h *HashJoinOptimizer) extractJoinCondition(expr Expr) *JoinCondition {
	if binary, ok := expr.(*Binary); ok && binary.Op == "=" {
		leftVar, leftOk := binary.Left.(*VarRef)
		rightVar, rightOk := binary.Right.(*VarRef)

		if leftOk && rightOk {
			return &JoinCondition{
				LeftColumn:  leftVar.Name,
				RightColumn: rightVar.Name,
				Operator:    "=",
				IsEquiJoin:  true,
			}
		}
	}
	return nil
}

// processHashJoin implements hash join for equi-join conditions
func (h *HashJoinOptimizer) processHashJoin(leftRows, rightRows []Row, condition *JoinCondition, joinType OptimizedJoinType) ([]Row, error) {
	// For LEFT JOIN, always probe with left rows so unmatched left rows can be detected.
	// For INNER JOIN, build from the smaller relation to minimise hash table size.
	var buildRows, probeRows []Row
	var buildColumn, probeColumn string

	if joinType == OptimizedJoinTypeLeft || len(leftRows) > len(rightRows) {
		// probe = left, build = right
		buildRows = rightRows
		probeRows = leftRows
		buildColumn = condition.RightColumn
		probeColumn = condition.LeftColumn
	} else {
		// probe = right, build = left (inner join, left is smaller)
		buildRows = leftRows
		probeRows = rightRows
		buildColumn = condition.LeftColumn
		probeColumn = condition.RightColumn
	}

	// Build hash table
	hashTable := make(map[any][]Row)
	for _, row := range buildRows {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}
		key := h.getJoinKey(row, buildColumn)
		if key != nil {
			hashTable[key] = append(hashTable[key], row)
		}
	}

	// Probe phase
	estimatedSize := len(probeRows)
	if joinType == OptimizedJoinTypeInner {
		estimatedSize = min(len(leftRows), len(rightRows))
	}
	result := make([]Row, 0, estimatedSize)

	for _, probeRow := range probeRows {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}

		key := h.getJoinKey(probeRow, probeColumn)
		matched := false

		if key != nil {
			if matchingRows, exists := hashTable[key]; exists {
				for _, buildRow := range matchingRows {
					// probeRow is always left when joinType == Left, or right when Inner+right>left
					var mergedRow Row
					if joinType == OptimizedJoinTypeLeft || len(leftRows) > len(rightRows) {
						mergedRow = mergeRows(probeRow, buildRow) // left=probe, right=build
					} else {
						mergedRow = mergeRows(buildRow, probeRow) // left=build, right=probe
					}
					result = append(result, mergedRow)
					matched = true
				}
			}
		}

		// For LEFT JOIN, emit unmatched left (probe) rows with null right columns.
		if !matched && joinType == OptimizedJoinTypeLeft {
			if len(buildRows) > 0 {
				result = append(result, h.addNullsForUnmatchedJoin(probeRow, buildRows[0]))
			} else {
				result = append(result, cloneRow(probeRow))
			}
		}
	}

	return result, nil
}

// getJoinKey extracts the join key from a row
func (h *HashJoinOptimizer) getJoinKey(row Row, column string) any {
	// Try different column name formats
	if val, exists := row[strings.ToLower(column)]; exists {
		return val
	}
	if val, exists := row[column]; exists {
		return val
	}

	// Try qualified names
	for key, val := range row {
		if strings.HasSuffix(strings.ToLower(key), "."+strings.ToLower(column)) {
			return val
		}
	}

	return nil
}

// addNullsForUnmatchedJoin creates a row with nulls for unmatched joins
func (h *HashJoinOptimizer) addNullsForUnmatchedJoin(matchedRow, sampleUnmatchedRow Row) Row {
	result := cloneRow(matchedRow)

	// Add null values for columns from the unmatched side
	for key := range sampleUnmatchedRow {
		if _, exists := result[key]; !exists {
			result[key] = nil
		}
	}

	return result
}

// processCrossJoin handles cross joins (cartesian product)
func (h *HashJoinOptimizer) processCrossJoin(leftRows, rightRows []Row, joinType OptimizedJoinType) ([]Row, error) {
	result := make([]Row, 0, len(leftRows)*len(rightRows))

	for _, leftRow := range leftRows {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}

		for _, rightRow := range rightRows {
			merged := mergeRows(leftRow, rightRow)
			result = append(result, merged)
		}
	}

	return result, nil
}

// processNestedLoopJoin handles complex join conditions that can't be optimized
func (h *HashJoinOptimizer) processNestedLoopJoin(leftRows, rightRows []Row, condition Expr, joinType OptimizedJoinType) ([]Row, error) {
	result := make([]Row, 0, len(leftRows))

	for _, leftRow := range leftRows {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}

		matched := false
		for _, rightRow := range rightRows {
			merged := mergeRows(leftRow, rightRow)

			shouldInclude := true
			if condition != nil {
				val, err := evalExpr(h.env, condition, merged)
				if err != nil {
					return nil, err
				}
				shouldInclude = (toTri(val) == tvTrue)
			}

			if shouldInclude {
				result = append(result, merged)
				matched = true
			}
		}

		// Handle LEFT JOIN unmatched rows
		if !matched && joinType == OptimizedJoinTypeLeft && len(rightRows) > 0 {
			nullRow := h.addNullsForUnmatchedJoin(leftRow, rightRows[0])
			result = append(result, nullRow)
		}
	}

	return result, nil
}
