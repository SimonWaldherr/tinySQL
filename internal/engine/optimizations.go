package engine

import (
	"fmt"
	"strings"
)

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
	// Build hash table from the smaller relation
	var buildRows, probeRows []Row
	var buildColumn, probeColumn string

	if len(leftRows) <= len(rightRows) {
		buildRows = leftRows
		probeRows = rightRows
		buildColumn = condition.LeftColumn
		probeColumn = condition.RightColumn
	} else {
		buildRows = rightRows
		probeRows = leftRows
		buildColumn = condition.RightColumn
		probeColumn = condition.LeftColumn
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
	var result []Row
	estimatedSize := len(probeRows)
	if joinType == OptimizedJoinTypeInner {
		estimatedSize = min(len(leftRows), len(rightRows))
	}
	result = make([]Row, 0, estimatedSize)

	for _, probeRow := range probeRows {
		if err := checkCtx(h.env.ctx); err != nil {
			return nil, err
		}

		key := h.getJoinKey(probeRow, probeColumn)
		matched := false

		if key != nil {
			if matchingRows, exists := hashTable[key]; exists {
				for _, buildRow := range matchingRows {
					var mergedRow Row
					if len(leftRows) <= len(rightRows) {
						mergedRow = mergeRows(buildRow, probeRow)
					} else {
						mergedRow = mergeRows(probeRow, buildRow)
					}
					result = append(result, mergedRow)
					matched = true
				}
			}
		}

		// Handle LEFT JOIN unmatched rows
		if !matched && joinType == OptimizedJoinTypeLeft {
			if len(leftRows) <= len(rightRows) {
				// probeRow is from right table, need to add nulls for left
				result = append(result, h.addNullsForUnmatchedJoin(probeRow, buildRows[0]))
			} else {
				// probeRow is from left table, need to add nulls for right
				result = append(result, h.addNullsForUnmatchedJoin(probeRow, buildRows[0]))
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

// ColumnIndex provides fast column lookups for better performance
type ColumnIndex struct {
	columnValues map[string]map[any][]int // column -> value -> row indices
	rows         []Row
}

// NewColumnIndex creates an index for the given rows and columns
func NewColumnIndex(rows []Row, columns []string) *ColumnIndex {
	index := &ColumnIndex{
		columnValues: make(map[string]map[any][]int),
		rows:         rows,
	}

	for _, column := range columns {
		index.columnValues[column] = make(map[any][]int)

		for i, row := range rows {
			value := index.getColumnValue(row, column)
			if value != nil {
				index.columnValues[column][value] = append(index.columnValues[column][value], i)
			}
		}
	}

	return index
}

// getColumnValue extracts column value from row with fallback logic
func (idx *ColumnIndex) getColumnValue(row Row, column string) any {
	// Try direct lookup
	if val, exists := row[column]; exists {
		return val
	}

	// Try lowercase
	if val, exists := row[strings.ToLower(column)]; exists {
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

// Lookup returns row indices that match the given column value
func (idx *ColumnIndex) Lookup(column string, value any) []int {
	if columnMap, exists := idx.columnValues[column]; exists {
		return columnMap[value]
	}
	return nil
}

// GetRows returns the rows at the given indices
func (idx *ColumnIndex) GetRows(indices []int) []Row {
	result := make([]Row, len(indices))
	for i, rowIdx := range indices {
		result[i] = idx.rows[rowIdx]
	}
	return result
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ExpressionCache provides caching for frequently evaluated expressions
type ExpressionCache struct {
	cache  map[string]any
	hits   int
	misses int
}

// NewExpressionCache creates a new expression cache
func NewExpressionCache() *ExpressionCache {
	return &ExpressionCache{
		cache: make(map[string]any),
	}
}

// Get retrieves a cached result for the given expression and row
func (ec *ExpressionCache) Get(exprStr string, row Row) (any, bool) {
	key := ec.generateKey(exprStr, row)
	if val, exists := ec.cache[key]; exists {
		ec.hits++
		return val, true
	}
	ec.misses++
	return nil, false
}

// Set stores a result in the cache
func (ec *ExpressionCache) Set(exprStr string, row Row, result any) {
	key := ec.generateKey(exprStr, row)
	ec.cache[key] = result
}

// generateKey creates a cache key from expression and row values
func (ec *ExpressionCache) generateKey(exprStr string, row Row) string {
	var parts []string
	parts = append(parts, exprStr)

	// Add relevant row values (simplified for performance)
	for k, v := range row {
		if v != nil {
			parts = append(parts, fmt.Sprintf("%s:%v", k, v))
		}
	}

	return strings.Join(parts, "|")
}

// Stats returns cache performance statistics
func (ec *ExpressionCache) Stats() (hits, misses int, hitRate float64) {
	total := ec.hits + ec.misses
	var hitRate64 float64
	if total > 0 {
		hitRate64 = float64(ec.hits) / float64(total)
	}
	return ec.hits, ec.misses, hitRate64
}

// Clear empties the cache
func (ec *ExpressionCache) Clear() {
	ec.cache = make(map[string]any)
	ec.hits = 0
	ec.misses = 0
}
