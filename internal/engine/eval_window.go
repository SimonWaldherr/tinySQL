// Window functions: LAG, LEAD, FIRST_VALUE, LAST_VALUE, moving aggregates,
// RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST and NTILE, plus the partitioning
// and frame arithmetic they share.
package engine

import (
	"fmt"
	"sort"
	"strings"
)

func columnsFromRows(rows []Row) []string {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	cols := make([]string, 0, len(rows[0]))
	for _, row := range rows {
		for k := range row {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

// projName generates the column name for a select item
func projName(it SelectItem, idx int) string {
	if it.Alias != "" {
		return it.Alias
	}
	if ref, ok := it.Expr.(*VarRef); ok {
		return ref.Name
	}
	return fmt.Sprintf("col_%d", idx)
}

// anyAggInSelect checks if any select item contains an aggregate function
func anyAggInSelect(items []SelectItem) bool {
	for _, it := range items {
		if isAggregate(it.Expr) {
			return true
		}
	}
	return false
}

// anyWindowInSelect checks if any window functions are used in SELECT projections
func anyWindowInSelect(items []SelectItem) bool {
	for _, it := range items {
		if hasWindowFunction(it.Expr) {
			return true
		}
	}
	return false
}

// hasWindowFunction checks if an expression contains a window function
func hasWindowFunction(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		if ex.Over != nil {
			return true
		}
		// Check arguments recursively
		for _, arg := range ex.Args {
			if hasWindowFunction(arg) {
				return true
			}
		}
	case *Unary:
		return hasWindowFunction(ex.Expr)
	case *Binary:
		return hasWindowFunction(ex.Left) || hasWindowFunction(ex.Right)
	case *IsNull:
		return hasWindowFunction(ex.Expr)
	case *CaseExpr:
		if ex.Operand != nil && hasWindowFunction(ex.Operand) {
			return true
		}
		for _, w := range ex.Whens {
			if hasWindowFunction(w.When) || hasWindowFunction(w.Then) {
				return true
			}
		}
		if ex.Else != nil && hasWindowFunction(ex.Else) {
			return true
		}
	}
	return false
}

// extractWindowOffset extracts and validates offset from window function arguments
func extractWindowOffset(env ExecEnv, args []Expr, row Row, defaultOffset int) (int, error) {
	if len(args) <= 1 {
		return defaultOffset, nil
	}
	offsetVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return 0, err
	}
	if offsetInt, ok := offsetVal.(int); ok {
		return offsetInt, nil
	}
	if offsetFloat, ok := offsetVal.(float64); ok {
		return int(offsetFloat), nil
	}
	return defaultOffset, nil
}

// evalLagFunction evaluates the LAG window function
func evalLagFunction(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	offset, err := extractWindowOffset(env, ex.Args, row, 1)
	if err != nil {
		return nil, err
	}
	lagIdx := currentIdx - offset
	if lagIdx < 0 || lagIdx >= len(partitionRows) {
		// Return default value if provided
		if len(ex.Args) > 2 {
			return evalExpr(env, ex.Args[2], row)
		}
		return nil, nil
	}
	return evalExpr(env, ex.Args[0], partitionRows[lagIdx])
}

// evalLeadFunction evaluates the LEAD window function
func evalLeadFunction(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	offset, err := extractWindowOffset(env, ex.Args, row, 1)
	if err != nil {
		return nil, err
	}
	leadIdx := currentIdx + offset
	if leadIdx < 0 || leadIdx >= len(partitionRows) {
		// Return default value if provided
		if len(ex.Args) > 2 {
			return evalExpr(env, ex.Args[2], row)
		}
		return nil, nil
	}
	return evalExpr(env, ex.Args[0], partitionRows[leadIdx])
}

// evalFirstValue evaluates the FIRST_VALUE window function
func evalFirstValue(env ExecEnv, ex *FuncCall, partitionRows []Row) (any, error) {
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("FIRST_VALUE requires an argument")
	}
	if len(partitionRows) == 0 {
		return nil, nil
	}
	return evalExpr(env, ex.Args[0], partitionRows[0])
}

// evalLastValue evaluates the LAST_VALUE window function
func evalLastValue(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int) (any, error) {
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("LAST_VALUE requires an argument")
	}
	if len(partitionRows) == 0 {
		return nil, nil
	}
	// Use frame end if specified
	endIdx := len(partitionRows) - 1
	if ex.Over.Frame != nil {
		endIdx = calculateFrameEnd(currentIdx, len(partitionRows), ex.Over.Frame)
	}
	return evalExpr(env, ex.Args[0], partitionRows[endIdx])
}

// evalMovingAggregate evaluates MOVING_SUM and MOVING_AVG window functions
func evalMovingAggregate(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	// Get window size from first argument
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("%s requires window size argument", ex.Name)
	}
	sizeVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	var windowSize int
	if sizeInt, ok := sizeVal.(int); ok {
		windowSize = sizeInt
	} else if sizeFloat, ok := sizeVal.(float64); ok {
		windowSize = int(sizeFloat)
	}

	// Calculate start of window
	startIdx := currentIdx - windowSize + 1
	if startIdx < 0 {
		startIdx = 0
	}

	// Get value expression (second argument if provided, otherwise assume column)
	var valueExpr Expr
	if len(ex.Args) > 1 {
		valueExpr = ex.Args[1]
	} else {
		// Use ORDER BY column as default
		if len(ex.Over.OrderBy) > 0 {
			valueExpr = newVarRef(ex.Over.OrderBy[0].Col)
		} else {
			return nil, fmt.Errorf("%s requires value expression", ex.Name)
		}
	}

	// Calculate sum over window
	var sum float64
	count := 0
	for i := startIdx; i <= currentIdx && i < len(partitionRows); i++ {
		val, err := evalExpr(env, valueExpr, partitionRows[i])
		if err != nil {
			return nil, err
		}
		if val != nil {
			if valFloat, ok := val.(float64); ok {
				sum += valFloat
			} else if valInt, ok := val.(int); ok {
				sum += float64(valInt)
			}
			count++
		}
	}

	if ex.Name == "MOVING_SUM" {
		return sum, nil
	}
	// MOVING_AVG
	if count == 0 {
		return nil, nil
	}
	return sum / float64(count), nil
}

// evalWindowFunction evaluates a window function with OVER clause
func evalWindowFunction(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Over == nil {
		return nil, fmt.Errorf("window function %s requires OVER clause", ex.Name)
	}

	// Get all rows for this window
	allRows := env.windowRows
	if allRows == nil {
		return nil, fmt.Errorf("window function context not available")
	}

	// Apply PARTITION BY to get relevant partition
	partitionRows := allRows
	if len(ex.Over.PartitionBy) > 0 {
		partitionRows = filterPartition(env, allRows, ex.Over.PartitionBy, row)
	}

	// Apply ORDER BY to partition
	if len(ex.Over.OrderBy) > 0 {
		partitionRows = sortRows(partitionRows, ex.Over.OrderBy)
	}

	// Find current row position in partition
	currentIdx := findRowIndex(partitionRows, row, env.windowIndex)

	// Evaluate the specific window function
	switch ex.Name {
	case "ROW_NUMBER":
		return currentIdx + 1, nil
	case "RANK":
		return evalRankFunction(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "DENSE_RANK":
		return evalDenseRankFunction(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "PERCENT_RANK":
		return evalPercentRank(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "CUME_DIST":
		return evalCumeDist(partitionRows, currentIdx, ex.Over.OrderBy), nil
	case "NTILE":
		return evalNtile(env, ex, partitionRows, currentIdx, row)
	case "EQUAL_INTERVAL":
		return evalEqualInterval(env, ex, partitionRows, currentIdx, row)
	case "NATURAL_BREAKS":
		return evalNaturalBreaks(env, ex, partitionRows, currentIdx, row)
	case "LAG":
		return evalLagFunction(env, ex, partitionRows, currentIdx, row)
	case "LEAD":
		return evalLeadFunction(env, ex, partitionRows, currentIdx, row)
	case "FIRST_VALUE":
		return evalFirstValue(env, ex, partitionRows)
	case "LAST_VALUE":
		return evalLastValue(env, ex, partitionRows, currentIdx)
	case "MOVING_SUM", "MOVING_AVG":
		return evalMovingAggregate(env, ex, partitionRows, currentIdx, row)
	default:
		return nil, fmt.Errorf("unsupported window function: %s", ex.Name)
	}
}

// rowsOrderTie reports whether a and b have identical values for every
// ORDER BY column — the sort direction is irrelevant for tie detection,
// only equality matters.
func rowsOrderTie(a, b Row, orderBy []OrderItem) bool {
	for _, oi := range orderBy {
		col := strings.ToLower(oi.Col)
		av, _ := getValLower(a, col)
		bv, _ := getValLower(b, col)
		if compareForOrder(av, bv, false) != 0 {
			return false
		}
	}
	return true
}

// evalRankFunction computes SQL RANK(): tied rows (identical ORDER BY key)
// share the same rank, and the rank after a tie group skips ahead by the
// group's size (e.g. 1, 1, 3, 4, 4, 6). partitionRows must already be
// sorted by orderBy (evalWindowFunction guarantees this).
func evalRankFunction(partitionRows []Row, currentIdx int, orderBy []OrderItem) int {
	if len(orderBy) == 0 {
		return currentIdx + 1
	}
	i := currentIdx
	for i > 0 && rowsOrderTie(partitionRows[i-1], partitionRows[currentIdx], orderBy) {
		i--
	}
	return i + 1
}

// evalDenseRankFunction computes SQL DENSE_RANK(): like RANK but without
// gaps after ties (e.g. 1, 1, 2, 3, 3, 4).
func evalDenseRankFunction(partitionRows []Row, currentIdx int, orderBy []OrderItem) int {
	if len(orderBy) == 0 {
		return currentIdx + 1
	}
	rank := 1
	for i := 1; i <= currentIdx; i++ {
		if !rowsOrderTie(partitionRows[i-1], partitionRows[i], orderBy) {
			rank++
		}
	}
	return rank
}

// evalPercentRank computes SQL PERCENT_RANK(): (RANK - 1) / (partition size - 1),
// or 0 when the partition has only one row.
func evalPercentRank(partitionRows []Row, currentIdx int, orderBy []OrderItem) float64 {
	total := len(partitionRows)
	if total <= 1 {
		return 0
	}
	rank := evalRankFunction(partitionRows, currentIdx, orderBy)
	return float64(rank-1) / float64(total-1)
}

// evalCumeDist computes SQL CUME_DIST(): the fraction of partition rows
// whose ORDER BY key is less than or equal to the current row's key. All
// rows in the same tie group get the same value (the group's last position).
func evalCumeDist(partitionRows []Row, currentIdx int, orderBy []OrderItem) float64 {
	total := len(partitionRows)
	if total == 0 {
		return 0
	}
	i := currentIdx
	for i+1 < total && rowsOrderTie(partitionRows[i+1], partitionRows[currentIdx], orderBy) {
		i++
	}
	return float64(i+1) / float64(total)
}

// evalNtile computes SQL NTILE(n): divides the partition into n
// (approximately) equal-sized buckets and returns the 1-based bucket number
// for the current row. When the partition size doesn't divide evenly, the
// first (size % n) buckets get one extra row each — matching PostgreSQL's
// NTILE bucketing.
func evalNtile(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("NTILE expects 1 argument")
	}
	nVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	n, err := toInt(nVal)
	if err != nil {
		return nil, fmt.Errorf("NTILE: %w", err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("NTILE argument must be positive, got %d", n)
	}
	total := len(partitionRows)
	base := total / n
	remainder := total % n
	boundary := remainder * (base + 1)
	if currentIdx < boundary {
		return currentIdx/(base+1) + 1, nil
	}
	if base == 0 {
		// n exceeds the partition size; every remaining row is its own
		// bucket, continuing on from the (remainder) buckets already used.
		return remainder + (currentIdx - boundary) + 1, nil
	}
	return remainder + (currentIdx-boundary)/base + 1, nil
}

// filterPartition returns rows that match the partition of the current row
func filterPartition(env ExecEnv, allRows []Row, partitionBy []Expr, currentRow Row) []Row {
	// Evaluate partition expressions for current row
	currentPartition := make([]any, len(partitionBy))
	for i, expr := range partitionBy {
		val, err := evalExpr(env, expr, currentRow)
		if err != nil {
			continue
		}
		currentPartition[i] = val
	}

	// Filter rows with same partition values
	var result []Row
	for _, row := range allRows {
		match := true
		for i, expr := range partitionBy {
			val, err := evalExpr(env, expr, row)
			cmp, cmpErr := compare(val, currentPartition[i])
			if err != nil || cmpErr != nil || cmp != 0 {
				match = false
				break
			}
		}
		if match {
			result = append(result, row)
		}
	}
	return result
}

// sortRows sorts rows according to ORDER BY items
// sortRows returns a sorted copy of rows (used for window-function partition
// ordering, which must not mutate the caller's slice). Like applySortOrder,
// sort keys are extracted once per row instead of re-looked-up from the row
// map on every comparator call.
func sortRows(rows []Row, orderBy []OrderItem) []Row {
	sorted := make([]Row, len(rows))
	copy(sorted, rows)
	if len(orderBy) == 0 || len(sorted) <= 1 {
		return sorted
	}
	lcOrdCols := make([]string, len(orderBy))
	for i, oi := range orderBy {
		lcOrdCols[i] = strings.ToLower(oi.Col)
	}
	items := make([]orderedValueRow, len(sorted))
	for i, row := range sorted {
		items[i] = buildOrderByValues(row, lcOrdCols)
	}
	sort.SliceStable(items, func(i, j int) bool {
		return compareOrderedValueRows(orderBy, items[i], items[j]) < 0
	})
	for i, item := range items {
		sorted[i] = item.row
	}
	return sorted
}

// findRowIndex finds the index of the current row in the partition
func findRowIndex(partitionRows []Row, currentRow Row, hint int) int {
	// Try hint first (optimization)
	if hint >= 0 && hint < len(partitionRows) {
		if rowsEqual(partitionRows[hint], currentRow) {
			return hint
		}
	}

	// Linear search
	for i, row := range partitionRows {
		if rowsEqual(row, currentRow) {
			return i
		}
	}
	return 0
}

// rowsEqual checks if two rows are equal (same values for all columns)
func rowsEqual(a, b Row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		cmp, err := compare(v, b[k])
		if err != nil || cmp != 0 {
			return false
		}
	}
	return true
}

// calculateFrameEnd calculates the end index for frame-based window functions
func calculateFrameEnd(currentIdx, totalRows int, frame *WindowFrame) int {
	if frame == nil {
		return currentIdx // Default: CURRENT ROW
	}

	switch frame.EndType {
	case "CURRENT":
		return currentIdx
	case "UNBOUNDED_FOLLOWING":
		return totalRows - 1
	case "OFFSET_FOLLOWING":
		endIdx := currentIdx + frame.EndValue
		if endIdx >= totalRows {
			return totalRows - 1
		}
		return endIdx
	case "OFFSET_PRECEDING":
		endIdx := currentIdx - frame.EndValue
		if endIdx < 0 {
			return 0
		}
		return endIdx
	default:
		return currentIdx
	}
}
