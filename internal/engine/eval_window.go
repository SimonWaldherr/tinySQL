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

	// Partition + sort the window's rows, and locate the current row within
	// that partition. See windowPartitionCache: this is memoized per distinct
	// PARTITION BY key so a query with N output rows across P partitions does
	// the O(N)-filter + O(N log N)-sort work P times, not N times.
	partitionRows, currentIdx := resolveWindowPartition(env, ex, allRows, row)

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

// windowPartitionCache memoizes, per window-function call site (the specific
// *FuncCall AST node for one OVER clause in the SELECT list -- stable across
// every output row of one query) and PARTITION BY key, the partitioned+
// ordered row set together with a lookup from a row's position in
// env.windowRows (the query's full, unpartitioned row set -- what
// env.windowIndex holds for the row currently being evaluated) to that row's
// position within the partition.
//
// evalWindowFunction runs once per output row -- that is correct SQL
// semantics, RANK/LAG/etc. can differ row to row -- but building the
// partition (filterPartition's O(N) scan, plus sortRows's O(N log N) sort
// when the OVER clause has an ORDER BY) only needs to happen once per
// distinct partition. Without this cache, a query with P partitions pays for
// that build P times too many: once per row instead of once per partition,
// turning an O(N log N) query into O(N^2 log N). This mirrors the memoization
// eval_window_classify.go's natBreaksCache uses for NATURAL_BREAKS's DP, but
// scoped to one query execution (a fresh cache per processNonAggregateQuery
// call, via ExecEnv.windowPartitions) rather than a package-level cache:
// this cache holds full Row slices, so keeping it alive across separate query
// executions would grow unbounded and risk serving stale rows for a reused
// *FuncCall (e.g. via tinysql.ExecuteCompiled).
type windowPartitionCache struct {
	entries map[windowPartitionCacheKey]*windowPartitionEntry
}

type windowPartitionCacheKey struct {
	fn        *FuncCall
	partition string
}

type windowPartitionEntry struct {
	rows []Row
	// posByOrigIdx maps a row's index within env.windowRows to that row's
	// position within rows (post partition-filter, post sort). Built once
	// per partition (see buildWindowPartition) so evalWindowFunction can
	// locate the current row in O(1) via env.windowIndex instead of
	// findRowIndex's O(P) rowsEqual scan.
	posByOrigIdx map[int]int
}

func newWindowPartitionCache() *windowPartitionCache {
	return &windowPartitionCache{entries: make(map[windowPartitionCacheKey]*windowPartitionEntry)}
}

// resolveWindowPartition returns the partitioned+ordered row set for one
// window-function evaluation and the current row's index within it, building
// that row set at most once per distinct partition (see windowPartitionCache)
// instead of once per output row.
func resolveWindowPartition(env ExecEnv, ex *FuncCall, allRows []Row, row Row) ([]Row, int) {
	cache := env.windowPartitions
	if cache == nil {
		// Defensive fallback: windowRows was set up without a cache (not
		// reachable via the normal exec_group.go path, but this function
		// stays correct on its own rather than trusting a cross-file
		// invariant). Same behavior as before this cache existed.
		partitionRows := allRows
		if len(ex.Over.PartitionBy) > 0 {
			partitionRows = filterPartition(env, allRows, ex.Over.PartitionBy, row)
		}
		if len(ex.Over.OrderBy) > 0 {
			partitionRows = sortRows(partitionRows, ex.Over.OrderBy)
		}
		return partitionRows, findRowIndex(partitionRows, row, env.windowIndex)
	}

	key := windowPartitionCacheKey{fn: ex, partition: formatWindowPartitionKey(env, ex.Over.PartitionBy, row)}
	entry, ok := cache.entries[key]
	if !ok {
		entry = buildWindowPartition(env, ex, allRows, row)
		cache.entries[key] = entry
	}

	if pos, ok := entry.posByOrigIdx[env.windowIndex]; ok {
		return entry.rows, pos
	}
	// env.windowIndex didn't resolve to a position (e.g. an unset/invalid
	// hint) -- fall back to the same content-equality scan evalWindowFunction
	// always used, so correctness never depends on the hint being right.
	return entry.rows, findRowIndex(entry.rows, row, -1)
}

// buildWindowPartition filters allRows down to currentRow's partition,
// applies the OVER clause's ORDER BY (if any), and records each surviving
// row's original index (its position in allRows, i.e. env.windowRows) so its
// position within the partition can be looked up in O(1) later.
func buildWindowPartition(env ExecEnv, ex *FuncCall, allRows []Row, currentRow Row) *windowPartitionEntry {
	var rows []Row
	var origIdx []int
	if len(ex.Over.PartitionBy) > 0 {
		rows, origIdx = filterPartitionIndexed(env, allRows, ex.Over.PartitionBy, currentRow)
	} else {
		rows = allRows
		origIdx = make([]int, len(allRows))
		for i := range origIdx {
			origIdx[i] = i
		}
	}

	if len(ex.Over.OrderBy) > 0 {
		rows, origIdx = sortRowsIndexed(rows, origIdx, ex.Over.OrderBy)
	}

	posByOrigIdx := make(map[int]int, len(origIdx))
	for pos, oi := range origIdx {
		posByOrigIdx[oi] = pos
	}
	return &windowPartitionEntry{rows: rows, posByOrigIdx: posByOrigIdx}
}

// filterPartitionIndexed is filterPartition plus, for each surviving row,
// that row's index within allRows -- the same indexing env.windowIndex uses
// -- so the caller can build an O(1) orig-index -> partition-position lookup
// instead of a per-row rowsEqual scan (see windowPartitionCache).
func filterPartitionIndexed(env ExecEnv, allRows []Row, partitionBy []Expr, currentRow Row) ([]Row, []int) {
	// Evaluate partition expressions for current row
	currentPartition := make([]any, len(partitionBy))
	for i, expr := range partitionBy {
		val, err := evalExpr(env, expr, currentRow)
		if err != nil {
			continue
		}
		currentPartition[i] = val
	}

	// Filter rows with same partition values, keeping their original index
	var rows []Row
	var idx []int
	for i, r := range allRows {
		match := true
		for pi, expr := range partitionBy {
			val, err := evalExpr(env, expr, r)
			cmp, cmpErr := compare(val, currentPartition[pi])
			if err != nil || cmpErr != nil || cmp != 0 {
				match = false
				break
			}
		}
		if match {
			rows = append(rows, r)
			idx = append(idx, i)
		}
	}
	return rows, idx
}

// orderedValuePermAsc sorts a permutation of indices into items rather than
// items itself, so the caller can recover each row's original position.
// A concrete Swap (on the []int permutation) sidesteps sort.Slice's
// reflect.Swapper, same rationale as orderedValueRowsAsc in exec_sort.go.
type orderedValuePermAsc struct {
	orderBy []OrderItem
	items   []orderedValueRow
	perm    []int
}

func (s orderedValuePermAsc) Len() int { return len(s.perm) }
func (s orderedValuePermAsc) Less(i, j int) bool {
	return compareOrderedValueRows(s.orderBy, s.items[s.perm[i]], s.items[s.perm[j]]) < 0
}
func (s orderedValuePermAsc) Swap(i, j int) { s.perm[i], s.perm[j] = s.perm[j], s.perm[i] }

// sortRowsIndexed is sortRows plus tracking, through the same permutation,
// each row's index within allRows -- so the caller can build the
// orig-index -> sorted-position lookup windowPartitionCache needs. It must
// apply the exact same ordering (including tie-breaking) as sortRows: it
// drives the sort from one permutation of positions using the identical
// comparator (compareOrderedValueRows over the same per-row keys extracted
// by buildOrderByValues) and the same stable sort, so ties resolve exactly
// as they do in sortRows -- by original relative order.
func sortRowsIndexed(rows []Row, origIdx []int, orderBy []OrderItem) ([]Row, []int) {
	sorted := make([]Row, len(rows))
	copy(sorted, rows)
	sortedIdx := make([]int, len(origIdx))
	copy(sortedIdx, origIdx)
	if len(orderBy) == 0 || len(sorted) <= 1 {
		return sorted, sortedIdx
	}

	lcOrdCols := make([]string, len(orderBy))
	for i, oi := range orderBy {
		lcOrdCols[i] = strings.ToLower(oi.Col)
	}
	items := make([]orderedValueRow, len(sorted))
	for i, r := range sorted {
		items[i] = buildOrderByValues(r, lcOrdCols)
	}

	perm := make([]int, len(sorted))
	for i := range perm {
		perm[i] = i
	}
	sort.Stable(orderedValuePermAsc{orderBy: orderBy, items: items, perm: perm})

	outRows := make([]Row, len(sorted))
	outIdx := make([]int, len(sorted))
	for pos, p := range perm {
		outRows[pos] = items[p].row
		outIdx[pos] = sortedIdx[p]
	}
	return outRows, outIdx
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
	sort.Stable(orderedValueRowsAsc{orderBy: orderBy, items: items})
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
