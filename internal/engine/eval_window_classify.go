// Choropleth classification window functions: EQUAL_INTERVAL and
// NATURAL_BREAKS (Jenks). Quantile classification needs no new code here --
// NTILE(n) OVER (ORDER BY kpi) (eval_window.go) already buckets a column
// into n roughly-equal-sized groups, exactly mapshaper's -classify
// "quantile" method.
//
// NATURAL_BREAKS is the one window function in this file that cannot be
// O(1) per row the way every other window function here is: computing
// Jenks' variance-minimizing class boundaries needs a single O(len*n^2)
// dynamic-programming pass over the whole partition, and evalWindowFunction
// is invoked once per output row. Recomputing that DP on every row would
// turn an O(len*n^2) cost into O(len^2*n^2) for the query as a whole, which
// is not acceptable even at moderate partition sizes -- so its result is
// memoized per (window-function AST node, partition, n) in natBreaksCache,
// bounded and evicted the same way every other cache in this codebase is.
package engine

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"sync"
)

// natBreaksCacheKey identifies one NATURAL_BREAKS computation: ex is the
// specific *FuncCall AST node for this SELECT-list item (stable across every
// row of one query, and -- this is why signature matters -- also stable
// across separate executions of the same compiled statement, e.g. via
// tinysql.ExecuteCompiled), partition is the formatted PARTITION BY tuple
// (empty for an unpartitioned window), n is the requested class count, and
// signature is a content hash of the classified column's values, so a
// cached entry from an earlier execution against different underlying data
// never gets served to a later one just because it reused the same AST node
// and partition value.
type natBreaksCacheKey struct {
	fn        *FuncCall
	partition string
	n         int
	signature uint64
}

// valuesSignature hashes the classified column's values (and which
// positions were valid numeric values at all) into one uint64, cheap
// relative to the O(n^2*k) DP this cache exists to avoid repeating.
func valuesSignature(values []float64, valid []bool) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for i, v := range values {
		if !valid[i] {
			h.Write([]byte{0})
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		h.Write(buf[:])
	}
	return h.Sum64()
}

const natBreaksCacheMaxEntries = 256

var (
	natBreaksMu    sync.Mutex
	natBreaksCache = make(map[natBreaksCacheKey][]int)
)

// formatWindowPartitionKey renders a PARTITION BY tuple, evaluated against
// the current row, as a cache-key string. It only needs to distinguish
// partitions from each other, not round-trip back to values.
func formatWindowPartitionKey(env ExecEnv, partitionBy []Expr, row Row) string {
	if len(partitionBy) == 0 {
		return ""
	}
	var b strings.Builder
	for i, expr := range partitionBy {
		if i > 0 {
			b.WriteByte('\x1f')
		}
		v, err := evalExpr(env, expr, row)
		if err != nil {
			b.WriteString("<error>")
			continue
		}
		fmt.Fprintf(&b, "%v", v)
	}
	return b.String()
}

// evalEqualInterval computes EQUAL_INTERVAL(n) OVER (ORDER BY kpi): divides
// the range [min(kpi), max(kpi)] into n equal-width bins and returns the
// current row's 1-based bin number. Because evalWindowFunction has already
// sorted partitionRows by the ORDER BY key (ascending or descending), the
// partition's extremes are simply its first and last rows -- O(1), no
// separate scan, regardless of sort direction (min/max of the two endpoint
// values, not assumed-ascending).
func evalEqualInterval(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("EQUAL_INTERVAL expects 1 argument")
	}
	if ex.Over == nil || len(ex.Over.OrderBy) == 0 {
		return nil, fmt.Errorf("EQUAL_INTERVAL requires ORDER BY in the OVER clause")
	}
	nVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	n, err := toInt(nVal)
	if err != nil {
		return nil, fmt.Errorf("EQUAL_INTERVAL: %w", err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("EQUAL_INTERVAL argument must be positive, got %d", n)
	}
	if len(partitionRows) == 0 {
		return nil, nil
	}

	col := strings.ToLower(ex.Over.OrderBy[0].Col)
	firstVal, _ := getValLower(partitionRows[0], col)
	lastVal, _ := getValLower(partitionRows[len(partitionRows)-1], col)
	firstF, ok1 := numeric(firstVal)
	lastF, ok2 := numeric(lastVal)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("EQUAL_INTERVAL: ORDER BY column %q is not numeric", ex.Over.OrderBy[0].Col)
	}
	minF, maxF := math.Min(firstF, lastF), math.Max(firstF, lastF)

	curVal, ok := getValLower(row, col)
	curF, numOk := numeric(curVal)
	if !ok || !numOk {
		return nil, nil
	}
	if maxF == minF {
		return 1, nil
	}
	bucket := int((curF - minF) / ((maxF - minF) / float64(n)))
	if bucket >= n {
		bucket = n - 1
	}
	if bucket < 0 {
		bucket = 0
	}
	return bucket + 1, nil
}

// evalNaturalBreaks computes NATURAL_BREAKS(n) OVER (ORDER BY kpi): Jenks
// natural breaks, minimizing the sum of within-class squared deviations
// from each class's own mean. See the package doc comment above for why
// this memoizes per partition instead of recomputing per row.
//
// Complexity: the underlying DP is O(len*n^2) per partition, computed once
// (not per row) thanks to the memoization above. That is comfortably fast
// for realistic choropleth partition sizes -- a few thousand regions
// (municipalities, postal codes, districts) with a handful of legend
// classes. Much larger partitions (tens of thousands of rows) would need
// the faster O(len*n) Fisher/Wang-Song variance-minimizing algorithm;
// that is deliberately not implemented here.
func evalNaturalBreaks(env ExecEnv, ex *FuncCall, partitionRows []Row, currentIdx int, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("NATURAL_BREAKS expects 1 argument")
	}
	if ex.Over == nil || len(ex.Over.OrderBy) == 0 {
		return nil, fmt.Errorf("NATURAL_BREAKS requires ORDER BY in the OVER clause")
	}
	nVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	n, err := toInt(nVal)
	if err != nil {
		return nil, fmt.Errorf("NATURAL_BREAKS: %w", err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("NATURAL_BREAKS argument must be positive, got %d", n)
	}
	if len(partitionRows) == 0 {
		return nil, nil
	}
	if currentIdx < 0 || currentIdx >= len(partitionRows) {
		return nil, nil
	}

	// Extract the classified column's values up front (O(n), cheap relative
	// to the O(n^2*k) DP this cache exists to avoid repeating) so the cache
	// key can include a content signature, not just the query's AST-node
	// identity and partition value. That identity alone is NOT enough: a
	// cached, compiled statement (tinysql.ExecuteCompiled, used by e.g. the
	// WASM query bridge) reuses the same *FuncCall across separate
	// executions of the same query text against a table whose *rows* have
	// since changed. Keying only on (fn, partition, n) would then serve a
	// stale classes slice sized for the OLD row count -- observed in
	// practice as an out-of-range panic when a later execution's
	// currentIdx exceeded the stale slice's length. The content signature
	// changes whenever the underlying data does, so a stale cache entry
	// simply misses instead of being served.
	col := strings.ToLower(ex.Over.OrderBy[0].Col)
	values := make([]float64, len(partitionRows))
	valid := make([]bool, len(partitionRows))
	for i, r := range partitionRows {
		v, exists := getValLower(r, col)
		if !exists {
			continue
		}
		if f, numOk := numeric(v); numOk {
			values[i] = f
			valid[i] = true
		}
	}

	key := natBreaksCacheKey{
		fn:        ex,
		partition: formatWindowPartitionKey(env, ex.Over.PartitionBy, row),
		n:         n,
		signature: valuesSignature(values, valid),
	}
	natBreaksMu.Lock()
	classes, ok := natBreaksCache[key]
	natBreaksMu.Unlock()
	if !ok {
		classes = computeJenksClasses(values, valid, n)
		natBreaksMu.Lock()
		evictOverCap(natBreaksCache, natBreaksCacheMaxEntries)
		natBreaksCache[key] = classes
		natBreaksMu.Unlock()
	}
	// Defensive: classes should always be exactly len(partitionRows) long
	// for a freshly-computed or correctly-keyed cached entry. Guarding the
	// index anyway -- rather than trusting the cache unconditionally --
	// matches this codebase's existing precedent for cache lookups feeding
	// a row index (see the stale-result-cache guard in vector_search.go).
	if currentIdx >= len(classes) {
		return nil, nil
	}
	class := classes[currentIdx]
	if class == 0 {
		return nil, nil
	}
	return class, nil
}

// computeJenksClasses runs the Jenks DP over only the valid (numeric)
// entries of values, in their existing order (already sorted by the
// caller's ORDER BY), and maps the resulting class numbers back onto a
// result slice the same length as values -- positions that were not valid
// numeric values get class 0 ("unclassifiable"), matching
// evalEqualInterval's NULL-for-non-numeric convention.
func computeJenksClasses(values []float64, valid []bool, n int) []int {
	result := make([]int, len(values))
	var filteredIdx []int
	var filteredVals []float64
	for i, v := range values {
		if valid[i] {
			filteredIdx = append(filteredIdx, i)
			filteredVals = append(filteredVals, v)
		}
	}
	m := len(filteredVals)
	if m == 0 {
		return result
	}
	k := n
	if k > m {
		k = m
	}
	classOf := jenksBreaks(filteredVals, k)
	for i, origIdx := range filteredIdx {
		result[origIdx] = classOf[i]
	}
	return result
}

// jenksBreaks partitions the already-sorted slice sorted into k contiguous
// classes minimizing the total within-class sum of squared deviations from
// each class's own mean (the standard Jenks/Fisher natural-breaks
// objective, equivalent to 1-D k-means restricted to contiguous classes).
// Returns each position's 1-based class number.
//
// DP: with prefix sums s1[i] = sum_{j<i} v[j] and s2[i] = sum_{j<i} v[j]^2,
// cost(i, j) (the cost of one class spanning indices [i, j)) is computed in
// O(1); dp[c][j] = min over i of dp[c-1][i] + cost(i, j). O(m*k) states,
// O(m) transition each: O(m^2*k) total.
func jenksBreaks(sorted []float64, k int) []int {
	m := len(sorted)
	result := make([]int, m)
	if m == 0 {
		return result
	}
	if k <= 1 || m == 1 {
		for i := range result {
			result[i] = 1
		}
		return result
	}

	s1 := make([]float64, m+1)
	s2 := make([]float64, m+1)
	for i := 0; i < m; i++ {
		s1[i+1] = s1[i] + sorted[i]
		s2[i+1] = s2[i] + sorted[i]*sorted[i]
	}
	cost := func(i, j int) float64 {
		n := float64(j - i)
		if n <= 0 {
			return 0
		}
		sum := s1[j] - s1[i]
		sumSq := s2[j] - s2[i]
		return sumSq - sum*sum/n
	}

	const inf = math.MaxFloat64
	dp := make([][]float64, k+1)
	split := make([][]int, k+1)
	for c := range dp {
		dp[c] = make([]float64, m+1)
		split[c] = make([]int, m+1)
		for j := range dp[c] {
			dp[c][j] = inf
		}
	}
	dp[0][0] = 0
	for c := 1; c <= k; c++ {
		for j := c; j <= m; j++ {
			best := inf
			bestI := 0
			for i := c - 1; i < j; i++ {
				if dp[c-1][i] >= inf {
					continue
				}
				candidate := dp[c-1][i] + cost(i, j)
				if candidate < best {
					best = candidate
					bestI = i
				}
			}
			dp[c][j] = best
			split[c][j] = bestI
		}
	}

	bnd := make([]int, k+1)
	bnd[k] = m
	for c := k; c >= 1; c-- {
		bnd[c-1] = split[c][bnd[c]]
	}
	classOf := make([]int, m)
	for c := 1; c <= k; c++ {
		for idx := bnd[c-1]; idx < bnd[c]; idx++ {
			classOf[idx] = c
		}
	}
	return classOf
}
