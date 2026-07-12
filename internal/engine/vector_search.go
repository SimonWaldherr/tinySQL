// Package engine provides a VEC_SEARCH table-valued function for k-nearest
// neighbor (k-NN) vector search — the core building block for
// Retrieval-Augmented Generation (RAG) workloads in tinySQL.
//
// Usage:
//
//	SELECT * FROM VEC_SEARCH('table_name', 'vector_column', query_vector, k [, 'metric' [, 'index']])
//
// Parameters:
//
//	table_name     – name of the table containing vectors
//	vector_column  – column storing VECTOR ([]float64) values
//	query_vector   – the search vector ([]float64 or JSON string)
//	k              – number of nearest neighbors to return
//	metric         – optional distance metric: 'cosine' (default), 'l2', 'manhattan', 'dot'
//	index          – optional index mode: 'flat' (default exact), 'ivf', 'hnsw'
//
// Returns all original columns plus:
//
//	_vec_distance  – computed distance from query_vector
//	_vec_rank      – 1-based rank (1 = closest)
//
// The results are returned in ascending order of distance (closest first).
package engine

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	vecSearchParallelMinRows   = 4096
	vecSearchParallelChunkRows = 2048
)

// VecSearchTableFunc implements the VEC_SEARCH table-valued function.
type VecSearchTableFunc struct{}

func (f *VecSearchTableFunc) Name() string { return "VEC_SEARCH" }

func (f *VecSearchTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 4 || len(args) > 6 {
		return fmt.Errorf("VEC_SEARCH requires 4-6 arguments: (table, column, query_vector, k [, metric [, index]])")
	}
	return nil
}

// vecSearchArgs holds parsed arguments for VEC_SEARCH.
type vecSearchArgs struct {
	tableName string
	colName   string
	queryVec  []float64
	k         int
	metric    string
	indexMode string
}

// vecParseArgs evaluates and validates all VEC_SEARCH arguments.
func vecParseArgs(env ExecEnv, args []Expr, row Row) (vecSearchArgs, error) {
	var a vecSearchArgs

	tableVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return a, fmt.Errorf("VEC_SEARCH table: %w", err)
	}
	tableName, ok := tableVal.(string)
	if !ok {
		return a, fmt.Errorf("VEC_SEARCH: table name must be a string, got %T", tableVal)
	}
	a.tableName = tableName

	colVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return a, fmt.Errorf("VEC_SEARCH column: %w", err)
	}
	colName, ok := colVal.(string)
	if !ok {
		return a, fmt.Errorf("VEC_SEARCH: column name must be a string, got %T", colVal)
	}
	a.colName = colName

	queryVec, err := toVec(env, args[2], row)
	if err != nil {
		return a, fmt.Errorf("VEC_SEARCH query_vector: %w", err)
	}
	a.queryVec = queryVec

	kVal, err := evalExpr(env, args[3], row)
	if err != nil {
		return a, fmt.Errorf("VEC_SEARCH k: %w", err)
	}
	k, err := toInt(kVal)
	if err != nil {
		return a, fmt.Errorf("VEC_SEARCH k: %w", err)
	}
	if k <= 0 {
		return a, fmt.Errorf("VEC_SEARCH: k must be > 0, got %d", k)
	}
	a.k = k

	a.metric = "cosine"
	if len(args) == 5 {
		mv, err := evalExpr(env, args[4], row)
		if err != nil {
			return a, fmt.Errorf("VEC_SEARCH metric: %w", err)
		}
		ms, ok := mv.(string)
		if !ok {
			return a, fmt.Errorf("VEC_SEARCH: metric must be a string, got %T", mv)
		}
		a.metric = normalizeVecMetric(ms)
		if a.metric == "" {
			return a, fmt.Errorf("VEC_SEARCH: unknown metric %q (supported: cosine, l2, euclidean, manhattan, l1, dot, inner_product)", ms)
		}
	} else {
		a.metric = "cosine"
	}
	a.indexMode = "flat"
	if len(args) == 6 {
		iv, err := evalExpr(env, args[5], row)
		if err != nil {
			return a, fmt.Errorf("VEC_SEARCH index: %w", err)
		}
		is, ok := iv.(string)
		if !ok {
			return a, fmt.Errorf("VEC_SEARCH: index must be a string, got %T", iv)
		}
		a.indexMode = normalizeVecIndexMode(is)
		if a.indexMode == "" {
			return a, fmt.Errorf("VEC_SEARCH: unknown index %q (supported: flat, exact, ivf, hnsw)", is)
		}
	}

	return a, nil
}

// vecScoredRow pairs a table row index with its computed distance.
type vecScoredRow struct {
	rowIdx   int
	distance float64
}

type vecScoredHeap []vecScoredRow

func (h vecScoredHeap) Len() int { return len(h) }
func (h vecScoredHeap) Less(i, j int) bool {
	if h[i].distance == h[j].distance {
		return h[i].rowIdx > h[j].rowIdx
	}
	return h[i].distance > h[j].distance
}
func (h vecScoredHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// vecScoredHeapPush/Pop/fixDown replicate container/heap's up/down algorithm
// directly on the concrete vecScoredHeap type instead of going through
// heap.Interface. heap.Push/Pop take/return `any`, which forces every
// vecScoredRow (an 8-byte int + 8-byte float64) to be heap-allocated just to
// box it into the interface — on the hot paths here (flat scan, IVF list
// scan, HNSW candidate expansion) that is one allocation per row considered.
// Calling Less/Swap directly on the concrete type keeps the exact same
// ordering with zero boxing and lets the compiler inline the comparisons.
func vecScoredHeapPush(h *vecScoredHeap, v vecScoredRow) {
	*h = append(*h, v)
	vecScoredHeapUp(*h, len(*h)-1)
}

func vecScoredHeapPop(h *vecScoredHeap) vecScoredRow {
	old := *h
	n := len(old) - 1
	old.Swap(0, n)
	vecScoredHeapDown(old[:n], 0)
	v := old[n]
	*h = old[:n]
	return v
}

func vecScoredHeapUp(h vecScoredHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func vecScoredHeapDown(h vecScoredHeap, i0 int) {
	n := len(h)
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

type vecSearchColumnCacheKey struct {
	tenant string
	table  string
	colIdx int
}

type vecSearchColumnCacheEntry struct {
	table      *storage.Table
	version    int
	vectors    [][]float64
	norms      []float64
	normsReady bool
	valid      []bool
}

type vecColumnBuildCall struct{ done chan struct{} }

// vecColumnCacheMaxEntries bounds the column cache. Entries are keyed by
// (tenant, table, column) and each one pins its *storage.Table — including
// every row — via the entry's table pointer. Same-name replacement reuses
// the key, and DROP TABLE purges eagerly (purgeVectorCachesFor), but paths
// with no purge hook (table renames, tenant removal) would otherwise leak
// one pinned table per orphaned key for the life of the process. When the
// cap is hit, arbitrary entries are evicted; the cost of a bad eviction is
// one lazy rebuild scan on next query.
const vecColumnCacheMaxEntries = 256

var (
	vecSearchColumnCacheMu sync.RWMutex
	vecSearchColumnCache   = make(map[vecSearchColumnCacheKey]vecSearchColumnCacheEntry)
	// vecSearchColumnBuilds coalesces concurrent cold reads for the same
	// vector column. Without it, a RAG request burst can make every caller
	// scan and normalize the whole column before any one cache entry wins.
	vecSearchColumnBuilds = make(map[vecSearchColumnCacheKey]*vecColumnBuildCall)
)

// purgeVectorCachesFor eagerly drops all cached vector-search structures
// (column cache, IVF and HNSW indexes) for one table, called from
// DROP TABLE. Without this, the last cache entry keeps the dropped table's
// entire row data reachable until the same (tenant, table, column) key is
// written again — which for a dropped name may be never.
func purgeVectorCachesFor(tenant, table string) {
	vecSearchColumnCacheMu.Lock()
	for k := range vecSearchColumnCache {
		if k.tenant == tenant && k.table == table {
			delete(vecSearchColumnCache, k)
		}
	}
	vecSearchColumnCacheMu.Unlock()

	vecIVFCacheMu.Lock()
	for k := range vecIVFCache {
		if k.tenant == tenant && k.table == table {
			delete(vecIVFCache, k)
		}
	}
	vecIVFCacheMu.Unlock()

	vecHNSWCacheMu.Lock()
	for k := range vecHNSWCache {
		if k.tenant == tenant && k.table == table {
			delete(vecHNSWCache, k)
		}
	}
	vecHNSWCacheMu.Unlock()
}

// evictOverCap removes arbitrary entries until the map is below the cap,
// making room for one more. Go's random map iteration order makes this a
// cheap pseudo-random eviction policy.
func evictOverCap[K comparable, V any](m map[K]V, maxEntries int) {
	for k := range m {
		if len(m) < maxEntries {
			return
		}
		delete(m, k)
	}
}

func getVecColumnCache(tenant string, table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	key := vecSearchColumnCacheKey{tenant: tenant, table: table.Name, colIdx: colIdx}

	for {
		vecSearchColumnCacheMu.RLock()
		if cached, ok := vecSearchColumnCache[key]; ok && cached.table == table && cached.version == table.Version && (!includeNorms || cached.normsReady) {
			vecSearchColumnCacheMu.RUnlock()
			return cached
		}
		vecSearchColumnCacheMu.RUnlock()

		vecSearchColumnCacheMu.Lock()
		if cached, ok := vecSearchColumnCache[key]; ok && cached.table == table && cached.version == table.Version && (!includeNorms || cached.normsReady) {
			vecSearchColumnCacheMu.Unlock()
			return cached
		}
		if call := vecSearchColumnBuilds[key]; call != nil {
			vecSearchColumnCacheMu.Unlock()
			<-call.done
			continue
		}
		call := &vecColumnBuildCall{done: make(chan struct{})}
		vecSearchColumnBuilds[key] = call
		vecSearchColumnCacheMu.Unlock()

		entry := buildVecColumnCache(table, colIdx, includeNorms)
		vecSearchColumnCacheMu.Lock()
		if _, exists := vecSearchColumnCache[key]; !exists {
			evictOverCap(vecSearchColumnCache, vecColumnCacheMaxEntries)
		}
		vecSearchColumnCache[key] = entry
		delete(vecSearchColumnBuilds, key)
		close(call.done)
		vecSearchColumnCacheMu.Unlock()
		return entry
	}
}

func buildVecColumnCache(table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	vectors := make([][]float64, len(table.Rows))
	var norms []float64
	if includeNorms {
		norms = make([]float64, len(table.Rows))
	}
	valid := make([]bool, len(table.Rows))

	for i, r := range table.Rows {
		if colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		vec, ok := vecRowValue(r[colIdx])
		if !ok {
			continue
		}
		if includeNorms {
			norms[i] = vectorL2Norm(vec)
		}
		valid[i] = true
		vectors[i] = vec
	}
	return vecSearchColumnCacheEntry{table: table, version: table.Version, vectors: vectors, norms: norms, normsReady: includeNorms, valid: valid}
}

func vectorL2Norm(v []float64) float64 {
	return math.Sqrt(vectorDot(v, v))
}

func pushTopK(heapRows *vecScoredHeap, rowIdx int, distance float64, k int) {
	if k <= 0 {
		return
	}
	if heapRows.Len() < k {
		vecScoredHeapPush(heapRows, vecScoredRow{rowIdx: rowIdx, distance: distance})
		return
	}
	if heapRows.Len() > 0 && vecScoredRowLess(vecScoredRow{rowIdx: rowIdx, distance: distance}, (*heapRows)[0]) {
		(*heapRows)[0] = vecScoredRow{rowIdx: rowIdx, distance: distance}
		vecScoredHeapDown(*heapRows, 0)
	}
}

func vecScoredRowLess(a, b vecScoredRow) bool {
	if a.distance == b.distance {
		return a.rowIdx < b.rowIdx
	}
	return a.distance < b.distance
}

func topKFromHeap(heapRows *vecScoredHeap, k int) []vecScoredRow {
	if k > heapRows.Len() {
		k = heapRows.Len()
	}
	if k <= 0 {
		return nil
	}
	rows := make([]vecScoredRow, k)
	for i := k - 1; i >= 0; i-- {
		rows[i] = vecScoredHeapPop(heapRows)
	}
	return rows
}

type vecDistanceFunc func(vec []float64, rowIdx int) (float64, bool)

func buildVecDistanceFunc(metric string, query []float64, queryNorm float64, cache vecSearchColumnCacheEntry) vecDistanceFunc {
	switch metric {
	case "cosine":
		if queryNorm == 0 {
			return func([]float64, int) (float64, bool) { return 0, false }
		}
		return func(vec []float64, rowIdx int) (float64, bool) {
			if rowIdx >= len(cache.valid) || !cache.valid[rowIdx] {
				return 0, false
			}
			return vectorDistance(metric, vec, query, cache.norms[rowIdx], queryNorm)
		}
	case "l2":
		return func(vec []float64, _ int) (float64, bool) {
			return vectorDistance(metric, vec, query, 0, 0)
		}
	case "manhattan":
		return func(vec []float64, _ int) (float64, bool) {
			return vectorDistance(metric, vec, query, 0, 0)
		}
	case "dot":
		return func(vec []float64, _ int) (float64, bool) {
			return vectorDistance(metric, vec, query, 0, 0)
		}
	default:
		return func([]float64, int) (float64, bool) { return 0, false }
	}
}

func vecSearchWorkerCount(rows, dims int) int {
	if rows < vecSearchParallelMinRows || dims == 0 {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		return 1
	}
	maxByRows := (rows + vecSearchParallelChunkRows - 1) / vecSearchParallelChunkRows
	if workers > maxByRows {
		workers = maxByRows
	}
	if workers < 2 {
		return 1
	}
	return workers
}

func vecSearchTopK(ctx context.Context, rows [][]any, queryLen int, k int, cache vecSearchColumnCacheEntry, distFn vecDistanceFunc) ([]vecScoredRow, error) {
	workers := vecSearchWorkerCount(len(rows), queryLen)
	if workers == 1 {
		h, err := vecSearchTopKRange(ctx, rows, 0, len(rows), queryLen, k, cache, distFn)
		if err != nil {
			return nil, err
		}
		return topKFromHeap(&h, k), nil
	}

	type workerResult struct {
		heapRows vecScoredHeap
		err      error
	}
	results := make([]workerResult, workers)
	var wg sync.WaitGroup
	chunk := (len(rows) + workers - 1) / workers

	for worker := 0; worker < workers; worker++ {
		start := worker * chunk
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(worker, start, end int) {
			defer wg.Done()
			h, err := vecSearchTopKRange(ctx, rows, start, end, queryLen, k, cache, distFn)
			results[worker] = workerResult{heapRows: h, err: err}
		}(worker, start, end)
	}
	wg.Wait()

	merged := &vecScoredHeap{}
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		localRows := topKFromHeap(&results[i].heapRows, k)
		for _, sr := range localRows {
			pushTopK(merged, sr.rowIdx, sr.distance, k)
		}
	}
	return topKFromHeap(merged, k), nil
}

func vecSearchTopKRange(ctx context.Context, rows [][]any, start, end, queryLen, k int, cache vecSearchColumnCacheEntry, distFn vecDistanceFunc) (vecScoredHeap, error) {
	scoredRows := &vecScoredHeap{}

	for i := start; i < end; i++ {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if i >= len(cache.valid) || !cache.valid[i] {
			continue
		}
		vec := cache.vectors[i]
		if len(vec) != queryLen {
			continue
		}
		dist, ok := distFn(vec, i)
		if !ok {
			continue
		}
		pushTopK(scoredRows, i, dist, k)
	}

	return *scoredRows, nil
}

func normalizeVecMetric(metric string) string {
	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "cosine":
		return "cosine"
	case "l2", "euclidean":
		return "l2"
	case "manhattan", "l1":
		return "manhattan"
	case "dot", "inner_product":
		return "dot"
	default:
		return ""
	}
}

func (f *VecSearchTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	a, err := vecParseArgs(env, args, row)
	if err != nil {
		return nil, err
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, a.tableName)
	if err != nil {
		return nil, fmt.Errorf("VEC_SEARCH: table %q not found: %w", a.tableName, err)
	}

	vecColIdx, err := table.ColIndex(a.colName)
	if err != nil {
		return nil, fmt.Errorf("VEC_SEARCH: %w", err)
	}

	resultCols := make([]string, 0, len(table.Cols)+2)
	for _, c := range table.Cols {
		resultCols = append(resultCols, c.Name)
	}
	resultCols = append(resultCols, "_vec_distance", "_vec_rank")

	queryLen := len(a.queryVec)
	var queryNorm float64
	if a.metric == "cosine" {
		queryNorm = vectorL2Norm(a.queryVec)
	}

	searchCtx := ctx
	if searchCtx == nil {
		searchCtx = env.ctx
	}
	started := time.Now()
	key := vecQueryKey(tenant, table.Name, a.colName, table.Version, a)
	scoredRowsOrdered, cacheHit := getVecQueryCache(key)
	if !cacheHit {
		cache := getVecColumnCache(tenant, table, vecColIdx, a.metric == "cosine")
		distFn := buildVecDistanceFunc(a.metric, a.queryVec, queryNorm, cache)
		scoredRowsOrdered, err = vecSearchTopKWithIndex(searchCtx, tenant, table, vecColIdx, a, queryLen, queryNorm, cache, distFn)
		if err != nil {
			return nil, err
		}
		putVecQueryCache(key, scoredRowsOrdered)
	}
	recordVecQuery(VectorQueryEvent{At: time.Now(), Table: table.Name, Column: a.colName, Metric: a.metric, Index: a.indexMode, K: a.k, CacheHit: cacheHit, Duration: time.Since(started)})

	resultRows := make([]Row, 0, len(scoredRowsOrdered))
	for rank, sr := range scoredRowsOrdered {
		r := make(Row)
		for ci, c := range table.Cols {
			if ci < len(table.Rows[sr.rowIdx]) {
				r[c.Name] = table.Rows[sr.rowIdx][ci]
			}
		}
		r["_vec_distance"] = sr.distance
		r["_vec_rank"] = rank + 1
		resultRows = append(resultRows, r)
	}

	return &ResultSet{
		Cols: resultCols,
		Rows: resultRows,
	}, nil
}

// vecRowValue extracts a []float64 from a stored row cell.
func vecRowValue(v any) ([]float64, bool) {
	switch val := v.(type) {
	case []float64:
		return val, true
	case string:
		coerced, err := coerceToVector(val)
		if err != nil {
			return nil, false
		}
		return coerced.([]float64), true
	default:
		return nil, false
	}
}

// computeDistance computes distance between two vectors using the specified metric.
func computeDistance(a, b []float64, metric string) (float64, error) {
	normalized := normalizeVecMetric(metric)
	if normalized == "" {
		return 0, fmt.Errorf("unknown metric %q (supported: cosine, l2, manhattan, dot)", metric)
	}
	var normA, normB float64
	if normalized == "cosine" {
		normA = vectorL2Norm(a)
		normB = vectorL2Norm(b)
	}
	dist, ok := vectorDistance(normalized, a, b, normA, normB)
	if !ok {
		if normalized == "cosine" && len(a) == len(b) && (normA == 0 || normB == 0) {
			return 0, fmt.Errorf("zero-length vector")
		}
		return 0, fmt.Errorf("dimension mismatch %d vs %d", len(a), len(b))
	}
	return dist, nil
}

// VecTopKTableFunc implements VEC_TOP_K — alias/alternative API for k-NN search.
// Usage: SELECT * FROM VEC_TOP_K('table', 'column', query_vec, k [, 'metric'])
type VecTopKTableFunc struct {
	inner VecSearchTableFunc
}

func (f *VecTopKTableFunc) Name() string { return "VEC_TOP_K" }

func (f *VecTopKTableFunc) ValidateArgs(args []Expr) error {
	return f.inner.ValidateArgs(args)
}

func (f *VecTopKTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	return f.inner.Execute(ctx, args, env, row)
}

func init() {
	RegisterTableFunc(&VecSearchTableFunc{})
	RegisterTableFunc(&VecTopKTableFunc{})
}
