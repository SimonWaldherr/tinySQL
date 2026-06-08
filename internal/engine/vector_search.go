// Package engine provides a VEC_SEARCH table-valued function for k-nearest
// neighbor (k-NN) vector search — the core building block for
// Retrieval-Augmented Generation (RAG) workloads in tinySQL.
//
// Usage:
//
//	SELECT * FROM VEC_SEARCH('table_name', 'vector_column', query_vector, k [, 'metric'])
//
// Parameters:
//
//	table_name     – name of the table containing vectors
//	vector_column  – column storing VECTOR ([]float64) values
//	query_vector   – the search vector ([]float64 or JSON string)
//	k              – number of nearest neighbors to return
//	metric         – optional distance metric: 'cosine' (default), 'l2', 'manhattan', 'dot'
//
// Returns all original columns plus:
//
//	_vec_distance  – computed distance from query_vector
//	_vec_rank      – 1-based rank (1 = closest)
//
// The results are returned in ascending order of distance (closest first).
package engine

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
)

// VecSearchTableFunc implements the VEC_SEARCH table-valued function.
type VecSearchTableFunc struct{}

func (f *VecSearchTableFunc) Name() string { return "VEC_SEARCH" }

func (f *VecSearchTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 4 || len(args) > 5 {
		return fmt.Errorf("VEC_SEARCH requires 4-5 arguments: (table, column, query_vector, k [, metric])")
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

	return a, nil
}

// vecScoredRow pairs a table row index with its computed distance.
type vecScoredRow struct {
	rowIdx   int
	distance float64
}

type vecScoredHeap []vecScoredRow

func (h vecScoredHeap) Len() int           { return len(h) }
func (h vecScoredHeap) Less(i, j int) bool { return h[i].distance > h[j].distance }
func (h vecScoredHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *vecScoredHeap) Push(x any) {
	*h = append(*h, x.(vecScoredRow))
}

func (h *vecScoredHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
}

type vecSearchNormCacheKey struct {
	tenant string
	table  string
	colIdx int
}

type vecSearchNormCacheEntry struct {
	version int
	norms   []float64
	valid   []bool
}

var (
	vecSearchNormCacheMu sync.RWMutex
	vecSearchNormCache   = make(map[vecSearchNormCacheKey]vecSearchNormCacheEntry)
)

func getVecColumnNorms(tenant, tableName string, tableVersion int, tableRows [][]any, colIdx int) ([]float64, []bool) {
	key := vecSearchNormCacheKey{tenant: tenant, table: tableName, colIdx: colIdx}

	vecSearchNormCacheMu.RLock()
	if cached, ok := vecSearchNormCache[key]; ok && cached.version == tableVersion {
		norms := cached.norms
		valid := cached.valid
		vecSearchNormCacheMu.RUnlock()
		return norms, valid
	}
	vecSearchNormCacheMu.RUnlock()

	norms := make([]float64, len(tableRows))
	valid := make([]bool, len(tableRows))

	for i, r := range tableRows {
		if colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		vec, ok := vecRowValue(r[colIdx])
		if !ok {
			continue
		}
		norms[i] = vectorL2Norm(vec)
		valid[i] = true
	}

	entry := vecSearchNormCacheEntry{version: tableVersion, norms: norms, valid: valid}
	vecSearchNormCacheMu.Lock()
	vecSearchNormCache[key] = entry
	vecSearchNormCacheMu.Unlock()
	return norms, valid
}

func vectorL2Norm(v []float64) float64 {
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	return math.Sqrt(sum)
}

func cosineDistanceFromNorm(vecA, vecB []float64, normA, normB float64) (float64, error) {
	if normA == 0 || normB == 0 {
		return 0, fmt.Errorf("zero-length vector")
	}
	if len(vecA) != len(vecB) {
		return 0, fmt.Errorf("dimension mismatch %d vs %d", len(vecA), len(vecB))
	}
	var dot float64
	for i := range vecA {
		dot += vecA[i] * vecB[i]
	}
	return 1.0 - dot/(normA*normB), nil
}

func pushTopK(heapRows *vecScoredHeap, rowIdx int, distance float64, k int) {
	if k <= 0 {
		return
	}
	if heapRows.Len() < k {
		heap.Push(heapRows, vecScoredRow{rowIdx: rowIdx, distance: distance})
		return
	}
	if heapRows.Len() > 0 && (*heapRows)[0].distance > distance {
		(*heapRows)[0] = vecScoredRow{rowIdx: rowIdx, distance: distance}
		heap.Fix(heapRows, 0)
	}
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
		rows[i] = heap.Pop(heapRows).(vecScoredRow)
	}
	return rows
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

	scoredRows := &vecScoredHeap{}
	heap.Init(scoredRows)
	queryLen := len(a.queryVec)
	var norms []float64
	var normValid []bool
	var queryNorm float64
	if a.metric == "cosine" {
		queryNorm = vectorL2Norm(a.queryVec)
		norms, normValid = getVecColumnNorms(tenant, table.Name, table.Version, table.Rows, vecColIdx)
	}
	for i, r := range table.Rows {
		if vecColIdx >= len(r) || r[vecColIdx] == nil {
			continue
		}
		vec, ok := vecRowValue(r[vecColIdx])
		if !ok {
			continue
		}
		if len(vec) != queryLen {
			continue
		}

		var dist float64
		var err error
		if a.metric == "cosine" {
			if i >= len(normValid) || !normValid[i] {
				continue
			}
			dist, err = cosineDistanceFromNorm(vec, a.queryVec, norms[i], queryNorm)
		} else {
			dist, err = computeDistance(vec, a.queryVec, a.metric)
		}
		if err != nil {
			continue
		}
		pushTopK(scoredRows, i, dist, a.k)
	}

	scoredRowsOrdered := topKFromHeap(scoredRows, a.k)

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
	switch metric {
	case "cosine":
		sim, err := cosineSimilarity(a, b)
		if err != nil {
			return 0, err
		}
		return 1.0 - sim, nil
	case "l2", "euclidean":
		var sum float64
		for i := range a {
			d := a[i] - b[i]
			sum += d * d
		}
		return math.Sqrt(sum), nil
	case "manhattan", "l1":
		var sum float64
		for i := range a {
			sum += math.Abs(a[i] - b[i])
		}
		return sum, nil
	case "dot", "inner_product":
		var dot float64
		for i := range a {
			dot += a[i] * b[i]
		}
		return -dot, nil // negate so smaller = more similar
	default:
		return 0, fmt.Errorf("unknown metric %q (supported: cosine, l2, manhattan, dot)", metric)
	}
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
