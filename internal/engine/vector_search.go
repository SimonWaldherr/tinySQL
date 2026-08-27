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
//	_vec_distance    – computed distance from query_vector (lower = closer)
//	_vec_similarity  – similarity derived from the distance (higher = closer);
//	                   feed this, not _vec_distance, into RAG_HYBRID_SCORE /
//	                   RAG_RANK_SCORE, which expect a similarity input
//	_vec_rank        – 1-based rank (1 = closest)
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

// vecColumnSegment is one immutable, contiguous portion of a vector column
// cache.  Keeping append-only rows in separate segments means a new INSERT
// copies only its own vectors, rather than recopying the entire column just
// because Table.Version advanced.  Segments are merged geometrically (see
// mergeVecColumnSegments) to retain bounded lookup overhead.
type vecColumnSegment struct {
	start   int
	data    []float64
	vectors [][]float64
	norms   []float64
	valid   []bool
}

type vecSearchColumnCacheEntry struct {
	table *storage.Table
	// version is the table.Version this entry covers.  structVersion changes
	// for updates/deletes/schema mutations, but not pure appends, allowing an
	// append-only version change to extend the cache without rebuilding it.
	version       int
	structVersion int
	rows          int
	segments      []vecColumnSegment
	// overrides hold UPDATE-only row deltas. They keep large immutable base
	// segments reusable instead of recopying an entire embedding column.
	overrides  map[int]vecColumnOverride
	normsReady bool
}

type vecColumnOverride struct {
	vector []float64
	norm   float64
	valid  bool
}

func (c vecSearchColumnCacheEntry) rowCount() int { return c.rows }

func (c vecSearchColumnCacheEntry) segmentFor(row int) *vecColumnSegment {
	if row < 0 || row >= c.rows {
		return nil
	}
	lo, hi := 0, len(c.segments)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if c.segments[mid].start <= row {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return nil
	}
	segment := &c.segments[lo-1]
	if row >= segment.start+len(segment.vectors) {
		return nil
	}
	return segment
}

func (c vecSearchColumnCacheEntry) vector(row int) []float64 {
	if override, ok := c.overrides[row]; ok {
		return override.vector
	}
	segment := c.segmentFor(row)
	if segment == nil {
		return nil
	}
	return segment.vectors[row-segment.start]
}

func (c vecSearchColumnCacheEntry) validAt(row int) bool {
	if override, ok := c.overrides[row]; ok {
		return override.valid
	}
	segment := c.segmentFor(row)
	return segment != nil && segment.valid[row-segment.start]
}

func (c vecSearchColumnCacheEntry) normAt(row int) float64 {
	if override, ok := c.overrides[row]; ok {
		return override.norm
	}
	segment := c.segmentFor(row)
	if segment == nil || !c.normsReady || len(segment.norms) == 0 {
		return vectorL2Norm(c.vector(row))
	}
	return segment.norms[row-segment.start]
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
	purgeRAGFilteredANNCachesFor(tenant, table)
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
		// Capture the stale entry while holding the cache lock.  It remains
		// immutable after we release the lock, so an append extension can share
		// its segments safely without doing a speculative full rebuild first.
		cached, canExtend := vecSearchColumnCache[key]
		canExtend = canExtend && canExtendVecColumnCache(cached, table)
		updatedRows, canRefresh := table.UpdatedRowsSince(cached.structVersion)
		call := &vecColumnBuildCall{done: make(chan struct{})}
		vecSearchColumnBuilds[key] = call
		vecSearchColumnCacheMu.Unlock()

		var entry vecSearchColumnCacheEntry
		if canExtend {
			// A stale entry can be safely extended only when structural changes
			// did not occur and the table grew by appending rows.
			entry = extendVecColumnCache(cached, table, colIdx, includeNorms)
		} else if canRefresh && cached.table == table && cached.rows <= len(table.Rows) {
			entry = refreshVecColumnCache(cached, table, colIdx, includeNorms, updatedRows)
		} else {
			entry = buildVecColumnCache(table, colIdx, includeNorms)
		}
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

func refreshVecColumnCache(cached vecSearchColumnCacheEntry, table *storage.Table, colIdx int, includeNorms bool, rows []int) vecSearchColumnCacheEntry {
	entry := extendVecColumnCache(cached, table, colIdx, includeNorms)
	entry.overrides = make(map[int]vecColumnOverride, len(cached.overrides)+len(rows))
	for row, value := range cached.overrides {
		if (includeNorms || cached.normsReady) && value.valid {
			value.norm = vectorL2Norm(value.vector)
		}
		entry.overrides[row] = value
	}
	for _, row := range rows {
		if row < 0 || row >= len(table.Rows) {
			continue
		}
		value := vecColumnOverride{}
		if colIdx < len(table.Rows[row]) && table.Rows[row][colIdx] != nil {
			if vector, ok := vecRowValue(table.Rows[row][colIdx]); ok {
				value.vector = append([]float64(nil), vector...)
				value.valid = true
				if includeNorms || cached.normsReady {
					value.norm = vectorL2Norm(value.vector)
				}
			}
		}
		entry.overrides[row] = value
	}
	if vecColumnOverridesNeedCompaction(entry) {
		return buildVecColumnCache(table, colIdx, includeNorms || cached.normsReady)
	}
	return entry
}

func vecColumnOverridesNeedCompaction(entry vecSearchColumnCacheEntry) bool {
	limit := entry.rows / 8
	if limit < 32 {
		limit = 32
	}
	if limit > 1024 {
		limit = 1024
	}
	return len(entry.overrides) > limit
}

// buildVecColumnCache extracts one table column into a cache entry backed by
// immutable, contiguous segments.  A cold build produces one segment; later
// pure appends add compact tail segments instead of copying the existing data.
//
// This is a two-pass build: pass 1 classifies each row exactly as before
// (skip missing/nil cells, skip whatever vecRowValue rejects) and tallies the
// total float64 count; pass 2 makes the one allocation for all row data and
// copies each valid row into its own tightly-packed region, in row order.
// Two passes (rather than growing one slice with append) avoid repeated
// reallocate-and-recopy of the whole buffer as it grows — exactly the
// allocator churn this rebuild is meant to eliminate. Rows are packed by
// their own true length, not a fixed dims*len(table.Rows) stride: a column
// mid-embedding-migration can be ragged (mixed lengths across rows), and a
// row whose length doesn't match the query is excluded at search time
// (vecSearchTopKRange, validCacheRow), never truncated or hard-errored here.
func buildVecColumnCache(table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	segment := buildVecColumnSegment(table.Rows, colIdx, includeNorms, 0)
	return vecSearchColumnCacheEntry{
		table:         table,
		version:       table.Version,
		structVersion: table.StructVersion(),
		rows:          len(table.Rows),
		segments:      []vecColumnSegment{segment},
		normsReady:    includeNorms,
	}
}

func buildVecColumnSegment(rows [][]any, colIdx int, includeNorms bool, start int) vecColumnSegment {
	n := len(rows)
	valid := make([]bool, n)
	// Use the final slice-header array as first-pass scratch, then replace each
	// entry with its packed destination in pass two. A separate [][]float64
	// scratch array cost 24 bytes per corpus row during every cold RAG build.
	vectors := make([][]float64, n)
	total := 0

	for i, r := range rows {
		if colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		vec, ok := vecRowValue(r[colIdx])
		if !ok {
			continue
		}
		valid[i] = true
		vectors[i] = vec
		total += len(vec)
	}

	data := make([]float64, total)
	var norms []float64
	if includeNorms {
		norms = make([]float64, n)
	}
	cursor := 0
	for i := 0; i < n; i++ {
		if !valid[i] {
			continue // vectors[i] stays nil (zero value) — identical to before
		}
		vec := vectors[i]
		dst := data[cursor : cursor+len(vec) : cursor+len(vec)] // cap==len: append reallocates, never corrupts row i+1
		copy(dst, vec)
		vectors[i] = dst
		if includeNorms {
			norms[i] = vectorL2Norm(dst)
		}
		cursor += len(vec)
	}
	return vecColumnSegment{start: start, data: data, vectors: vectors, norms: norms, valid: valid}
}

func canExtendVecColumnCache(cached vecSearchColumnCacheEntry, table *storage.Table) bool {
	return cached.table == table &&
		cached.structVersion == table.StructVersion() &&
		cached.rows <= len(table.Rows) &&
		cached.version <= table.Version
}

// extendVecColumnCache returns a new immutable cache view that shares old
// segments and only builds the rows appended since cached.rows.  If cosine
// norms are requested for a previously non-cosine cache, it computes norms
// from the existing packed vectors but still never recopies vector data.
func extendVecColumnCache(cached vecSearchColumnCacheEntry, table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	segments := append([]vecColumnSegment(nil), cached.segments...)
	if includeNorms && !cached.normsReady {
		for i := range segments {
			segment := &segments[i]
			segment.norms = make([]float64, len(segment.vectors))
			for row, vector := range segment.vectors {
				if segment.valid[row] {
					segment.norms[row] = vectorL2Norm(vector)
				}
			}
		}
	}
	if cached.rows < len(table.Rows) {
		segments = append(segments, buildVecColumnSegment(table.Rows[cached.rows:], colIdx, includeNorms || cached.normsReady, cached.rows))
	}
	segments = mergeVecColumnSegments(segments, includeNorms || cached.normsReady)
	overrides := cached.overrides
	if len(overrides) != 0 {
		overrides = make(map[int]vecColumnOverride, len(cached.overrides))
		for row, value := range cached.overrides {
			if (includeNorms || cached.normsReady) && value.valid {
				value.norm = vectorL2Norm(value.vector)
			}
			overrides[row] = value
		}
	}
	return vecSearchColumnCacheEntry{
		table:         table,
		version:       table.Version,
		structVersion: table.StructVersion(),
		rows:          len(table.Rows),
		segments:      segments,
		overrides:     overrides,
		normsReady:    includeNorms || cached.normsReady,
	}
}

// mergeVecColumnSegments performs binary-counter compaction on similarly
// sized adjacent tail segments.  Single-row ingestion therefore copies only
// O(log appendedRows) small tail data amortized; the large original segment is
// copied only once the accumulated append tail has become comparably large.
func mergeVecColumnSegments(segments []vecColumnSegment, normsReady bool) []vecColumnSegment {
	for len(segments) >= 2 {
		right := segments[len(segments)-1]
		left := segments[len(segments)-2]
		if len(left.vectors) == 0 || len(right.vectors) == 0 || len(left.vectors) != len(right.vectors) {
			break
		}
		segments = append(segments[:len(segments)-2], mergeTwoVecColumnSegments(left, right, normsReady))
	}
	return segments
}

func mergeTwoVecColumnSegments(left, right vecColumnSegment, normsReady bool) vecColumnSegment {
	rows := len(left.vectors) + len(right.vectors)
	valid := make([]bool, rows)
	vectors := make([][]float64, rows)
	total := len(left.data) + len(right.data)
	data := make([]float64, total)
	var norms []float64
	if normsReady {
		norms = make([]float64, rows)
	}
	cursor := 0
	copySegment := func(segment vecColumnSegment, offset int) {
		for i, vector := range segment.vectors {
			if !segment.valid[i] {
				continue
			}
			dst := data[cursor : cursor+len(vector) : cursor+len(vector)]
			copy(dst, vector)
			vectors[offset+i] = dst
			valid[offset+i] = true
			if normsReady {
				if len(segment.norms) > i {
					norms[offset+i] = segment.norms[i]
				} else {
					norms[offset+i] = vectorL2Norm(dst)
				}
			}
			cursor += len(vector)
		}
	}
	copySegment(left, 0)
	copySegment(right, len(left.vectors))
	return vecColumnSegment{start: left.start, data: data, vectors: vectors, norms: norms, valid: valid}
}

func vectorL2Norm(v []float64) float64 {
	return math.Sqrt(vectorDot(v, v))
}

func pushTopK(heapRows *vecScoredHeap, rowIdx int, distance float64, k int) {
	// A NaN distance (e.g. from a NaN/Inf-poisoned vector component that
	// slipped past storage-layer validation) must never enter the heap:
	// every comparison against NaN in vecScoredRowLess/vecScoredHeap.Less is
	// false per IEEE 754, so a NaN row that lands at the heap root blocks
	// every later, genuinely-closer real candidate from ever being judged
	// "better" and corrupts the top-k result silently. Flat scan, IVF, and
	// HNSW all fill their result heaps through this one function, so
	// guarding here excludes such rows from top-k consideration everywhere.
	if k <= 0 || math.IsNaN(distance) {
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
			if !cache.validAt(rowIdx) {
				return 0, false
			}
			return vecCheckedDistance(metric, vec, query, cache.normAt(rowIdx), queryNorm)
		}
	case "l2":
		return func(vec []float64, _ int) (float64, bool) {
			return vecCheckedDistance(metric, vec, query, 0, 0)
		}
	case "manhattan":
		return func(vec []float64, _ int) (float64, bool) {
			return vecCheckedDistance(metric, vec, query, 0, 0)
		}
	case "dot":
		return func(vec []float64, _ int) (float64, bool) {
			return vecCheckedDistance(metric, vec, query, 0, 0)
		}
	default:
		return func([]float64, int) (float64, bool) { return 0, false }
	}
}

// vecCheckedDistance wraps vectorRankingDistance with a NaN guard. A NaN or
// Inf-derived-NaN vector component (e.g. dividing by a zero-ish norm that
// underflowed, or a NaN stored via a non-SQL insertion path) can produce a
// NaN distance even though vectorDistance's own "ok" contract only rejects
// dimension mismatches and zero-norm vectors. Treat a NaN result the same as
// any other invalid-row case (ok=false) so it is excluded from top-k
// consideration instead of poisoning the heap (see pushTopK).
//
// Returns a ranking-only distance (see vectorRankingDistance) — every caller
// of buildVecDistanceFunc's returned closure funnels into
// vecSearchTopKWithIndex, which finalizes the small top-k result once
// instead of every candidate paying for it during the scan.
func vecCheckedDistance(metric string, a, b []float64, normA, normB float64) (float64, bool) {
	dist, ok := vectorRankingDistance(metric, a, b, normA, normB)
	if !ok || math.IsNaN(dist) {
		return 0, false
	}
	return dist, true
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
			// A panic in one worker (e.g. a future edge case in a distance
			// function) must not take down the whole process: recover here and
			// surface it as an ordinary worker error, following the same
			// results[worker].err propagation path used below for non-panic
			// errors, instead of letting the goroutine crash uncaught.
			defer func() {
				if r := recover(); r != nil {
					results[worker].err = fmt.Errorf("VEC_SEARCH: worker panic: %v", r)
				}
			}()
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
		if !cache.validAt(i) {
			continue
		}
		vec := cache.vector(i)
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
	table, scoredRowsOrdered, err := vecSearchCandidates(ctx, env, a)
	if err != nil {
		return nil, err
	}
	return materializeVecCandidates(table, scoredRowsOrdered, a.metric), nil
}

// vecSearchCandidates executes only the ranking phase. Keeping row IDs and
// distances compact lets RAG_SEARCH fuse candidate sets before copying any
// source columns; the public VEC_SEARCH function materializes them afterward.
func vecSearchCandidates(ctx context.Context, env ExecEnv, a vecSearchArgs) (*storage.Table, []vecScoredRow, error) {
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, a.tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("VEC_SEARCH: table %q not found: %w", a.tableName, err)
	}

	vecColIdx, err := table.ColIndex(a.colName)
	if err != nil {
		return nil, nil, fmt.Errorf("VEC_SEARCH: %w", err)
	}

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
	// Only hash the query vector (SHA-256 over every element) when the opt-in
	// result cache is actually enabled; it is off by default, so the common
	// path skips the hash entirely.
	cacheEnabled := vecQueryCacheEnabled()
	var (
		key               vecQueryCacheKey
		scoredRowsOrdered []vecScoredRow
		cacheHit          bool
	)
	if cacheEnabled {
		key = vecQueryKey(tenant, table.Name, a.colName, table.Version, a)
		scoredRowsOrdered, cacheHit = getVecQueryCache(key)
	}
	if !cacheHit {
		cache := getVecColumnCache(tenant, table, vecColIdx, a.metric == "cosine")
		distFn := buildVecDistanceFunc(a.metric, a.queryVec, queryNorm, cache)
		scoredRowsOrdered, err = vecSearchTopKWithIndex(searchCtx, tenant, table, vecColIdx, a, queryLen, queryNorm, cache, distFn)
		if err != nil {
			return nil, nil, err
		}
		if cacheEnabled {
			putVecQueryCache(key, scoredRowsOrdered)
		}
	}
	recordVecQuery(VectorQueryEvent{At: time.Now(), Table: table.Name, Column: a.colName, Metric: a.metric, Index: a.indexMode, K: a.k, CacheHit: cacheHit, Duration: time.Since(started)})
	return table, scoredRowsOrdered, nil
}

func materializeVecCandidates(table *storage.Table, scoredRowsOrdered []vecScoredRow, metric string) *ResultSet {
	resultCols := make([]string, 0, len(table.Cols)+3)
	for _, c := range table.Cols {
		resultCols = append(resultCols, c.Name)
	}
	resultCols = append(resultCols, "_vec_distance", "_vec_similarity", "_vec_rank")
	resultRows := make([]Row, 0, len(scoredRowsOrdered))
	rank := 0
	for _, sr := range scoredRowsOrdered {
		// Defensive: a stale opt-in result-cache entry (e.g. from a DROP+CREATE
		// that reproduced table.Version 0 before the cache was purged) could
		// otherwise index past the current table.Rows and panic.
		if sr.rowIdx < 0 || sr.rowIdx >= len(table.Rows) {
			continue
		}
		rank++
		// The complete output width is known. Reserving it avoids repeated map
		// growth while copying wide RAG source rows into every top-k hit.
		r := make(Row, len(table.Cols)+3)
		for ci, c := range table.Cols {
			if ci < len(table.Rows[sr.rowIdx]) {
				r[c.Name] = table.Rows[sr.rowIdx][ci]
			}
		}
		r["_vec_distance"] = sr.distance
		r["_vec_similarity"] = vecSimilarityFromDistance(metric, sr.distance)
		r["_vec_rank"] = rank
		resultRows = append(resultRows, r)
	}

	return &ResultSet{
		Cols: resultCols,
		Rows: resultRows,
	}
}

// vecSimilarityFromDistance converts a VEC_SEARCH distance (lower = closer)
// into a similarity score (higher = closer) for RAG_HYBRID_SCORE/
// RAG_RANK_SCORE, which normalize their similarity input as cosine
// similarity in [-1, 1]. Feeding _vec_distance directly into those scorers
// silently inverts ranking: for cosine, an exact match (distance 0) would
// score 0.5 while an opposite match (distance 2) would score 1.0.
//
// cosine distance is 1-similarity, so similarity = 1-distance, matching
// VEC_COSINE_SIMILARITY's [-1, 1] range. For l2/manhattan/dot, distance has
// no fixed upper bound, so similarity is just the negated distance — for
// "dot", that recovers the raw (unnegated) inner product.
func vecSimilarityFromDistance(metric string, distance float64) float64 {
	if metric == "cosine" {
		return 1.0 - distance
	}
	return -distance
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
