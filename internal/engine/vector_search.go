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
	"maps"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
	"github.com/SimonWaldherr/tinySQL/internal/engine/sqlval"
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
	if len(args) >= 5 {
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
	source *storage.Table
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
	// garbage is how many floats of data no row of vectors points to any more
	// (rows a followed removal dropped); see followVecColumnCache.
	garbage int
}

type vecSearchColumnCacheEntry struct {
	table *storage.Table
	// version is the table.Version this entry covers.  structVersion changes
	// for updates/deletes/schema mutations, but not pure appends, allowing an
	// append-only version change to extend the cache without rebuilding it.
	version       int
	structVersion int
	rows          int
	shape         *vecColumnShapeCache
	segments      []vecColumnSegment
	// overrides hold UPDATE-only row deltas. They keep large immutable base
	// segments reusable instead of recopying an entire embedding column.
	overrides    map[int]vecColumnOverride
	overrideRows []int // sorted row IDs for sequential scans
	normsReady   bool
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

// resolveRow resolves row's validity, vector, and (when needNorm is set) its
// norm with a single override/segment lookup. validAt/vector/normAt each
// redo the same overrides-map probe and segmentFor binary search
// independently, so a caller that needs all three for the same row — the
// flat-scan hot loops in vecSearchTopKRange/ragVecTopKAllowedRange — used to
// pay for that lookup three times per row, plus a fourth time inside the
// cosine distFn closure (which used to call cache.validAt/cache.normAt
// itself). needNorm lets a non-cosine caller skip norm resolution entirely,
// since its distance function never reads the value.
func (c vecSearchColumnCacheEntry) resolveRow(row int, needNorm bool) (vec []float64, norm float64, valid bool) {
	if override, ok := c.overrides[row]; ok {
		if !needNorm {
			return override.vector, 0, override.valid
		}
		return override.vector, override.norm, override.valid
	}
	segment := c.segmentFor(row)
	if segment == nil {
		return nil, 0, false
	}
	idx := row - segment.start
	valid = segment.valid[idx]
	vec = segment.vectors[idx]
	if !needNorm || !valid {
		return vec, 0, valid
	}
	if !c.normsReady || len(segment.norms) == 0 {
		return vec, vectorL2Norm(vec), valid
	}
	return vec, segment.norms[idx], valid
}

// contiguousSegment returns the direct row-to-vector layout used by a fresh
// cache build.  The common read-only RAG corpus has exactly this shape: one
// segment spanning every row and no UPDATE overrides.  A flat vector scan can
// then index its row directly, avoiding an overrides-map lookup and a binary
// segment lookup for every embedding. Random-access ANN resolvers use this
// layout; flat scans can also walk several adjacent segments directly.
func (c vecSearchColumnCacheEntry) contiguousSegment() (vecColumnSegment, bool) {
	if len(c.overrides) != 0 || len(c.segments) != 1 {
		return vecColumnSegment{}, false
	}
	segment := c.segments[0]
	if segment.start != 0 || len(segment.vectors) != c.rows || len(segment.valid) != c.rows {
		return vecColumnSegment{}, false
	}
	if c.normsReady && len(segment.norms) != c.rows {
		return vecColumnSegment{}, false
	}
	return segment, true
}

// directScanSegments admits complete, adjacent segments without UPDATE
// overrides. Validate the layout once so a sequential scan can walk each
// segment instead of resolving its row through a binary search on every visit.
// Incomplete or overlapping layouts keep resolveRow's lookup semantics.
func (c vecSearchColumnCacheEntry) directScanSegments(start, end int) bool {
	return len(c.overrides) == 0 && c.adjacentSegments(start, end)
}

func (c vecSearchColumnCacheEntry) adjacentSegments(start, end int) bool {
	if start < 0 || end > c.rows {
		return false
	}
	next := 0
	for _, segment := range c.segments {
		if segment.start != next || len(segment.valid) != len(segment.vectors) ||
			(c.normsReady && len(segment.norms) != len(segment.vectors)) || len(segment.vectors) > c.rows-next {
			return false
		}
		next += len(segment.vectors)
	}
	return next == c.rows
}

type vecColumnBuildCall struct{ done chan struct{} }

// vecColumnCacheMaxEntries bounds the column cache. Entries are keyed by
// (source identity, tenant, table, column) and each one pins its table,
// including every row. DROP TABLE purges eagerly (purgeVectorCachesFor),
// but replacements, renames and tenant removal may leave older identities
// behind. Without a cap those entries would remain for the life of the
// process. At capacity, arbitrary entries are evicted; the cost of a bad
// eviction is one lazy rebuild scan on the next query.
const vecColumnCacheMaxEntries = 256

var (
	vecSearchColumnCacheMu sync.RWMutex
	vecSearchColumnCache   = make(map[vecSearchColumnCacheKey]vecSearchColumnCacheEntry)
	// vecSearchColumnBuilds coalesces concurrent cold reads for the same
	// vector column. Without it, a RAG request burst can make every caller
	// scan and normalize the whole column before any one cache entry wins.
	vecSearchColumnBuilds = make(map[vecSearchColumnCacheKey]*vecColumnBuildCall)
	// vecSearchColumnCacheSnapshot mirrors vecSearchColumnCache for lock-free
	// reads — see publishVecSearchColumnCacheSnapshotLocked. A warm column
	// cache hit is the overwhelmingly common case on every single
	// VEC_SEARCH/RAG_SEARCH call, and used to pay an RLock/RUnlock pair on
	// vecSearchColumnCacheMu just for that (the same rationale
	// compileCachedLikeMatcher documents in like_cache.go for a per-row
	// cache; this one is per-query rather than per-row, but every concurrent
	// VEC_SEARCH/RAG_SEARCH call still contends on the same single mutex).
	vecSearchColumnCacheSnapshot atomic.Pointer[map[vecSearchColumnCacheKey]vecSearchColumnCacheEntry]
)

func init() {
	empty := make(map[vecSearchColumnCacheKey]vecSearchColumnCacheEntry)
	vecSearchColumnCacheSnapshot.Store(&empty)
}

// publishVecSearchColumnCacheSnapshotLocked refreshes the lock-free snapshot
// from the authoritative vecSearchColumnCache. Callers must already hold
// vecSearchColumnCacheMu and must call this immediately after every mutation
// of vecSearchColumnCache (insert or delete), so a lock-free reader can never
// observe a snapshot older than what the mutex-protected map already
// reflects — in particular, purgeVectorCachesFor's deletes must republish
// too, or a lock-free reader could keep returning an already-dropped table's
// entry forever. The map is capped at vecColumnCacheMaxEntries, so a full
// copy here is cheap relative to the cache-miss rebuild that triggered it.
func publishVecSearchColumnCacheSnapshotLocked() {
	snap := make(map[vecSearchColumnCacheKey]vecSearchColumnCacheEntry, len(vecSearchColumnCache))
	for k, v := range vecSearchColumnCache {
		snap[k] = v
	}
	vecSearchColumnCacheSnapshot.Store(&snap)
}

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
	publishVecSearchColumnCacheSnapshotLocked()
	vecSearchColumnCacheMu.Unlock()

	vecIVFCacheMu.Lock()
	for k := range vecIVFCache {
		if k.tenant == tenant && k.table == table {
			delete(vecIVFCache, k)
		}
	}
	publishVecIVFCacheSnapshotLocked()
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
// evictOverCap forwards to sqlval.EvictOverCap; see the forwarder note on
// valueText (coerce.go) for why the implementation lives outside this package.
func evictOverCap[K comparable, V any](m map[K]V, maxEntries int) {
	sqlval.EvictOverCap(m, maxEntries)
}

// Published segments and overrides are immutable, so a lock-free snapshot
// can return a ready column without committing the caller to a cache build.
func currentVecColumnCache(key vecSearchColumnCacheKey, includeNorms bool) (vecSearchColumnCacheEntry, bool) {
	if snap := vecSearchColumnCacheSnapshot.Load(); snap != nil {
		if cached, ok := (*snap)[key]; ok && cached.table == key.source && cached.version == key.source.Version && (!includeNorms || cached.normsReady) {
			return cached, true
		}
	}
	return vecSearchColumnCacheEntry{}, false
}

func getVecColumnCache(tenant string, table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	key := vecSearchColumnCacheKey{source: table, tenant: tenant, table: table.Name, colIdx: colIdx}
	if cached, ok := currentVecColumnCache(key, includeNorms); ok {
		return cached
	}

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
		cached, exists := vecSearchColumnCache[key]
		call := &vecColumnBuildCall{done: make(chan struct{})}
		vecSearchColumnBuilds[key] = call
		vecSearchColumnCacheMu.Unlock()

		// A panic in a build below must not leave the call registered: every
		// later search of this column would wait for it forever.
		published := false
		defer func() {
			if !published {
				vecSearchColumnCacheMu.Lock()
				delete(vecSearchColumnBuilds, key)
				close(call.done)
				vecSearchColumnCacheMu.Unlock()
			}
		}()
		// Read the change log outside the shared cache lock. Even an update
		// that leaves embeddings alone may have changed thousands of rows.
		interest := storage.ColumnBit(colIdx)
		usable := exists && cached.table == table && cached.rows <= len(table.Rows)
		var updatedRows []int
		canRefresh := false
		if usable {
			updatedRows, canRefresh = table.UpdatedRowsSinceCols(cached.structVersion, interest)
		}
		canExtend := canRefresh && len(updatedRows) == 0 && cached.version <= table.Version
		var removal storage.TableChanges
		canFollow := false
		if exists && cached.table == table && !canRefresh {
			var vouched bool
			removal, vouched = table.ChangesSince(cached.structVersion, cached.rows, interest)
			switch {
			case !vouched:
			case len(removal.Dead) > 0:
				canFollow = true
			default:
				// The window removed rows, but only ones the entry never covered
				// (appended since and deleted again, a failed load cleaned up, say):
				// every covered row is where it was, so this is the append-and-update
				// case after all.
				usable, canRefresh = true, true
				updatedRows = removal.Updated
				canExtend = len(updatedRows) == 0 && cached.version <= table.Version
			}
		}

		var entry vecSearchColumnCacheEntry
		if canExtend {
			// A stale entry can be safely extended when no structural change
			// touched this column (the table grew by appending rows, or other
			// columns were updated): its stamps move forward and only the
			// appended tail is read.
			entry = extendVecColumnCache(cached, table, colIdx, includeNorms)
		} else if canRefresh && usable {
			entry = refreshVecColumnCache(cached, table, colIdx, includeNorms, updatedRows)
		} else if canFollow {
			var followed bool
			if entry, followed = followVecColumnCache(cached, table, colIdx, includeNorms, removal); !followed {
				entry = buildVecColumnCache(table, colIdx, includeNorms || cached.normsReady)
			}
		} else {
			entry = buildVecColumnCache(table, colIdx, includeNorms)
		}
		vecSearchColumnCacheMu.Lock()
		if _, exists := vecSearchColumnCache[key]; !exists {
			evictOverCap(vecSearchColumnCache, vecColumnCacheMaxEntries)
		}
		vecSearchColumnCache[key] = entry
		publishVecSearchColumnCacheSnapshotLocked()
		delete(vecSearchColumnBuilds, key)
		close(call.done)
		published = true
		vecSearchColumnCacheMu.Unlock()
		return entry
	}
}

func refreshVecColumnCache(cached vecSearchColumnCacheEntry, table *storage.Table, colIdx int, includeNorms bool, rows []int) vecSearchColumnCacheEntry {
	normsReady := includeNorms || cached.normsReady
	// The change log returns distinct rows. Only changes inside the published
	// prefix need overrides; extendVecColumnCache reads the current tail.
	covered := min(cached.rows, len(table.Rows))
	limit := vecColumnOverrideLimit(len(table.Rows))
	remaining := limit - len(cached.overrides)
	if len(rows) > remaining {
		for _, row := range rows {
			if row < 0 || row >= covered {
				continue
			}
			if _, exists := cached.overrides[row]; !exists {
				remaining--
			}
			if remaining < 0 {
				break
			}
		}
	}
	if remaining < 0 {
		if len(cached.segments) > 1 && covered == cached.rows && cached.adjacentSegments(0, covered) {
			return repackVecColumnUpdates(cached, table, colIdx, normsReady, rows)
		}
		return buildVecColumnCache(table, colIdx, normsReady)
	}
	entry := extendVecColumnCache(cached, table, colIdx, includeNorms)
	if entry.shape == cached.shape {
		entry.shape = new(vecColumnShapeCache)
	}
	// Append-only views share immutable overrides. A norm upgrade already
	// created a private map; otherwise take a copy before changing row values.
	if len(cached.overrides) == 0 {
		entry.overrides = make(map[int]vecColumnOverride, min(len(rows), limit))
	} else if !includeNorms || cached.normsReady {
		entry.overrides = maps.Clone(cached.overrides)
	}
	for _, row := range rows {
		if row < 0 || row >= covered {
			continue
		}
		entry.overrides[row] = vecColumnOverrideOf(table.Rows[row], colIdx, normsReady)
	}
	// A refresh only adds or replaces overrides, so an unchanged count can
	// keep sharing the published row list.
	if len(entry.overrides) != len(entry.overrideRows) {
		entry.overrideRows = make([]int, 0, len(entry.overrides))
		for row := range entry.overrides {
			entry.overrideRows = append(entry.overrideRows, row)
		}
		slices.Sort(entry.overrideRows)
	}
	return entry
}

// repackVecColumnUpdates replaces the touched segments of a complete cache.
// The old segments and overrides may still be in use by concurrent searches.
func repackVecColumnUpdates(cached vecSearchColumnCacheEntry, table *storage.Table, colIdx int, normsReady bool, rows []int) vecSearchColumnCacheEntry {
	segmentIndex := func(row int) int {
		return sort.Search(len(cached.segments), func(i int) bool {
			segment := cached.segments[i]
			return row < segment.start+len(segment.vectors)
		})
	}
	changed := make([]bool, len(cached.segments))
	for _, row := range rows {
		if row >= 0 && row < cached.rows {
			changed[segmentIndex(row)] = true
		}
	}
	entry := vecSearchColumnCacheEntry{
		table:         table,
		version:       table.Version,
		structVersion: table.StructVersion(),
		rows:          len(table.Rows),
		shape:         new(vecColumnShapeCache),
		segments:      make([]vecColumnSegment, 0, len(cached.segments)),
		normsReady:    normsReady,
	}
	upgradeNorms := normsReady && !cached.normsReady
	for i, segment := range cached.segments {
		if changed[i] {
			end := segment.start + len(segment.vectors)
			entry.segments = appendVecColumnSegments(entry.segments, table.Rows[segment.start:end], colIdx, normsReady, segment.start)
			continue
		}
		if upgradeNorms {
			segment.norms = vecSegmentNorms(segment)
		}
		entry.segments = append(entry.segments, segment)
	}
	if cached.rows < entry.rows {
		entry.segments = appendVecColumnSegments(entry.segments, table.Rows[cached.rows:], colIdx, normsReady, cached.rows)
	}
	entry.segments = limitVecColumnSegments(entry.segments, normsReady)
	for _, row := range cached.overrideRows {
		if changed[segmentIndex(row)] {
			continue
		}
		value := cached.overrides[row]
		if upgradeNorms && value.valid {
			value.norm = vectorL2Norm(value.vector)
		}
		if entry.overrides == nil {
			entry.overrides = make(map[int]vecColumnOverride, len(cached.overrides))
		}
		entry.overrides[row] = value
		entry.overrideRows = append(entry.overrideRows, row)
	}
	return entry
}

func vecColumnOverridesNeedCompaction(entry vecSearchColumnCacheEntry) bool {
	return len(entry.overrides) > vecColumnOverrideLimit(entry.rows)
}

// vecColumnOverrideLimit is how many overridden rows a cache of rows rows keeps
// before a rebuild is the better deal.
func vecColumnOverrideLimit(rows int) int {
	limit := rows / 8
	if limit < 32 {
		limit = 32
	}
	if limit > 1024 {
		limit = 1024
	}
	return limit
}

// buildVecColumnCache extracts one table column into immutable, adjacent
// segments. Larger columns are packed in bounded segments so later appends and
// removals can keep the unchanged parts of the cache.
//
// This is a two-pass build: pass 1 classifies each row exactly as before
// (skip missing/nil cells, skip whatever vecRowValue rejects) and tallies the
// total float64 count; pass 2 makes one allocation for the segment data and
// copies each valid row into its own tightly-packed region, in row order.
// Two passes (rather than growing one slice with append) avoid repeated
// reallocate-and-recopy of the whole buffer as it grows — exactly the
// allocator churn this rebuild is meant to eliminate. Rows are packed by
// their own true length, not a fixed dims*len(table.Rows) stride: a column
// mid-embedding-migration can be ragged (mixed lengths across rows), and a
// row whose length doesn't match the query is excluded at search time
// (vecSearchTopKRange, validCacheRow), never truncated or hard-errored here.
func buildVecColumnCache(table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	return vecSearchColumnCacheEntry{
		table:         table,
		version:       table.Version,
		structVersion: table.StructVersion(),
		rows:          len(table.Rows),
		shape:         new(vecColumnShapeCache),
		segments:      appendVecColumnSegments(nil, table.Rows, colIdx, includeNorms, 0),
		normsReady:    includeNorms,
	}
}

const (
	vecColumnSegmentMaxFloats = (32 << 20) / 8
	vecColumnSegmentMaxRows   = 64 << 10
)

// appendVecColumnSegments bounds both the packed allocation and the arrays of
// row metadata. An individual vector larger than the budget gets its own
// segment. String cells can include legacy JSON vectors. Their byte length
// gives an upper bound without parsing an embedding twice.
func appendVecColumnSegments(dst []vecColumnSegment, rows [][]any, colIdx int, includeNorms bool, start int) []vecColumnSegment {
	from, floats := 0, 0
	for i, row := range rows {
		dims := 0
		if colIdx < len(row) {
			switch value := row[colIdx].(type) {
			case []float64:
				dims = len(value)
			case string:
				dims = len(value) / 2 // a JSON coordinate needs at least a digit and a separator
			}
		}
		if i > from && (dims > vecColumnSegmentMaxFloats-floats || i-from >= vecColumnSegmentMaxRows) {
			dst = append(dst, buildVecColumnSegment(rows[from:i], colIdx, includeNorms, start+from))
			from, floats = i, 0
		}
		floats += dims
	}
	return append(dst, buildVecColumnSegment(rows[from:], colIdx, includeNorms, start+from))
}

// buildVecColumnSegment packs one column of rows into a segment, spreading
// larger builds across cores while preserving the serial build's results.
func buildVecColumnSegment(rows [][]any, colIdx int, includeNorms bool, start int) vecColumnSegment {
	if workers := vecBuildWorkerCount(len(rows)); workers > 1 {
		return buildVecColumnSegmentParallel(rows, colIdx, includeNorms, start, workers)
	}
	return buildVecColumnSegmentSerial(rows, colIdx, includeNorms, start)
}

const (
	// vecBuildParallelMinRows is where spreading the build over several cores
	// starts to pay for starting them; below it (an INSERT's tail segment, a small
	// table) the serial build is used.
	vecBuildParallelMinRows = 2048
	// vecBuildChunkRows is the unit of work handed to a worker, small enough to
	// balance ragged data and large enough to keep scheduling negligible.
	vecBuildChunkRows = 1024
)

func vecBuildWorkerCount(rows int) int {
	if rows < vecBuildParallelMinRows {
		return 1
	}
	return min(runtime.GOMAXPROCS(0), rows/vecBuildChunkRows)
}

// buildVecColumnSegmentParallel is the same two-pass build as the serial one with
// both passes spread over workers goroutines on disjoint row ranges: pass one
// classifies the rows of each chunk and counts its floats, a prefix sum over the
// chunk totals gives every chunk its offset in the packed array, pass two copies
// and takes norms. Chunks are contiguous and processed in row order inside, so
// the packed layout (and every norm) is exactly the serial build's.
func buildVecColumnSegmentParallel(rows [][]any, colIdx int, includeNorms bool, start, workers int) vecColumnSegment {
	n := len(rows)
	valid := make([]bool, n)
	vectors := make([][]float64, n)
	chunks := (n + vecBuildChunkRows - 1) / vecBuildChunkRows
	totals := make([]int, chunks+1) // totals[c+1] is chunk c's float count, then its prefix sum

	runChunks := func(work func(c, from, to int)) {
		var next atomic.Int64
		var wg sync.WaitGroup
		for range min(workers, chunks) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					c := int(next.Add(1)) - 1
					if c >= chunks {
						return
					}
					work(c, c*vecBuildChunkRows, min((c+1)*vecBuildChunkRows, n))
				}
			}()
		}
		wg.Wait()
	}

	runChunks(func(c, from, to int) {
		total := 0
		for i := from; i < to; i++ {
			r := rows[i]
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
		totals[c+1] = total
	})
	for c := 1; c <= chunks; c++ {
		totals[c] += totals[c-1]
	}

	data := make([]float64, totals[chunks])
	var norms []float64
	if includeNorms {
		norms = make([]float64, n)
	}
	runChunks(func(c, from, to int) {
		cursor := totals[c]
		for i := from; i < to; i++ {
			if !valid[i] {
				continue
			}
			vec := vectors[i]
			dst := data[cursor : cursor+len(vec) : cursor+len(vec)]
			copy(dst, vec)
			vectors[i] = dst
			if includeNorms {
				norms[i] = vectorL2Norm(dst)
			}
			cursor += len(vec)
		}
	})
	return vecColumnSegment{start: start, data: data, vectors: vectors, norms: norms, valid: valid}
}

// buildVecColumnSegmentSerial is the reference build, used for small inputs.
func buildVecColumnSegmentSerial(rows [][]any, colIdx int, includeNorms bool, start int) vecColumnSegment {
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

// extendVecColumnCache returns a new immutable cache view that shares old
// segments and only builds the rows appended since cached.rows.  If cosine
// norms are requested for a previously non-cosine cache, it computes norms
// from the existing packed vectors but still never recopies vector data.
func extendVecColumnCache(cached vecSearchColumnCacheEntry, table *storage.Table, colIdx int, includeNorms bool) vecSearchColumnCacheEntry {
	rows := len(table.Rows)
	upgradeNorms := includeNorms && !cached.normsReady
	cached.table = table
	cached.version = table.Version
	cached.structVersion = table.StructVersion()
	if cached.shape == nil || cached.rows != rows {
		cached.shape = new(vecColumnShapeCache)
	}
	if cached.rows == rows && !upgradeNorms {
		return cached
	}
	segments := append([]vecColumnSegment(nil), cached.segments...)
	if upgradeNorms {
		for i := range segments {
			segments[i].norms = vecSegmentNorms(segments[i])
		}
	}
	cached.normsReady = cached.normsReady || includeNorms
	if cached.rows < rows {
		segments = appendVecColumnSegments(segments, table.Rows[cached.rows:], colIdx, cached.normsReady, cached.rows)
		segments = limitVecColumnSegments(mergeVecColumnSegments(segments, cached.normsReady), cached.normsReady)
	}
	cached.segments = segments
	cached.rows = rows
	// Appends do not change existing row overrides or computed norms. Share
	// their immutable map; refresh takes a private copy before a later UPDATE.
	if len(cached.overrides) != 0 && upgradeNorms {
		overrides := make(map[int]vecColumnOverride, len(cached.overrides))
		for row, value := range cached.overrides {
			if value.valid {
				value.norm = vectorL2Norm(value.vector)
			}
			overrides[row] = value
		}
		cached.overrides = overrides
	}
	return cached
}

// mergeVecColumnSegments joins similarly sized tail segments while they fit
// the segment budget. Single-row ingestion copies only O(log appendedRows)
// small tail segments amortized.
func mergeVecColumnSegments(segments []vecColumnSegment, normsReady bool) []vecColumnSegment {
	for len(segments) >= 2 {
		right := segments[len(segments)-1]
		left := segments[len(segments)-2]
		if len(left.vectors) == 0 || len(left.vectors) != len(right.vectors) || !vecColumnSegmentsFit(left, right) {
			break
		}
		last := len(segments) - 1
		segments[last-1] = repackVecColumnSegments(normsReady, left, right)
		// The header slice is owned by this extension; release the superseded
		// segment's packed arrays instead of keeping them in its unused tail.
		segments[last] = vecColumnSegment{}
		segments = segments[:last]
	}
	return segments
}

func vecColumnSegmentsFit(left, right vecColumnSegment) bool {
	return len(left.vectors)+len(right.vectors) <= vecColumnSegmentMaxRows &&
		len(left.data)-left.garbage+len(right.data)-right.garbage <= vecColumnSegmentMaxFloats
}

// repackVecColumnSegments compacts the live values of one or more adjacent
// segments into a single segment. The originals may still be held by readers.
func repackVecColumnSegments(normsReady bool, segments ...vecColumnSegment) vecColumnSegment {
	rows, total := 0, 0
	for _, segment := range segments {
		rows += len(segment.vectors)
		total += len(segment.data) - segment.garbage
	}
	valid := make([]bool, rows)
	vectors := make([][]float64, rows)
	data := make([]float64, total)
	var norms []float64
	if normsReady {
		norms = make([]float64, rows)
	}
	cursor, offset := 0, 0
	for _, segment := range segments {
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
		offset += len(segment.vectors)
	}
	return vecColumnSegment{start: segments[0].start, data: data, vectors: vectors, norms: norms, valid: valid}
}

func vectorL2Norm(v []float64) float64 {
	return search.VectorNorm(v)
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

// newScoredHeap returns a top-k heap with room for k rows already reserved.
// pushTopK caps the heap at k, so that is its final size; growing into it from
// nil made the append chain one of the larger allocation sites in vector
// search. capHint bounds the reservation when the caller knows it cannot
// produce k rows -- a scan range shorter than k, say -- so a large k over a
// small table does not over-reserve.
func newScoredHeap(k, capHint int) *vecScoredHeap {
	if capHint >= 0 && capHint < k {
		k = capHint
	}
	if k < 0 {
		k = 0
	}
	h := make(vecScoredHeap, 0, k)
	return &h
}

func topKFromHeap(heapRows *vecScoredHeap, k int) []vecScoredRow {
	if k > heapRows.Len() {
		k = heapRows.Len()
	}
	if k <= 0 {
		return nil
	}
	if k == heapRows.Len() {
		if rows, ok := takeOrderedVecHeap(heapRows); ok {
			return rows
		}
		// Pop already moves each worst entry to the end of the backing slice.
		// Draining the whole heap therefore sorts that storage best-first.
		rows := []vecScoredRow(*heapRows)
		for heapRows.Len() > 0 {
			vecScoredHeapPop(heapRows)
		}
		// Transfer ownership: a later push must not overwrite returned rows.
		*heapRows = nil
		return rows
	}
	rows := make([]vecScoredRow, k)
	for i := k - 1; i >= 0; i-- {
		rows[i] = vecScoredHeapPop(heapRows)
	}
	return rows
}

// vecDistanceFunc computes a candidate row's ranking distance from the query
// vector, given the row's already-resolved vector and (for metrics that need
// one) its precomputed norm. Every caller resolves both through resolveRow
// exactly once per row before invoking this, instead of this closure
// re-resolving validity/norm itself as it used to for the cosine metric.
type vecDistanceFunc func(vec []float64, norm float64) (float64, bool)

// vecDistanceFuncNeedsNorm reports whether metric's distance function reads
// its norm argument, so a caller knows whether resolveRow needs to bother
// resolving a row's norm at all.
func vecDistanceFuncNeedsNorm(metric string) bool { return metric == "cosine" }

// Resolve the metric once per query. Each specialized path preserves the
// generic length and NaN checks while avoiding dispatch for every candidate.
func buildVecDistanceFunc(metric string, query []float64, queryNorm float64) vecDistanceFunc {
	switch metric {
	case "cosine":
		if queryNorm == 0 {
			return func([]float64, float64) (float64, bool) { return 0, false }
		}
		return func(vec []float64, norm float64) (float64, bool) {
			return vecCosineRankingDistance(vec, query, norm, queryNorm)
		}
	case "l2":
		return func(vec []float64, _ float64) (float64, bool) {
			if len(vec) != len(query) {
				return 0, false
			}
			distance := search.VectorL2Squared(vec, query)
			if math.IsNaN(distance) {
				return 0, false
			}
			return distance, true
		}
	case "manhattan":
		return func(vec []float64, _ float64) (float64, bool) {
			if len(vec) != len(query) {
				return 0, false
			}
			distance := search.VectorL1Distance(vec, query)
			if math.IsNaN(distance) {
				return 0, false
			}
			return distance, true
		}
	case "dot":
		return func(vec []float64, _ float64) (float64, bool) {
			if len(vec) != len(query) {
				return 0, false
			}
			distance := -search.VectorDot(vec, query)
			if math.IsNaN(distance) {
				return 0, false
			}
			return distance, true
		}
	default:
		return func([]float64, float64) (float64, bool) { return 0, false }
	}
}

// vecCosineRankingDistance is the cosine branch of VectorRankingDistance without
// its per-row metric dispatch. Cosine is RAG_SEARCH's default and every flat
// candidate otherwise passes through VectorRankingDistance and VectorDistance
// just to select this same formula. Keeping the length and NaN checks here
// preserves the generic path's exclusion semantics for malformed vectors.
//
// This matters especially on Linux/amd64: AVX2/FMA makes the dot product fast
// enough that the generic function/switch chain becomes measurable overhead.
func vecCosineRankingDistance(vec, query []float64, norm, queryNorm float64) (float64, bool) {
	similarity, ok := search.VectorCosineWithNorms(vec, query, norm, queryNorm)
	if !ok {
		return 0, false
	}
	distance := 1.0 - similarity
	if math.IsNaN(distance) {
		return 0, false
	}
	return distance, true
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

type vecFlatScan struct {
	cache    vecSearchColumnCacheEntry
	distance vecDistanceFunc
	needNorm bool
	l2Query  []float64 // non-nil only for adjacent segments without overrides
}

func vecSearchFlat(ctx context.Context, rows [][]any, metric string, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry) ([]vecScoredRow, error) {
	if metric == "l2" && k > 0 && len(query) != 0 && len(rows) == cache.rows && cache.directScanSegments(0, len(rows)) {
		scan := vecFlatScan{cache: cache, l2Query: query}
		return scan.topK(ctx, rows, len(query), k)
	}
	return vecSearchTopK(ctx, rows, len(query), k, cache, buildVecDistanceFunc(metric, query, queryNorm), vecDistanceFuncNeedsNorm(metric))
}

func vecSearchTopK(ctx context.Context, rows [][]any, queryLen int, k int, cache vecSearchColumnCacheEntry, distFn vecDistanceFunc, needNorm bool) ([]vecScoredRow, error) {
	scan := vecFlatScan{cache: cache, distance: distFn, needNorm: needNorm}
	return scan.topK(ctx, rows, queryLen, k)
}

func (scan *vecFlatScan) scanRange(ctx context.Context, rows [][]any, start, end, queryLen, k int) (vecScoredHeap, error) {
	if scan.l2Query != nil {
		return scanVecL2Range(ctx, scan.cache.segments, scan.l2Query, start, end, k)
	}
	return vecSearchTopKRange(ctx, rows, start, end, queryLen, k, scan.cache, scan.distance, scan.needNorm)
}

func (scan vecFlatScan) topK(ctx context.Context, rows [][]any, queryLen, k int) ([]vecScoredRow, error) {
	workers := vecSearchWorkerCount(len(rows), queryLen)
	if workers == 1 {
		h, err := scan.scanRange(ctx, rows, 0, len(rows), queryLen, k)
		if err != nil {
			return nil, err
		}
		return topKFromHeap(&h, k), nil
	}
	return scan.topKParallel(ctx, rows, queryLen, k, workers)
}

func (scan vecFlatScan) topKParallel(ctx context.Context, rows [][]any, queryLen, k, workers int) ([]vecScoredRow, error) {
	results := make([]vecSearchWorkerResult, workers)
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
		run := func() {
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
			h, err := scan.scanRange(ctx, rows, start, end, queryLen, k)
			results[worker] = vecSearchWorkerResult{heapRows: h, err: err}
		}
		// Use the caller as one worker instead of parking it for the whole scan.
		if worker == workers-1 {
			run()
		} else {
			go run()
		}
	}
	wg.Wait()

	return mergeVecSearchResults(results, k, len(rows))
}

func vecSearchTopKRange(ctx context.Context, _ [][]any, start, end, queryLen, k int, cache vecSearchColumnCacheEntry, distFn vecDistanceFunc, needNorm bool) (vecScoredHeap, error) {
	scoredRows := newScoredHeap(k, end-start)
	if k >= vecTopKBatchMin && end-start >= vecTopKBatchMin {
		var err error
		start, err = fillVecTopKBatch(ctx, start, end, queryLen, k, &cache, distFn, needNorm, scoredRows)
		if err != nil {
			return nil, err
		}
	}
	if len(cache.overrides) == len(cache.overrideRows) && cache.adjacentSegments(start, end) {
		// Overlay only changed rows while scanning the packed base sequentially.
		// Skip overrides before this worker's slice and probe the map only at
		// a matching row ID.
		overrides := cache.overrideRows[sort.SearchInts(cache.overrideRows, start):]
		scan := func(segment vecColumnSegment, from, to int) error {
			for i := from; i < to; i++ {
				if i&1023 == 0 {
					if err := checkCtx(ctx); err != nil {
						return err
					}
				}
				local := i - segment.start
				if !segment.valid[local] {
					continue
				}
				vec := segment.vectors[local]
				if len(vec) != queryLen {
					continue
				}
				var norm float64
				if needNorm {
					if cache.normsReady {
						norm = segment.norms[local]
					} else {
						norm = vectorL2Norm(vec)
					}
				}
				dist, ok := distFn(vec, norm)
				if !ok {
					continue
				}
				pushTopK(scoredRows, i, dist, k)
			}
			return nil
		}
		for _, segment := range cache.segments {
			from := max(start, segment.start)
			to := min(end, segment.start+len(segment.vectors))
			if from >= to {
				continue
			}
			for len(overrides) != 0 && overrides[0] < to {
				row := overrides[0]
				if err := scan(segment, from, row); err != nil {
					return nil, err
				}
				if row&1023 == 0 {
					if err := checkCtx(ctx); err != nil {
						return nil, err
					}
				}
				vec, norm, valid := cache.resolveRow(row, needNorm)
				if valid && len(vec) == queryLen {
					if dist, ok := distFn(vec, norm); ok {
						pushTopK(scoredRows, row, dist, k)
					}
				}
				overrides = overrides[1:]
				from = row + 1
			}
			if err := scan(segment, from, to); err != nil {
				return nil, err
			}
		}
		return *scoredRows, nil
	}

	for i := start; i < end; i++ {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		vec, norm, valid := cache.resolveRow(i, needNorm)
		if !valid || len(vec) != queryLen {
			continue
		}
		dist, ok := distFn(vec, norm)
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
	return findVecSearchCandidates(ctx, env, a, false)
}

func vecSearchSource(db *storage.DB, tenant string, a vecSearchArgs) (*storage.Table, int, error) {
	table, err := db.Get(tenant, a.tableName)
	if err != nil {
		return nil, 0, fmt.Errorf("VEC_SEARCH: table %q not found: %w", a.tableName, err)
	}
	col, err := table.ColIndex(a.colName)
	if err != nil {
		return nil, 0, fmt.Errorf("VEC_SEARCH: %w", err)
	}
	return table, col, nil
}

func findVecSearchCandidates(ctx context.Context, env ExecEnv, a vecSearchArgs, allowConcurrent bool) (*storage.Table, []vecScoredRow, error) {
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, vecColIdx, err := vecSearchSource(env.db, tenant, a)
	if err != nil {
		return nil, nil, err
	}

	var queryNorm float64
	if a.metric == "cosine" {
		queryNorm = vectorL2Norm(a.queryVec)
	}

	searchCtx := ctx
	if searchCtx == nil {
		searchCtx = env.ctx
	}
	// Only capture a start time when analytics is actually enabled: it is off
	// by default, and every VectorQueryEvent field below built from it
	// (At, Duration) was previously computed and then discarded inside
	// recordVecQuery's own disabled fast path — two clock reads per
	// VEC_SEARCH call for nothing. See vecAnalyticsEnabled.
	analyticsOn := vecAnalyticsEnabled()
	var started time.Time
	if analyticsOn {
		started = time.Now()
	}
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
		key = vecQueryKey(tenant, table, a.colName, a)
		scoredRowsOrdered, cacheHit = getVecQueryCache(key)
	}
	if !cacheHit {
		cache := getVecColumnCache(tenant, table, vecColIdx, a.metric == "cosine")
		if allowConcurrent && !cacheEnabled && a.indexMode == vecIndexFlat &&
			largeVecScan(cache.rows, len(a.queryVec)) && storage.ResidentTable(env.db, tenant, a.tableName) == table {
			table, scoredRowsOrdered, err = rankVecSearchAlongsideWrites(searchCtx, env, tenant, a, queryNorm, table, vecColIdx, cache)
		} else {
			scoredRowsOrdered, err = vecSearchTopKWithIndex(searchCtx, tenant, table, vecColIdx, a, queryNorm, cache)
		}
		if err != nil {
			return nil, nil, err
		}
		if cacheEnabled {
			putVecQueryCache(key, scoredRowsOrdered)
		}
	}
	if analyticsOn {
		recordVecQuery(VectorQueryEvent{At: time.Now(), Table: table.Name, Column: a.colName, Metric: a.metric, Index: a.indexMode, K: a.k, CacheHit: cacheHit, Duration: time.Since(started)})
	}
	return table, scoredRowsOrdered, nil
}

func validateVecMaxDistance(metric string, limit *float64) error {
	if limit == nil {
		return nil
	}
	if math.IsNaN(*limit) || math.IsInf(*limit, 0) {
		return fmt.Errorf("max_distance must be finite")
	}
	if metric != "dot" && *limit < 0 {
		return fmt.Errorf("max_distance must be nonnegative for metric %s", metric)
	}
	return nil
}

// trimVecDistance keeps a prefix of closest-first candidates in output
// distance units. Reslicing leaves cached rankings and ANN traversal intact.
func trimVecDistance(rows []vecScoredRow, limit *float64) []vecScoredRow {
	if limit == nil || len(rows) == 0 || rows[len(rows)-1].distance <= *limit {
		return rows
	}
	end := sort.Search(len(rows), func(i int) bool { return rows[i].distance > *limit })
	return rows[:end]
}

func materializeVecCandidates(table *storage.Table, scoredRowsOrdered []vecScoredRow, metric string) *ResultSet {
	resultCols := make([]string, 0, len(table.Cols)+3)
	// Lower-cased once per column up front: table.Cols is loop-invariant
	// across every scored row below, so ToLower-ing it per (row, column) pair
	// redid the same string conversion len(scoredRowsOrdered) times over.
	var lowerScratch [16]string
	lowerCols := lowerScratch[:]
	if len(table.Cols) > len(lowerCols) {
		lowerCols = make([]string, len(table.Cols))
	} else {
		lowerCols = lowerCols[:len(table.Cols)]
	}
	for i, c := range table.Cols {
		resultCols = append(resultCols, c.Name)
		lowerCols[i] = strings.ToLower(c.Name)
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
		for ci := range table.Cols {
			if ci < len(table.Rows[sr.rowIdx]) {
				// Lower-cased to match ragFuseCandidates and ragCopyOutputRow.
				// ragValue looks up the lower-cased key first and only then
				// falls back to an EqualFold scan over every key, so writing
				// the schema's own casing here put every lookup against an
				// upper-case schema on the slow path -- and made a vector-only
				// RAG_SEARCH hand back differently-keyed rows than a hybrid one.
				r[lowerCols[ci]] = table.Rows[sr.rowIdx][ci]
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
	dist, ok := search.VectorDistance(normalized, a, b, normA, normB)
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
