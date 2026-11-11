package engine

import (
	"context"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Filter-local HNSW graphs are intentionally process-local. They model an ACL
// or tenant slice and must be built before searching, never by filtering a
// global ANN frontier afterwards. That preserves both isolation and recall
// within the allowed set.
const maxRAGFilteredANNCaches = 64

type ragFilteredANNKey struct {
	tenant  string
	table   *storage.Table
	version int
	colIdx  int
	metric  string
	filter  *ragRowFilter
}

type ragFilteredANNBuildCall struct {
	done chan struct{}
	idx  *vecHNSWIndex
	err  error
}

var ragFilteredANNCache = struct {
	sync.RWMutex
	entries map[ragFilteredANNKey]*vecHNSWIndex
	builds  map[ragFilteredANNKey]*ragFilteredANNBuildCall
}{
	entries: make(map[ragFilteredANNKey]*vecHNSWIndex),
	builds:  make(map[ragFilteredANNKey]*ragFilteredANNBuildCall),
}

func getRAGFilteredANNIndex(ctx context.Context, tenant string, table *storage.Table, colIdx int, metric string, dims int, filter *ragRowFilter, cache vecSearchColumnCacheEntry) (*vecHNSWIndex, error) {
	key := ragFilteredANNKey{tenant: tenant, table: table, version: table.Version, colIdx: colIdx, metric: metric, filter: filter}
	ragFilteredANNCache.RLock()
	idx := ragFilteredANNCache.entries[key]
	ragFilteredANNCache.RUnlock()
	if idx != nil {
		return idx, nil
	}

	ragFilteredANNCache.Lock()
	if idx = ragFilteredANNCache.entries[key]; idx != nil {
		ragFilteredANNCache.Unlock()
		return idx, nil
	}
	if call := ragFilteredANNCache.builds[key]; call != nil {
		ragFilteredANNCache.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return call.idx, call.err
		}
	}
	call := &ragFilteredANNBuildCall{done: make(chan struct{})}
	ragFilteredANNCache.builds[key] = call
	ragFilteredANNCache.Unlock()

	call.idx, call.err = buildRAGFilteredANNIndex(ctx, table, metric, dims, cache, filter)

	ragFilteredANNCache.Lock()
	delete(ragFilteredANNCache.builds, key)
	if call.err == nil && call.idx != nil && table.Version == key.version {
		// Compact graphs borrow immutable vector payloads. Retire older
		// generations before publishing so a busy ACL cache cannot pin many
		// obsolete packed columns after updates or rebuilds.
		for stale := range ragFilteredANNCache.entries {
			if stale.tenant == key.tenant && stale.table == key.table && stale.version != key.version {
				delete(ragFilteredANNCache.entries, stale)
			}
		}
		if len(ragFilteredANNCache.entries) >= maxRAGFilteredANNCaches {
			for stale := range ragFilteredANNCache.entries {
				delete(ragFilteredANNCache.entries, stale)
				break
			}
		}
		ragFilteredANNCache.entries[key] = call.idx
	}
	close(call.done)
	ragFilteredANNCache.Unlock()
	return call.idx, call.err
}

func buildRAGFilteredANNIndex(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry, filter *ragRowFilter) (*vecHNSWIndex, error) {
	version, structVersion := table.Version, table.StructVersion()
	local, rowIDs, err := compactRAGFilterCache(ctx, cache, filter.rows, metricNeedsNorms(metric))
	if err != nil {
		return nil, err
	}
	buildCache := cache
	if local != nil {
		buildCache = *local
	}
	idx := &vecHNSWIndex{
		table: table, version: version, structVersion: structVersion,
		metric: metric, dims: dims, entry: -1, maxLevel: -1,
		levels: make([]int, buildCache.rowCount()), neighbors: make([][][]int, buildCache.rowCount()),
		filteredRows: rowIDs, filteredCache: local,
	}
	visited := acquireVisited(buildCache.rowCount())
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for pos, rowIdx := range filter.rows {
		if pos&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if local != nil {
			rowIdx = pos
		}
		if validCacheRow(buildCache, rowIdx, dims) {
			idx.insertHNSWNode(rowIdx, buildCache, visited, scratch)
		}
	}
	return idx, nil
}

// A selective graph should scale with the allowed set, not with its source
// corpus. This view copies vector headers and cached norms, never payloads.
// For broad filters the extra headers would cost more than they save, so keep
// the original physical-ID layout. Sorted IDs preserve every distance tie.
func compactRAGFilterCache(ctx context.Context, cache vecSearchColumnCacheEntry, rows []int, needNorm bool) (*vecSearchColumnCacheEntry, []int, error) {
	if len(rows) > cache.rowCount()/4 {
		return nil, nil, nil
	}
	previous := -1
	for pos, row := range rows {
		if pos&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, nil, err
			}
		}
		if row <= previous || row >= cache.rowCount() {
			return nil, nil, nil
		}
		previous = row
	}
	segment := vecColumnSegment{vectors: make([][]float64, len(rows)), valid: make([]bool, len(rows))}
	if needNorm {
		segment.norms = make([]float64, len(rows))
	}
	for pos, row := range rows {
		if pos&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, nil, err
			}
		}
		vec, norm, valid := cache.resolveRow(row, needNorm)
		segment.vectors[pos], segment.valid[pos] = vec, valid
		if needNorm {
			segment.norms[pos] = norm
		}
	}
	local := &vecSearchColumnCacheEntry{
		table: cache.table, version: cache.version, structVersion: cache.structVersion,
		rows: len(rows), segments: []vecColumnSegment{segment}, normsReady: needNorm,
	}
	return local, append([]int(nil), rows...), nil
}

// searchFiltered traverses a graph containing only allowed rows. The exact
// fallback is restricted to the same rows, so a filtered ANN search cannot
// expose candidates from a different tenant or ACL scope.
func (idx *vecHNSWIndex) searchFiltered(ctx context.Context, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry, allowed *ragRowFilter) ([]vecScoredRow, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.entry < 0 || k <= 0 {
		return nil, nil
	}
	searchCache := cache
	if idx.filteredCache != nil {
		searchCache = *idx.filteredCache
	}
	visited := acquireVisited(searchCache.rowCount())
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	scorer := newVecRowScorer(idx.metric, query, queryNorm, newVecRowResolver(&searchCache, metricNeedsNorms(idx.metric)))
	candidates, err := idx.searchCandidates(ctx, &scorer, chooseHNSWEfSearch(k), visited, scratch)
	if err != nil {
		return nil, err
	}
	if len(candidates) >= k {
		// Filter-local graphs have no delta rows. The traversal already sorts
		// candidates nearest first; copy the prefix out of pooled scratch.
		result := append([]vecScoredRow(nil), candidates[:k]...)
		if idx.filteredCache != nil {
			for i := range result {
				result[i].rowIdx = idx.filteredRows[result[i].rowIdx]
			}
		}
		return result, nil
	}
	distFn := buildVecDistanceFunc(idx.metric, query, queryNorm)
	return ragVecTopKAllowed(ctx, allowed.rows, len(query), k, cache, distFn, vecDistanceFuncNeedsNorm(idx.metric))
}

func purgeRAGFilteredANNCachesFor(tenant, table string) {
	ragFilteredANNCache.Lock()
	for key := range ragFilteredANNCache.entries {
		if key.table.Name == table && (tenant == "" || key.tenant == tenant) {
			delete(ragFilteredANNCache.entries, key)
		}
	}
	ragFilteredANNCache.Unlock()
}
