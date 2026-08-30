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
	if call.err == nil && call.idx != nil {
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
	idx := &vecHNSWIndex{
		table: table, version: table.Version, structVersion: table.StructVersion(),
		metric: metric, dims: dims, entry: -1, maxLevel: -1,
		levels: make([]int, cache.rowCount()), neighbors: make([][][]int, cache.rowCount()),
	}
	visited := acquireVisited(cache.rowCount())
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for pos, rowIdx := range filter.rows {
		if pos&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if validCacheRow(cache, rowIdx, dims) {
			idx.insertHNSWNode(rowIdx, cache, visited, scratch)
		}
	}
	return idx, nil
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
	current := idx.entry
	rowCount := cache.rowCount()
	visited := acquireVisited(rowCount)
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for layer := idx.maxLevel; layer > 0; layer-- {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		best := idx.searchLayer(query, queryNorm, current, 1, layer, cache, visited, scratch)
		if len(best) > 0 {
			current = best[0].rowIdx
		}
	}
	candidates := idx.searchLayer(query, queryNorm, current, chooseHNSWEfSearch(k), 0, cache, visited, scratch)
	result := newScoredHeap(k, len(candidates))
	for _, sr := range candidates {
		pushTopK(result, sr.rowIdx, sr.distance, k)
	}
	if result.Len() < k {
		distFn := buildVecDistanceFunc(idx.metric, query, queryNorm)
		return ragVecTopKAllowed(ctx, allowed.rows, len(query), k, cache, distFn, vecDistanceFuncNeedsNorm(idx.metric))
	}
	return topKFromHeap(result, k), nil
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
