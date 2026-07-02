package engine

import (
	"container/heap"
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	vecIndexFlat = "flat"
	vecIndexIVF  = "ivf"
	vecIndexHNSW = "hnsw"

	vecIVFKMeansIters = 3

	vecHNSWM              = 12
	vecHNSWEfConstruction = 48
	vecHNSWMaxLevel       = 8
)

type vecIndexCacheKey struct {
	tenant string
	table  string
	colIdx int
	metric string
}

type vecIVFIndex struct {
	table         *storage.Table
	version       int
	metric        string
	dims          int
	centroids     [][]float64
	centroidNorms []float64
	lists         [][]int
}

type vecHNSWIndex struct {
	table     *storage.Table
	version   int
	metric    string
	dims      int
	entry     int
	maxLevel  int
	levels    []int
	neighbors [][][]int
}

type vecMinScoredHeap []vecScoredRow

func (h vecMinScoredHeap) Len() int { return len(h) }
func (h vecMinScoredHeap) Less(i, j int) bool {
	return vecScoredRowLess(h[i], h[j])
}
func (h vecMinScoredHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *vecMinScoredHeap) Push(x any)   { *h = append(*h, x.(vecScoredRow)) }
func (h *vecMinScoredHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
}

var (
	vecIVFCacheMu  sync.RWMutex
	vecIVFCache    = make(map[vecIndexCacheKey]*vecIVFIndex)
	vecHNSWCacheMu sync.RWMutex
	vecHNSWCache   = make(map[vecIndexCacheKey]*vecHNSWIndex)
)

func normalizeVecIndexMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "flat", "exact", "scan":
		return vecIndexFlat
	case "ivf":
		return vecIndexIVF
	case "hnsw":
		return vecIndexHNSW
	default:
		return ""
	}
}

func vecSearchTopKWithIndex(
	ctx context.Context,
	tenant string,
	table *storage.Table,
	colIdx int,
	args vecSearchArgs,
	queryLen int,
	queryNorm float64,
	cache vecSearchColumnCacheEntry,
	distFn vecDistanceFunc,
) ([]vecScoredRow, error) {
	switch args.indexMode {
	case vecIndexFlat:
		return vecSearchTopK(ctx, table.Rows, queryLen, args.k, cache, distFn)
	case vecIndexIVF:
		idx, err := getVecIVFIndex(ctx, tenant, table, colIdx, args.metric, queryLen, cache)
		if err != nil {
			return nil, err
		}
		return idx.search(ctx, args.queryVec, queryNorm, args.k, cache)
	case vecIndexHNSW:
		idx, err := getVecHNSWIndex(ctx, tenant, table, colIdx, args.metric, queryLen, cache)
		if err != nil {
			return nil, err
		}
		return idx.search(ctx, args.queryVec, queryNorm, args.k, cache)
	default:
		return vecSearchTopK(ctx, table.Rows, queryLen, args.k, cache, distFn)
	}
}

func getVecIVFIndex(ctx context.Context, tenant string, table *storage.Table, colIdx int, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecIVFIndex, error) {
	key := vecIndexCacheKey{tenant: tenant, table: table.Name, colIdx: colIdx, metric: metric}
	vecIVFCacheMu.RLock()
	if idx := vecIVFCache[key]; idx != nil && idx.table == table && idx.version == table.Version && idx.dims == dims {
		vecIVFCacheMu.RUnlock()
		return idx, nil
	}
	vecIVFCacheMu.RUnlock()

	idx, err := buildVecIVFIndex(ctx, table, metric, dims, cache)
	if err != nil {
		return nil, err
	}
	vecIVFCacheMu.Lock()
	vecIVFCache[key] = idx
	vecIVFCacheMu.Unlock()
	return idx, nil
}

func buildVecIVFIndex(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecIVFIndex, error) {
	rows := validVectorRows(cache, dims)
	nlist := chooseIVFListCount(len(rows))
	idx := &vecIVFIndex{table: table, version: table.Version, metric: metric, dims: dims}
	if len(rows) == 0 || nlist == 0 {
		return idx, nil
	}

	idx.centroids = make([][]float64, nlist)
	for i := range idx.centroids {
		src := cache.vectors[rows[(i*len(rows))/nlist]]
		idx.centroids[i] = append([]float64(nil), src...)
	}
	assignments := make([]int, len(rows))
	for iter := 0; iter < vecIVFKMeansIters; iter++ {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		idx.centroidNorms = centroidNorms(idx.centroids)
		sums := make([]float64, nlist*dims)
		counts := make([]int, nlist)
		for i, rowIdx := range rows {
			if i&1023 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			c := nearestCentroid(metric, cache.vectors[rowIdx], rowNorm(cache, rowIdx), idx.centroids, idx.centroidNorms)
			assignments[i] = c
			counts[c]++
			base := c * dims
			vec := cache.vectors[rowIdx]
			for d := 0; d < dims; d++ {
				sums[base+d] += vec[d]
			}
		}
		for c := range idx.centroids {
			if counts[c] == 0 {
				continue
			}
			inv := 1.0 / float64(counts[c])
			base := c * dims
			for d := 0; d < dims; d++ {
				idx.centroids[c][d] = sums[base+d] * inv
			}
		}
	}

	idx.centroidNorms = centroidNorms(idx.centroids)
	idx.lists = make([][]int, nlist)
	for i, rowIdx := range rows {
		c := assignments[i]
		idx.lists[c] = append(idx.lists[c], rowIdx)
	}
	return idx, nil
}

func (idx *vecIVFIndex) search(ctx context.Context, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry) ([]vecScoredRow, error) {
	if len(idx.centroids) == 0 {
		return nil, nil
	}
	probes := chooseIVFNProbe(len(idx.centroids), k)
	centroidHeap := &vecScoredHeap{}
	heap.Init(centroidHeap)
	for i, c := range idx.centroids {
		dist, ok := vectorDistance(idx.metric, c, query, idx.centroidNorms[i], queryNorm)
		if !ok {
			continue
		}
		pushTopK(centroidHeap, i, dist, probes)
	}
	bestCentroids := topKFromHeap(centroidHeap, probes)

	resultHeap := &vecScoredHeap{}
	heap.Init(resultHeap)
	for _, c := range bestCentroids {
		list := idx.lists[c.rowIdx]
		for i, rowIdx := range list {
			if i&1023 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			dist, ok := rowDistance(idx.metric, query, queryNorm, cache, rowIdx)
			if ok {
				pushTopK(resultHeap, rowIdx, dist, k)
			}
		}
	}
	return topKFromHeap(resultHeap, k), nil
}

func getVecHNSWIndex(ctx context.Context, tenant string, table *storage.Table, colIdx int, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecHNSWIndex, error) {
	key := vecIndexCacheKey{tenant: tenant, table: table.Name, colIdx: colIdx, metric: metric}
	vecHNSWCacheMu.RLock()
	if idx := vecHNSWCache[key]; idx != nil && idx.table == table && idx.version == table.Version && idx.dims == dims {
		vecHNSWCacheMu.RUnlock()
		return idx, nil
	}
	vecHNSWCacheMu.RUnlock()

	idx, err := buildVecHNSWIndex(ctx, table, metric, dims, cache)
	if err != nil {
		return nil, err
	}
	vecHNSWCacheMu.Lock()
	vecHNSWCache[key] = idx
	vecHNSWCacheMu.Unlock()
	return idx, nil
}

func buildVecHNSWIndex(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecHNSWIndex, error) {
	idx := &vecHNSWIndex{
		table:     table,
		version:   table.Version,
		metric:    metric,
		dims:      dims,
		entry:     -1,
		maxLevel:  -1,
		levels:    make([]int, len(cache.vectors)),
		neighbors: make([][][]int, len(cache.vectors)),
	}

	visited := make([]bool, len(cache.vectors))
	for rowIdx := range cache.vectors {
		if rowIdx&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if !validCacheRow(cache, rowIdx, dims) {
			continue
		}
		level := hnswLevel(rowIdx)
		idx.levels[rowIdx] = level
		idx.neighbors[rowIdx] = make([][]int, level+1)
		if idx.entry < 0 {
			idx.entry = rowIdx
			idx.maxLevel = level
			continue
		}

		current := idx.entry
		query := cache.vectors[rowIdx]
		queryNorm := rowNorm(cache, rowIdx)
		for layer := idx.maxLevel; layer > level; layer-- {
			best := idx.searchLayer(query, queryNorm, current, 1, layer, cache, visited)
			if len(best) > 0 {
				current = best[0].rowIdx
			}
		}
		upper := level
		if idx.maxLevel < upper {
			upper = idx.maxLevel
		}
		for layer := upper; layer >= 0; layer-- {
			candidates := idx.searchLayer(query, queryNorm, current, vecHNSWEfConstruction, layer, cache, visited)
			selected := selectHNSWNeighbors(candidates, vecHNSWM)
			for _, nb := range selected {
				idx.addHNSWLink(rowIdx, nb.rowIdx, layer, cache)
			}
			if len(selected) > 0 {
				current = selected[0].rowIdx
			}
		}
		if level > idx.maxLevel {
			idx.entry = rowIdx
			idx.maxLevel = level
		}
	}
	return idx, nil
}

func (idx *vecHNSWIndex) search(ctx context.Context, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry) ([]vecScoredRow, error) {
	if idx.entry < 0 {
		return nil, nil
	}
	current := idx.entry
	visited := make([]bool, len(cache.vectors))
	for layer := idx.maxLevel; layer > 0; layer-- {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		best := idx.searchLayer(query, queryNorm, current, 1, layer, cache, visited)
		if len(best) > 0 {
			current = best[0].rowIdx
		}
	}
	efSearch := chooseHNSWEfSearch(k)
	candidates := idx.searchLayer(query, queryNorm, current, efSearch, 0, cache, visited)
	resultHeap := &vecScoredHeap{}
	heap.Init(resultHeap)
	for _, sr := range candidates {
		pushTopK(resultHeap, sr.rowIdx, sr.distance, k)
	}
	return topKFromHeap(resultHeap, k), nil
}

func (idx *vecHNSWIndex) searchLayer(query []float64, queryNorm float64, entry int, ef int, layer int, cache vecSearchColumnCacheEntry, visited []bool) []vecScoredRow {
	if entry < 0 || ef <= 0 || !idx.hasLayer(entry, layer) {
		return nil
	}
	dist, ok := rowDistance(idx.metric, query, queryNorm, cache, entry)
	if !ok {
		return nil
	}
	if len(visited) < len(cache.vectors) {
		visited = make([]bool, len(cache.vectors))
	}
	touched := make([]int, 0, ef*vecHNSWM)
	markVisited := func(rowIdx int) bool {
		if rowIdx < 0 || rowIdx >= len(visited) || visited[rowIdx] {
			return false
		}
		visited[rowIdx] = true
		touched = append(touched, rowIdx)
		return true
	}
	markVisited(entry)
	defer func() {
		for _, rowIdx := range touched {
			visited[rowIdx] = false
		}
	}()
	candidates := &vecMinScoredHeap{}
	results := &vecScoredHeap{}
	heap.Init(candidates)
	heap.Init(results)
	heap.Push(candidates, vecScoredRow{rowIdx: entry, distance: dist})
	pushTopK(results, entry, dist, ef)

	for candidates.Len() > 0 {
		nearest := heap.Pop(candidates).(vecScoredRow)
		if results.Len() >= ef && !vecScoredRowLess(nearest, (*results)[0]) {
			break
		}
		for _, nb := range idx.neighborLayer(nearest.rowIdx, layer) {
			if !markVisited(nb) {
				continue
			}
			dist, ok := rowDistance(idx.metric, query, queryNorm, cache, nb)
			if !ok {
				continue
			}
			if results.Len() < ef || vecScoredRowLess(vecScoredRow{rowIdx: nb, distance: dist}, (*results)[0]) {
				heap.Push(candidates, vecScoredRow{rowIdx: nb, distance: dist})
				pushTopK(results, nb, dist, ef)
			}
		}
	}
	return topKFromHeap(results, ef)
}

func (idx *vecHNSWIndex) addHNSWLink(a, b, layer int, cache vecSearchColumnCacheEntry) {
	if a == b || !idx.hasLayer(a, layer) || !idx.hasLayer(b, layer) {
		return
	}
	if !containsInt(idx.neighbors[a][layer], b) {
		idx.neighbors[a][layer] = append(idx.neighbors[a][layer], b)
		idx.pruneHNSWNeighbors(a, layer, cache)
	}
	if !containsInt(idx.neighbors[b][layer], a) {
		idx.neighbors[b][layer] = append(idx.neighbors[b][layer], a)
		idx.pruneHNSWNeighbors(b, layer, cache)
	}
}

func (idx *vecHNSWIndex) pruneHNSWNeighbors(rowIdx, layer int, cache vecSearchColumnCacheEntry) {
	nbs := idx.neighbors[rowIdx][layer]
	if len(nbs) <= vecHNSWM {
		return
	}
	query := cache.vectors[rowIdx]
	queryNorm := rowNorm(cache, rowIdx)
	sort.Slice(nbs, func(i, j int) bool {
		di, okI := rowDistance(idx.metric, query, queryNorm, cache, nbs[i])
		dj, okJ := rowDistance(idx.metric, query, queryNorm, cache, nbs[j])
		if !okI {
			return false
		}
		if !okJ {
			return true
		}
		if di == dj {
			return nbs[i] < nbs[j]
		}
		return di < dj
	})
	idx.neighbors[rowIdx][layer] = nbs[:vecHNSWM]
}

func (idx *vecHNSWIndex) hasLayer(rowIdx, layer int) bool {
	return rowIdx >= 0 && rowIdx < len(idx.neighbors) && layer >= 0 && layer < len(idx.neighbors[rowIdx])
}

func (idx *vecHNSWIndex) neighborLayer(rowIdx, layer int) []int {
	if !idx.hasLayer(rowIdx, layer) {
		return nil
	}
	return idx.neighbors[rowIdx][layer]
}

func validVectorRows(cache vecSearchColumnCacheEntry, dims int) []int {
	rows := make([]int, 0, len(cache.vectors))
	for i := range cache.vectors {
		if validCacheRow(cache, i, dims) {
			rows = append(rows, i)
		}
	}
	return rows
}

func validCacheRow(cache vecSearchColumnCacheEntry, rowIdx int, dims int) bool {
	return rowIdx >= 0 && rowIdx < len(cache.valid) && cache.valid[rowIdx] && len(cache.vectors[rowIdx]) == dims
}

func rowNorm(cache vecSearchColumnCacheEntry, rowIdx int) float64 {
	if rowIdx >= 0 && rowIdx < len(cache.norms) {
		return cache.norms[rowIdx]
	}
	return vectorL2Norm(cache.vectors[rowIdx])
}

func rowDistance(metric string, query []float64, queryNorm float64, cache vecSearchColumnCacheEntry, rowIdx int) (float64, bool) {
	if rowIdx < 0 || rowIdx >= len(cache.vectors) || !cache.valid[rowIdx] {
		return 0, false
	}
	return vectorDistance(metric, cache.vectors[rowIdx], query, rowNorm(cache, rowIdx), queryNorm)
}

func centroidNorms(centroids [][]float64) []float64 {
	norms := make([]float64, len(centroids))
	for i, c := range centroids {
		norms[i] = vectorL2Norm(c)
	}
	return norms
}

func nearestCentroid(metric string, vec []float64, vecNorm float64, centroids [][]float64, norms []float64) int {
	best := 0
	bestDist := math.MaxFloat64
	for i, c := range centroids {
		dist, ok := vectorDistance(metric, c, vec, norms[i], vecNorm)
		if !ok {
			continue
		}
		if dist < bestDist {
			best = i
			bestDist = dist
		}
	}
	return best
}

func chooseIVFListCount(rows int) int {
	if rows <= 0 {
		return 0
	}
	nlist := int(math.Sqrt(float64(rows)) / 2)
	if nlist < 4 {
		nlist = min(rows, 4)
	}
	if nlist > 64 {
		nlist = 64
	}
	return nlist
}

func chooseIVFNProbe(nlist, k int) int {
	nprobe := int(math.Sqrt(float64(nlist))) + k/32
	if nprobe < 2 {
		nprobe = 2
	}
	if nprobe > 16 {
		nprobe = 16
	}
	if nprobe > nlist {
		nprobe = nlist
	}
	return nprobe
}

func hnswLevel(rowIdx int) int {
	x := uint64(rowIdx+1) * 11400714819323198485
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	level := 0
	for level < vecHNSWMaxLevel && x&0x7 < 1 {
		level++
		x >>= 3
	}
	return level
}

func chooseHNSWEfSearch(k int) int {
	ef := k * 3
	if ef < 24 {
		ef = 24
	}
	if ef > 96 {
		ef = 96
	}
	return ef
}

func selectHNSWNeighbors(candidates []vecScoredRow, limit int) []vecScoredRow {
	if len(candidates) <= limit {
		return candidates
	}
	return candidates[:limit]
}

func containsInt(values []int, needle int) bool {
	for _, v := range values {
		if v == needle {
			return true
		}
	}
	return false
}
