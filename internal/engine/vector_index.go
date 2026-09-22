package engine

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SimonWaldherr/tinySQL/internal/engine/search"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	vecIndexFlat = "flat"
	vecIndexIVF  = "ivf"
	vecIndexHNSW = "hnsw"

	vecIVFKMeansIters = 3

	// vecHNSWM is the number of links each inserted node selects, and the
	// degree cap on layers >= 1; layer 0 allows vecHNSWM0 (see
	// hnswMaxNeighbors). Together with vecHNSWEfConstruction and
	// vecHNSWEfSearchMin these match pgvector's HNSW defaults.
	vecHNSWM              = 16
	vecHNSWM0             = 2 * vecHNSWM
	vecHNSWEfConstruction = 64
	vecHNSWEfSearchMin    = 40
	vecHNSWMaxLevel       = 8

	// vecIndexCacheMaxEntries bounds the IVF and HNSW caches — see the
	// vecColumnCacheMaxEntries comment in vector_search.go for rationale.
	// Lower than the column-cache cap because index entries are larger
	// (centroids/graph) and each also pins its *storage.Table.
	vecIndexCacheMaxEntries = 64
)

type vecIndexCacheKey struct {
	tenant string
	table  string
	colIdx int
	metric string
}

// vecIndexBuildCall coalesces concurrent cold ANN index builds for one cache
// key — see vecIVFBuilds/vecHNSWBuilds and vecColumnBuildCall (vector_search.go)
// for the shared pattern.
type vecIndexBuildCall struct{ done chan struct{} }

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
	table   *storage.Table
	version int
	// structVersion is the table's StructVersion() as of the last time this
	// graph was fully built or incrementally extended. Equal to the table's
	// current StructVersion() proves every row this graph already indexes
	// (rows [0, len(levels))) is unchanged, so the only possible reason
	// idx.version can be behind table.Version is that rows were appended —
	// see canExtendVecHNSWIndex.
	structVersion int
	metric        string
	dims          int
	entry         int
	maxLevel      int
	levels        []int
	neighbors     [][][]int
	// deltaRows are UPDATE-only vectors whose old graph placement may no
	// longer make them reachable with good recall. Search scores this bounded
	// set exactly and merges it into the ANN top-k. A full rebuild clears it.
	deltaRows []int
	deltaSet  map[int]struct{}
	// mu guards every field above against extendVecHNSWIndex mutating an
	// already-published graph (see getVecHNSWIndex) while a concurrent
	// search() call is reading it. A freshly built graph (buildVecHNSWIndex)
	// needs no locking: it is not reachable through the cache, and therefore
	// not visible to any other goroutine, until after it is fully built.
	mu sync.RWMutex
}

type vecMinScoredHeap []vecScoredRow

func (h vecMinScoredHeap) Len() int { return len(h) }
func (h vecMinScoredHeap) Less(i, j int) bool {
	return vecScoredRowLess(h[i], h[j])
}
func (h vecMinScoredHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// vecMinHeapPush/Pop mirror vecScoredHeapPush/Pop (see vector_search.go) for
// the candidates frontier — direct concrete-type push/pop instead of
// heap.Push/heap.Pop, which box every vecScoredRow into an `any` and
// allocate. searchLayer calls this once per neighbor considered during graph
// traversal, so this was the largest remaining allocation source in HNSW
// build/search.
func vecMinHeapPush(h *vecMinScoredHeap, v vecScoredRow) {
	*h = append(*h, v)
	vecMinHeapUp(*h, len(*h)-1)
}

func vecMinHeapPop(h *vecMinScoredHeap) vecScoredRow {
	old := *h
	n := len(old) - 1
	old.Swap(0, n)
	vecMinHeapDown(old[:n], 0)
	v := old[n]
	*h = old[:n]
	return v
}

func vecMinHeapUp(h vecMinScoredHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func vecMinHeapDown(h vecMinScoredHeap, i0 int) {
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

var (
	vecIVFCacheMu sync.RWMutex
	vecIVFCache   = make(map[vecIndexCacheKey]*vecIVFIndex)
	// vecIVFBuilds coalesces concurrent cold IVF builds for the same
	// (tenant, table, colIdx, metric) key, mirroring vecSearchColumnBuilds in
	// vector_search.go. Keying the in-flight-build map by the same cache key
	// (rather than one global mutex) lets concurrent builds for DIFFERENT
	// keys proceed in parallel — e.g. VEC_WARM warming several tables at
	// startup takes max(build_i) instead of sum(build_i) — while concurrent
	// requests for the SAME key still coalesce onto a single build.
	vecIVFBuilds = make(map[vecIndexCacheKey]*vecIndexBuildCall)
	// vecIVFCacheSnapshot mirrors vecIVFCache for lock-free reads, the same
	// way vecSearchColumnCacheSnapshot mirrors the column cache
	// (vector_search.go) — every *vecIVFIndex is fully built before it is
	// ever published (no in-place extend exists for IVF, unlike HNSW), so a
	// snapshot read without vecIVFCacheMu is always safe.
	vecIVFCacheSnapshot atomic.Pointer[map[vecIndexCacheKey]*vecIVFIndex]

	vecHNSWCacheMu sync.RWMutex
	vecHNSWCache   = make(map[vecIndexCacheKey]*vecHNSWIndex)
	// vecHNSWBuilds is vecIVFBuilds' counterpart for the HNSW cache.
	vecHNSWBuilds = make(map[vecIndexCacheKey]*vecIndexBuildCall)
)

func init() {
	empty := make(map[vecIndexCacheKey]*vecIVFIndex)
	vecIVFCacheSnapshot.Store(&empty)
}

// publishVecIVFCacheSnapshotLocked refreshes the lock-free snapshot from the
// authoritative vecIVFCache. Callers must already hold vecIVFCacheMu and
// must call this immediately after every mutation (insert or delete —
// including purgeVectorCachesFor's deletes), so a lock-free reader never
// observes a snapshot older than what the mutex-protected map already
// reflects. Mirrors publishVecSearchColumnCacheSnapshotLocked
// (vector_search.go); capped at vecIndexCacheMaxEntries, so the copy is
// cheap relative to the k-means build that triggered it.
func publishVecIVFCacheSnapshotLocked() {
	snap := make(map[vecIndexCacheKey]*vecIVFIndex, len(vecIVFCache))
	for k, v := range vecIVFCache {
		snap[k] = v
	}
	vecIVFCacheSnapshot.Store(&snap)
}

// vecHNSWScratch holds the reusable candidate/result heaps for one HNSW
// traversal (one search() call, or the whole sequential build loop).
// searchLayer runs once per graph layer visited during a traversal and
// insertHNSWNode prunes neighbor lists on nearly every insertion, so reusing
// the backing arrays (reset to length 0, capacity retained) across calls,
// and across pooled instances between queries, converges to zero
// steady-state allocations for the traversal's own bookkeeping.
//
// results is also what searchLayer returns: the slice stays owned by the
// scratch and is only valid until the next searchLayer call on it.
type vecHNSWScratch struct {
	candidates vecMinScoredHeap
	results    vecScoredHeap
	// selected backs insertHNSWNode's neighbor selection for the node being
	// inserted; pruneScored/pruneSelected back the reverse-link pruning that
	// runs while that selection is still being iterated, so they must stay
	// separate buffers.
	selected      []vecScoredRow
	pruneScored   []vecScoredRow
	pruneSelected []vecScoredRow
}

var vecHNSWScratchPool = sync.Pool{
	New: func() any { return new(vecHNSWScratch) },
}

func acquireHNSWScratch() *vecHNSWScratch {
	return vecHNSWScratchPool.Get().(*vecHNSWScratch)
}

func releaseHNSWScratch(s *vecHNSWScratch) {
	vecHNSWScratchPool.Put(s)
}

// vecVisitedSet marks the rows one HNSW layer traversal has already scored.
// A row counts as visited when its mark equals the current epoch, so starting
// the next traversal is a single increment instead of clearing (or tracking
// and un-marking) every touched row. That matters most for large tables: a
// cleared []bool cost one memclr of len(rows) bytes per query even though a
// search touches only a few hundred rows.
type vecVisitedSet struct {
	marks []uint32
	epoch uint32
}

var vecVisitedPool = sync.Pool{
	New: func() any { return new(vecVisitedSet) },
}

func acquireVisited(n int) *vecVisitedSet {
	v := vecVisitedPool.Get().(*vecVisitedSet)
	v.ensure(n)
	return v
}

func releaseVisited(v *vecVisitedSet) {
	vecVisitedPool.Put(v)
}

// ensure makes rows [0, n) addressable. Growing within capacity is safe
// without clearing: marks beyond the old length were written in earlier
// epochs, and epochs only increase. Allocation grows with slack (like
// append's own doubling) because extendVecHNSWIndex, called once per
// single-row INSERT, asks for one more row on every call.
func (v *vecVisitedSet) ensure(n int) {
	if n <= len(v.marks) {
		return
	}
	if n <= cap(v.marks) {
		v.marks = v.marks[:n]
		return
	}
	v.marks = make([]uint32, n, n+n/2+64)
	v.epoch = 0
}

// reset starts a new traversal in which every row reads as unvisited.
func (v *vecVisitedSet) reset() {
	v.epoch++
	if v.epoch == 0 {
		clear(v.marks[:cap(v.marks)])
		v.epoch = 1
	}
}

// visit marks row and reports whether it was unvisited in this traversal.
func (v *vecVisitedSet) visit(row int) bool {
	if uint(row) >= uint(len(v.marks)) || v.marks[row] == v.epoch {
		return false
	}
	v.marks[row] = v.epoch
	return true
}

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

// vecSearchTopKWithIndex is the single choke point every index mode's
// search funnels through before a result ever reaches a caller. Internally,
// flat/IVF/HNSW all rank candidates using search.VectorRankingDistance (skipping
// l2's sqrt until it's actually needed); finalizeVecScoredRows converts the
// (at most k) surviving rows back to real distances exactly once here,
// rather than every candidate the scan/traversal considered paying for it.
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
	needNorm := vecDistanceFuncNeedsNorm(args.metric)
	var rows []vecScoredRow
	var err error
	switch args.indexMode {
	case vecIndexFlat:
		rows, err = vecSearchTopK(ctx, table.Rows, queryLen, args.k, cache, distFn, needNorm)
	case vecIndexIVF:
		var idx *vecIVFIndex
		idx, err = getVecIVFIndex(ctx, tenant, table, colIdx, args.metric, queryLen, cache)
		if err == nil {
			rows, err = idx.search(ctx, args.queryVec, queryNorm, args.k, cache)
		}
	case vecIndexHNSW:
		var idx *vecHNSWIndex
		idx, err = getVecHNSWIndex(ctx, tenant, table, colIdx, args.metric, queryLen, cache)
		if err == nil {
			rows, err = idx.search(ctx, args.queryVec, queryNorm, args.k, cache)
		}
	default:
		rows, err = vecSearchTopK(ctx, table.Rows, queryLen, args.k, cache, distFn, needNorm)
	}
	if err != nil {
		return nil, err
	}
	finalizeVecScoredRows(args.metric, rows)
	return rows, nil
}

// finalizeVecScoredRows converts every row's ranking-only distance (see
// search.VectorRankingDistance) into the real distance search.VectorDistance would have
// returned, in place. Cheap to call unconditionally: for every metric
// except l2 search.VectorFinalizeDistance is already a no-op, and rows is bounded
// to k (or fewer) entries regardless of how many candidates the scan or
// graph traversal that produced it considered.
func finalizeVecScoredRows(metric string, rows []vecScoredRow) {
	if metric != "l2" {
		return
	}
	for i := range rows {
		rows[i].distance = search.VectorFinalizeDistance(metric, rows[i].distance)
	}
}

func getVecIVFIndex(ctx context.Context, tenant string, table *storage.Table, colIdx int, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecIVFIndex, error) {
	key := vecIndexCacheKey{tenant: tenant, table: table.Name, colIdx: colIdx, metric: metric}

	// Lock-free fast path — see publishVecIVFCacheSnapshotLocked.
	if snap := vecIVFCacheSnapshot.Load(); snap != nil {
		if idx := (*snap)[key]; idx != nil && idx.table == table && idx.version == table.Version && idx.dims == dims {
			return idx, nil
		}
	}

	for {
		vecIVFCacheMu.RLock()
		if idx := vecIVFCache[key]; idx != nil && idx.table == table && idx.version == table.Version && idx.dims == dims {
			vecIVFCacheMu.RUnlock()
			return idx, nil
		}
		vecIVFCacheMu.RUnlock()

		vecIVFCacheMu.Lock()
		if idx := vecIVFCache[key]; idx != nil && idx.table == table && idx.version == table.Version && idx.dims == dims {
			vecIVFCacheMu.Unlock()
			return idx, nil
		}
		if call := vecIVFBuilds[key]; call != nil {
			vecIVFCacheMu.Unlock()
			<-call.done
			continue
		}
		call := &vecIndexBuildCall{done: make(chan struct{})}
		vecIVFBuilds[key] = call
		vecIVFCacheMu.Unlock()

		idx, err := buildVecIVFIndex(ctx, table, metric, dims, cache)

		vecIVFCacheMu.Lock()
		delete(vecIVFBuilds, key)
		if err == nil {
			if _, exists := vecIVFCache[key]; !exists {
				evictOverCap(vecIVFCache, vecIndexCacheMaxEntries)
			}
			vecIVFCache[key] = idx
			publishVecIVFCacheSnapshotLocked()
		}
		close(call.done)
		vecIVFCacheMu.Unlock()
		return idx, err
	}
}

func buildVecIVFIndex(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecIVFIndex, error) {
	rows := validVectorRows(cache, dims)
	nlist := chooseIVFListCount(len(rows))
	idx := &vecIVFIndex{table: table, version: table.Version, metric: metric, dims: dims}
	if len(rows) == 0 || nlist == 0 {
		return idx, nil
	}

	// Seed centroids by taking nlist rows spread evenly across the (already
	// arbitrary-ordered) row set, rather than the first nlist rows — cheap
	// and avoids every centroid starting near-identical if the table happens
	// to be sorted or clustered by insertion order.
	idx.centroids = make([][]float64, nlist)
	for i := range idx.centroids {
		src := cache.vector(rows[(i*len(rows))/nlist])
		idx.centroids[i] = append([]float64(nil), src...)
	}
	// Reuse the accumulation buffers across k-means iterations instead of
	// reallocating nlist*dims floats per pass.
	sums := make([]float64, nlist*dims)
	counts := make([]int, nlist)
	// Resolved once: metric is fixed for the whole build, and resolveRow lets
	// each row below pay for one overrides/segment lookup instead of the two
	// (cache.vector, then rowNormFor->cache.normAt) that nearestCentroid's
	// separate vector/norm arguments used to require.
	needNorm := metricNeedsNorms(metric)
	for iter := 0; iter < vecIVFKMeansIters; iter++ {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		idx.centroidNorms = centroidNormsFor(metric, idx.centroids)
		clear(sums)
		clear(counts)
		for i, rowIdx := range rows {
			if i&1023 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			vec, norm, _ := cache.resolveRow(rowIdx, needNorm)
			c := nearestCentroid(metric, vec, norm, idx.centroids, idx.centroidNorms)
			counts[c]++
			base := c * dims
			search.VectorAccumulate(sums[base:base+dims], vec)
		}
		for c := range idx.centroids {
			// A centroid can end up with zero assigned rows this iteration
			// (every row happened to be closer to some other centroid) —
			// a normal k-means edge case, more likely with unlucky seeding
			// or nlist close to the number of distinct clusters actually
			// present. Leave it at its previous position rather than
			// dividing by zero; it may pick up members in a later
			// iteration as other centroids move, or simply stay unused.
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
	// Assign against the final centroids. The loop above assigns at the top of
	// each iteration and moves the centroids at the bottom, so its last
	// assignment predates the last move: building the lists from it would file
	// rows under a centroid that is no longer their nearest, and search probing
	// the correct centroid would then miss them.
	for i, rowIdx := range rows {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		vec, norm, _ := cache.resolveRow(rowIdx, needNorm)
		c := nearestCentroid(metric, vec, norm, idx.centroids, idx.centroidNorms)
		idx.lists[c] = append(idx.lists[c], rowIdx)
	}
	return idx, nil
}

func (idx *vecIVFIndex) search(ctx context.Context, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry) ([]vecScoredRow, error) {
	if len(idx.centroids) == 0 {
		return nil, nil
	}
	probes := chooseIVFNProbe(len(idx.centroids), k)
	// Rank every centroid, not just the first `probes`. The probe budget is a
	// recall/latency trade, but it must not cap the result *count*: if the
	// probed lists between them hold fewer than k rows, the scan continues
	// down the ranking. Without that, VEC_SEARCH with an ivf index silently
	// returned fewer rows than asked for -- 7 of 10 on a 10-row table -- while
	// flat and hnsw returned k.
	centroidHeap := newScoredHeap(len(idx.centroids), -1)
	for i, c := range idx.centroids {
		// Selecting which centroids to probe, not a value ever exposed to a
		// caller — ranking-only distance is enough (see rowDistance below
		// for the row-level equivalent).
		dist, ok := search.VectorRankingDistance(idx.metric, c, query, idx.centroidNorms[i], queryNorm)
		if !ok {
			continue
		}
		pushTopK(centroidHeap, i, dist, len(idx.centroids))
	}
	rankedCentroids := topKFromHeap(centroidHeap, len(idx.centroids))

	resultHeap := newScoredHeap(k, -1)
	for probed, c := range rankedCentroids {
		// Past the probe budget, keep going only while short of k.
		if probed >= probes && resultHeap.Len() >= k {
			break
		}
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

	for {
		vecHNSWCacheMu.RLock()
		idx := vecHNSWCache[key]
		vecHNSWCacheMu.RUnlock()
		if idx != nil && idx.table == table && idx.dims == dims && idx.versionMatches(table.Version) {
			return idx, nil
		}

		vecHNSWCacheMu.Lock()
		idx = vecHNSWCache[key]
		if idx != nil && idx.table == table && idx.dims == dims && idx.versionMatches(table.Version) {
			vecHNSWCacheMu.Unlock()
			return idx, nil
		}
		if call := vecHNSWBuilds[key]; call != nil {
			vecHNSWCacheMu.Unlock()
			<-call.done
			continue
		}

		// A runtime cache miss after reopening no longer has to mean a graph
		// rebuild.  Hydrate the validated, table-persisted HNSW topology first;
		// an append-only table can then extend it by just the new rows below.
		if idx == nil || idx.table != table {
			idx = loadPersistentVecHNSWIndex(table, colIdx, metric, dims, cache)
			if idx != nil {
				if _, exists := vecHNSWCache[key]; !exists {
					evictOverCap(vecHNSWCache, vecIndexCacheMaxEntries)
				}
				vecHNSWCache[key] = idx
				if idx.version == table.Version {
					vecHNSWCacheMu.Unlock()
					return idx, nil
				}
			}
		}

		// A cached graph that is stale only because rows were appended since
		// it was built or last extended (no UPDATE, DELETE, or schema change
		// happened in between — see canExtendVecHNSWIndex) can grow in place
		// instead of being discarded and rebuilt from scratch. This mirrors
		// the incremental-append/full-rebuild-on-anything-else split already
		// used for constraintIndexes (exec_dml_insert.go): appends are cheap
		// to absorb, everything else re-derives the whole structure.
		updatedRows, canRefresh := vecHNSWRefreshRows(idx, table, dims)
		if canExtendVecHNSWIndex(idx, table, dims) || canRefresh {
			call := &vecIndexBuildCall{done: make(chan struct{})}
			vecHNSWBuilds[key] = call
			vecHNSWCacheMu.Unlock()

			idx.mu.Lock()
			err := extendVecHNSWIndex(ctx, idx, cache)
			if err == nil {
				if canRefresh {
					idx.addDeltaRows(updatedRows)
				}
				idx.version = table.Version
				idx.structVersion = table.StructVersion()
			}
			hasDeltas := len(idx.deltaRows) != 0
			idx.mu.Unlock()
			// A delta graph is correct at runtime because search merges deltaRows
			// exactly. Persisting only its stale topology would lose that merge
			// after reopen, so persistence resumes after the next compact rebuild.
			if err == nil && !hasDeltas {
				persistVecHNSWIndex(table, colIdx, metric, idx)
			}

			vecHNSWCacheMu.Lock()
			delete(vecHNSWBuilds, key)
			close(call.done)
			vecHNSWCacheMu.Unlock()
			if err != nil {
				return nil, err
			}
			return idx, nil
		}

		call := &vecIndexBuildCall{done: make(chan struct{})}
		vecHNSWBuilds[key] = call
		vecHNSWCacheMu.Unlock()

		newIdx, err := buildVecHNSWIndex(ctx, table, metric, dims, cache)
		if err == nil {
			persistVecHNSWIndex(table, colIdx, metric, newIdx)
		}

		vecHNSWCacheMu.Lock()
		delete(vecHNSWBuilds, key)
		if err == nil {
			if _, exists := vecHNSWCache[key]; !exists {
				evictOverCap(vecHNSWCache, vecIndexCacheMaxEntries)
			}
			vecHNSWCache[key] = newIdx
		}
		close(call.done)
		vecHNSWCacheMu.Unlock()
		return newIdx, err
	}
}

// versionMatches reports whether idx.version equals v, under idx.mu.
//
// idx.version is written by getVecHNSWIndex's extend path (idx.version =
// table.Version) under idx.mu.Lock() only — vecHNSWCacheMu is deliberately
// released before that write starts, so a slow extend doesn't stall lookups
// for other cache keys. A caller that instead read idx.version guarded only
// by vecHNSWCacheMu (as this and the two functions below used to) raced with
// that write: two different mutexes protecting the same memory is not
// synchronization. See canExtendVecHNSWIndex/vecHNSWRefreshRows for the same
// fix applied to idx.structVersion/idx.levels/idx.deltaRows/idx.deltaSet.
func (idx *vecHNSWIndex) versionMatches(v int) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.version == v
}

// canExtendVecHNSWIndex reports whether idx can grow in place to cover
// table's current rows instead of being rebuilt from scratch. This is safe
// exactly when table.StructVersion() has not moved since idx was last built
// or extended: that counter only advances on UPDATE, DELETE, or a schema
// change (see Table.noteStructuralChange), never on INSERT, so an unchanged
// value proves every row idx already indexes still holds exactly what it did
// then — the table can only have grown by appending. The row-count check is
// a second, independent guard against the same conclusion reached unsafely
// (e.g. a table rolled back to fewer rows than idx has already indexed,
// which would otherwise still show an unchanged StructVersion).
//
// idx.structVersion/idx.levels are read under idx.mu: extendVecHNSWIndex
// appends to idx.levels and bumps idx.structVersion under idx.mu.Lock() only
// (see versionMatches), so reading them under vecHNSWCacheMu alone — as this
// used to — is a data race whenever this runs concurrently with an extend of
// the same *vecHNSWIndex for a different cache key's lookup.
func canExtendVecHNSWIndex(idx *vecHNSWIndex, table *storage.Table, dims int) bool {
	if idx == nil || idx.table != table || idx.dims != dims {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.structVersion == table.StructVersion() && len(idx.levels) <= len(table.Rows)
}

// vecHNSWRefreshRows admits bounded UPDATE-only deltas. The graph topology stays
// intact while search reads the vector-column cache's row overrides, so the
// changed embeddings are immediately visible without rebuilding every edge.
// This is still ANN (topology may be less ideal after many moves); DELETE and
// schema changes are rejected by UpdatedRowsSince because row positions shift.
//
// Every read of idx's mutable fields below holds idx.mu (see versionMatches
// for why): table.UpdatedRowsSince between the two RLock sections touches
// only table, never idx, so it is safe to run without idx.mu held.
func vecHNSWRefreshRows(idx *vecHNSWIndex, table *storage.Table, dims int) ([]int, bool) {
	if idx == nil || idx.table != table || idx.dims != dims {
		return nil, false
	}
	idx.mu.RLock()
	levelsFit := len(idx.levels) <= len(table.Rows)
	structVersion := idx.structVersion
	idx.mu.RUnlock()
	if !levelsFit {
		return nil, false
	}

	rows, ok := table.UpdatedRowsSince(structVersion)
	if !ok || len(rows) == 0 {
		return nil, false
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	unique := len(idx.deltaRows)
	for _, row := range rows {
		if _, exists := idx.deltaSet[row]; !exists {
			unique++
		}
	}
	limit := len(table.Rows) / 8
	if limit < 32 {
		limit = 32
	}
	if limit > 1024 {
		limit = 1024
	}
	if unique > limit {
		return nil, false
	}
	return rows, true
}

func (idx *vecHNSWIndex) addDeltaRows(rows []int) {
	if len(rows) == 0 {
		return
	}
	if idx.deltaSet == nil {
		idx.deltaSet = make(map[int]struct{}, len(rows))
	}
	for _, row := range rows {
		if _, exists := idx.deltaSet[row]; exists {
			continue
		}
		idx.deltaSet[row] = struct{}{}
		idx.deltaRows = append(idx.deltaRows, row)
	}
}

func buildVecHNSWIndex(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry) (*vecHNSWIndex, error) {
	idx := &vecHNSWIndex{
		table:         table,
		version:       table.Version,
		structVersion: table.StructVersion(),
		metric:        metric,
		dims:          dims,
		entry:         -1,
		maxLevel:      -1,
		levels:        make([]int, cache.rowCount()),
		neighbors:     make([][][]int, cache.rowCount()),
	}

	visited := acquireVisited(cache.rowCount())
	defer releaseVisited(visited)
	// The build loop is strictly sequential (single goroutine inserting one
	// row at a time), so a single scratch instance can be reused across all
	// insertions instead of round-tripping through the pool per row.
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for rowIdx := 0; rowIdx < cache.rowCount(); rowIdx++ {
		if rowIdx&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if !validCacheRow(cache, rowIdx, dims) {
			continue
		}
		idx.insertHNSWNode(rowIdx, cache, visited, scratch)
	}
	return idx, nil
}

// extendVecHNSWIndex grows an already-built HNSW graph in place by inserting
// every row appended to the table since idx was last built or extended,
// using insertHNSWNode — the exact same per-row insert logic
// buildVecHNSWIndex uses for a from-scratch build. The resulting graph is
// therefore structurally identical to one built from scratch on the same
// final data (same neighbor selection, same pruning, same entry-point
// evolution), not merely "a working graph".
//
// Callers must already have established, via canExtendVecHNSWIndex, that
// only appends — never an UPDATE, DELETE, or schema change — happened to
// the table since idx.version; this function does not re-check that and
// trusts idx's existing rows [0, len(idx.levels)) to still be exactly what
// they were.
//
// idx is the same object already published in vecHNSWCache, so the caller
// must hold idx.mu for writing for the duration of this call — a concurrent
// search() call may be reading idx through a reference obtained earlier.
//
// If ctx is cancelled partway through, idx.levels/idx.neighbors are
// truncated back to the last row fully inserted (every link created so far
// only ever points at rows below that point, since a node can only link to
// something already reachable in the graph) so idx is left in exactly the
// state a shorter, successful extend would have produced — never partially
// linked. idx.version/structVersion are left untouched on error, so the next
// getVecHNSWIndex call sees it as still eligible for (and resumes) an
// incremental extend from that point, rather than paying for a full rebuild.
func extendVecHNSWIndex(ctx context.Context, idx *vecHNSWIndex, cache vecSearchColumnCacheEntry) error {
	oldRows := len(idx.levels)
	newRows := cache.rowCount()
	if newRows <= oldRows {
		return nil
	}
	idx.levels = append(idx.levels, make([]int, newRows-oldRows)...)
	idx.neighbors = append(idx.neighbors, make([][][]int, newRows-oldRows)...)

	// Sized to the final row count up front, exactly like buildVecHNSWIndex's
	// visited array, since traversal while inserting a new row can touch any
	// row already linked into the graph, old or newly inserted earlier in
	// this same call. Pooled (see acquireVisited/search) rather than a plain
	// make(): a from-scratch build pays one allocation for the whole build,
	// but every single-row INSERT that reaches this function would otherwise
	// pay a fresh O(existing rows) allocation of its own on every call — an
	// allocation cost that grows with table size defeating the entire point
	// of extending in place instead of rebuilding.
	visited := acquireVisited(newRows)
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for rowIdx := oldRows; rowIdx < newRows; rowIdx++ {
		if rowIdx&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				idx.levels = idx.levels[:rowIdx]
				idx.neighbors = idx.neighbors[:rowIdx]
				return err
			}
		}
		if !validCacheRow(cache, rowIdx, idx.dims) {
			continue
		}
		idx.insertHNSWNode(rowIdx, cache, visited, scratch)
	}
	return nil
}

// insertHNSWNode inserts rowIdx into idx's graph. It assumes idx.levels and
// idx.neighbors already have a slot allocated for every index up to rowIdx
// (as both buildVecHNSWIndex and extendVecHNSWIndex arrange before calling
// this), and that rows below rowIdx are already fully inserted. Shared by
// both, so a from-scratch build and an incremental extend grow the exact
// same graph structure via the exact same logic.
//
// This is Algorithm 1 of Malkov & Yashunin's HNSW paper: greedy descent to
// the node's own top layer, then an efConstruction-wide beam search per
// layer whose result is thinned by the neighbor-selection heuristic
// (selectHNSWNeighbors) before the node is linked in both directions.
func (idx *vecHNSWIndex) insertHNSWNode(rowIdx int, cache vecSearchColumnCacheEntry, visited *vecVisitedSet, scratch *vecHNSWScratch) {
	level := hnswLevel(rowIdx)
	idx.levels[rowIdx] = level
	idx.neighbors[rowIdx] = make([][]int, level+1)
	rows := newVecRowResolver(&cache, metricNeedsNorms(idx.metric))
	query, queryNorm, ok := rows.resolve(rowIdx)
	if !ok || (rows.needNorm && queryNorm == 0) {
		// A zero vector has no cosine distance to anything. Linking it would
		// only add dead edges, and making it the entry point would leave
		// every later insert and search unable to score its starting node.
		return
	}
	if idx.entry < 0 {
		idx.entry = rowIdx
		idx.maxLevel = level
		return
	}
	scorer := newVecHNSWScorer(idx.metric, query, queryNorm, rows)
	current := idx.entry
	currentDist, ok := scorer.distance(current)
	if !ok {
		return
	}
	for layer := idx.maxLevel; layer > level; layer-- {
		current, currentDist = idx.greedyClosest(&scorer, current, currentDist, layer)
	}
	for layer := min(level, idx.maxLevel); layer >= 0; layer-- {
		candidates := idx.searchLayer(&scorer, current, currentDist, vecHNSWEfConstruction, layer, visited, scratch)
		if len(candidates) == 0 {
			continue
		}
		current, currentDist = candidates[0].rowIdx, candidates[0].distance
		selected := selectHNSWNeighbors(&scorer, candidates, vecHNSWM, &scratch.selected)
		// Reserve room for the reverse links later inserts add, so filling
		// a list up to its cap never reallocates it.
		links := make([]int, len(selected), hnswMaxNeighbors(layer)+1)
		for i, nb := range selected {
			links[i] = nb.rowIdx
		}
		idx.neighbors[rowIdx][layer] = links
		for _, nb := range selected {
			idx.addHNSWReverseLink(&scorer, nb.rowIdx, rowIdx, nb.distance, layer, scratch)
		}
	}
	if level > idx.maxLevel {
		idx.entry = rowIdx
		idx.maxLevel = level
	}
}

// addHNSWReverseLink links from -> to on layer, where dist is their distance
// (every supported metric is symmetric, so the inserting node's distance to
// from is reused). A list already at its cap is re-thinned with the same
// heuristic that selected it, over its current members plus the new one.
func (idx *vecHNSWIndex) addHNSWReverseLink(s *vecHNSWScorer, from, to int, dist float64, layer int, scratch *vecHNSWScratch) {
	if from == to || !idx.hasLayer(from, layer) {
		return
	}
	nbs := idx.neighbors[from][layer]
	maxNeighbors := hnswMaxNeighbors(layer)
	if len(nbs) < maxNeighbors {
		idx.neighbors[from][layer] = append(nbs, to)
		return
	}
	fromVec, fromNorm, ok := s.rows.resolve(from)
	if !ok {
		return
	}
	scored := append(scratch.pruneScored[:0], vecScoredRow{rowIdx: to, distance: dist})
	for _, nb := range nbs {
		d, ok := s.distanceBetween(fromVec, fromNorm, nb)
		if !ok {
			d = math.Inf(1)
		}
		scored = append(scored, vecScoredRow{rowIdx: nb, distance: d})
	}
	// At most maxNeighbors+1 rows: insertion sort beats sort.Slice here.
	for i := 1; i < len(scored); i++ {
		v := scored[i]
		j := i - 1
		for j >= 0 && vecScoredRowLess(v, scored[j]) {
			scored[j+1] = scored[j]
			j--
		}
		scored[j+1] = v
	}
	scratch.pruneScored = scored
	selected := selectHNSWNeighbors(s, scored, maxNeighbors, &scratch.pruneSelected)
	nbs = nbs[:0]
	for _, sr := range selected {
		nbs = append(nbs, sr.rowIdx)
	}
	idx.neighbors[from][layer] = nbs
}

// selectHNSWNeighbors is the neighbor-selection heuristic of the HNSW paper
// (Algorithm 4, as used by hnswlib and Faiss). candidates must be sorted
// nearest first. A candidate is kept only if it is closer to the base node
// than to every neighbor already kept, so a dense cluster contributes a few
// representatives instead of all limit slots. Keeping the plain nearest
// limit rows instead leaves clustered data as islands without edges between
// them, where search cannot leave the cluster it entered.
func selectHNSWNeighbors(s *vecHNSWScorer, candidates []vecScoredRow, limit int, buf *[]vecScoredRow) []vecScoredRow {
	if len(candidates) <= limit {
		return candidates
	}
	selected := (*buf)[:0]
	// Each accepted neighbor is compared with every later candidate. Keep its
	// resolved vector here instead of resolving the same row on every pair.
	var vectorBuf [vecHNSWM0][]float64
	var normBuf [vecHNSWM0]float64
	vectors, norms := vectorBuf[:0], normBuf[:0]
	if limit > len(vectorBuf) {
		vectors = make([][]float64, 0, limit)
		norms = make([]float64, 0, limit)
	}
	for _, c := range candidates {
		if len(selected) >= limit {
			break
		}
		vec, norm, ok := s.rows.resolve(c.rowIdx)
		if !ok {
			continue
		}
		keep := true
		for i := range selected {
			d, ok := vecRankingDistanceKind(s.kind, vectors[i], vec, norms[i], norm)
			if ok && d < c.distance {
				keep = false
				break
			}
		}
		if keep {
			selected = append(selected, c)
			vectors = append(vectors, vec)
			norms = append(norms, norm)
		}
	}
	*buf = selected
	return selected
}

func (idx *vecHNSWIndex) search(ctx context.Context, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry) ([]vecScoredRow, error) {
	// extendVecHNSWIndex can mutate this same, already-published object in
	// place (see getVecHNSWIndex); a plain full rebuild never touches an
	// object once published, so this read lock costs nothing extra in that
	// (still far more common) case beyond an uncontended RWMutex RLock.
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.entry < 0 {
		return nil, nil
	}
	visited := acquireVisited(cache.rowCount())
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	scorer := newVecHNSWScorer(idx.metric, query, queryNorm, newVecRowResolver(&cache, metricNeedsNorms(idx.metric)))
	candidates, err := idx.searchCandidates(ctx, &scorer, chooseHNSWEfSearch(k), visited, scratch)
	if err != nil {
		return nil, err
	}
	if len(idx.deltaRows) == 0 && len(candidates) >= k {
		// candidates is sorted nearest first and owned by scratch.
		return append([]vecScoredRow(nil), candidates[:k]...), nil
	}
	resultHeap := newScoredHeap(k, len(candidates)+len(idx.deltaRows))
	var seen map[int]struct{}
	if len(idx.deltaRows) != 0 {
		seen = make(map[int]struct{}, len(candidates))
	}
	for _, sr := range candidates {
		pushTopK(resultHeap, sr.rowIdx, sr.distance, k)
		if seen != nil {
			seen[sr.rowIdx] = struct{}{}
		}
	}
	for _, rowIdx := range idx.deltaRows {
		if _, duplicate := seen[rowIdx]; duplicate {
			continue
		}
		distance, ok := scorer.distance(rowIdx)
		if ok {
			pushTopK(resultHeap, rowIdx, distance, k)
		}
	}
	if resultHeap.Len() < k {
		distFn := buildVecDistanceFunc(idx.metric, query, queryNorm)
		return vecSearchTopK(ctx, idx.table.Rows, idx.dims, k, cache, distFn, vecDistanceFuncNeedsNorm(idx.metric))
	}
	return topKFromHeap(resultHeap, k), nil
}

// searchCandidates descends greedily from the entry point to layer 1 and
// returns layer 0's ef nearest rows, sorted nearest first and owned by
// scratch. Shared by search and searchFiltered.
func (idx *vecHNSWIndex) searchCandidates(ctx context.Context, s *vecHNSWScorer, ef int, visited *vecVisitedSet, scratch *vecHNSWScratch) ([]vecScoredRow, error) {
	current := idx.entry
	currentDist, ok := s.distance(current)
	if !ok {
		return nil, nil
	}
	for layer := idx.maxLevel; layer > 0; layer-- {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		current, currentDist = idx.greedyClosest(s, current, currentDist, layer)
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	return idx.searchLayer(s, current, currentDist, ef, 0, visited, scratch), nil
}

// greedyClosest walks layer from current to the nearest node reachable by
// strictly improving moves. This is searchLayer with ef == 1, minus the
// heaps and the visited set: distance strictly decreases on every move, so
// the walk cannot cycle.
func (idx *vecHNSWIndex) greedyClosest(s *vecHNSWScorer, current int, currentDist float64, layer int) (int, float64) {
	for changed := true; changed; {
		changed = false
		for _, nb := range idx.neighborLayer(current, layer) {
			d, ok := s.distance(nb)
			if ok && vecScoredRowLess(vecScoredRow{rowIdx: nb, distance: d}, vecScoredRow{rowIdx: current, distance: currentDist}) {
				current, currentDist, changed = nb, d, true
			}
		}
	}
	return current, currentDist
}

// searchLayer is the HNSW beam search (Algorithm 2): it returns the ef
// nearest rows found on layer from entry, sorted nearest first. The slice is
// scratch.results and is only valid until the next call with scratch.
func (idx *vecHNSWIndex) searchLayer(s *vecHNSWScorer, entry int, entryDist float64, ef int, layer int, visited *vecVisitedSet, scratch *vecHNSWScratch) []vecScoredRow {
	if entry < 0 || ef <= 0 || !idx.hasLayer(entry, layer) {
		return nil
	}
	visited.ensure(len(idx.neighbors))
	visited.reset()
	visited.visit(entry)
	candidates := scratch.candidates[:0]
	results := scratch.results[:0]
	start := vecScoredRow{rowIdx: entry, distance: entryDist}
	vecMinHeapPush(&candidates, start)
	vecScoredHeapPush(&results, start)
	for len(candidates) > 0 {
		nearest := vecMinHeapPop(&candidates)
		// Stop once the closest unexpanded candidate is farther than the
		// worst result. A candidate that *is* the worst result must still be
		// expanded: with ef == 1 the start node is both, and stopping on it
		// would never move off the entry point.
		if len(results) >= ef && vecScoredRowLess(results[0], nearest) {
			break
		}
		for _, nb := range idx.neighborLayer(nearest.rowIdx, layer) {
			if !visited.visit(nb) {
				continue
			}
			d, ok := s.distance(nb)
			if !ok {
				continue
			}
			cand := vecScoredRow{rowIdx: nb, distance: d}
			if len(results) < ef {
				vecMinHeapPush(&candidates, cand)
				vecScoredHeapPush(&results, cand)
			} else if vecScoredRowLess(cand, results[0]) {
				vecMinHeapPush(&candidates, cand)
				results[0] = cand
				vecScoredHeapDown(results, 0)
			}
		}
	}
	// Heap-sort in place: each step moves the current worst to the end.
	for n := len(results) - 1; n > 0; n-- {
		results[0], results[n] = results[n], results[0]
		vecScoredHeapDown(results[:n], 0)
	}
	scratch.candidates = candidates
	scratch.results = results
	return results
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

// hnswMaxNeighbors is the degree cap per layer. Layer 0 holds every node and
// carries the final beam search, so it gets twice the links of the sparse
// upper layers, as in the HNSW paper and hnswlib.
func hnswMaxNeighbors(layer int) int {
	if layer == 0 {
		return vecHNSWM0
	}
	return vecHNSWM
}

// vecRowResolver is cache.resolveRow with a direct path for the common
// read-only corpus: one contiguous segment and no UPDATE overrides (see
// contiguousSegment). Graph traversal resolves one row per edge it follows,
// so skipping the overrides-map probe and the segment binary search there
// is measurable.
type vecRowResolver struct {
	cache    *vecSearchColumnCacheEntry
	seg      *vecColumnSegment
	needNorm bool
}

func newVecRowResolver(cache *vecSearchColumnCacheEntry, needNorm bool) vecRowResolver {
	r := vecRowResolver{cache: cache, needNorm: needNorm}
	if len(cache.overrides) == 0 && len(cache.segments) == 1 {
		seg := &cache.segments[0]
		if seg.start == 0 && len(seg.vectors) == cache.rows && len(seg.valid) == cache.rows &&
			(!cache.normsReady || len(seg.norms) == cache.rows) {
			r.seg = seg
		}
	}
	return r
}

func (r *vecRowResolver) resolve(row int) (vec []float64, norm float64, valid bool) {
	seg := r.seg
	if seg == nil {
		return r.cache.resolveRow(row, r.needNorm)
	}
	if uint(row) >= uint(len(seg.vectors)) || !seg.valid[row] {
		return nil, 0, false
	}
	vec = seg.vectors[row]
	if !r.needNorm {
		return vec, 0, true
	}
	if r.cache.normsReady {
		return vec, seg.norms[row], true
	}
	return vec, vectorL2Norm(vec), true
}

const (
	vecMetricUnknown = iota
	vecMetricCosine
	vecMetricL2
	vecMetricManhattan
	vecMetricDot
)

func vecMetricKind(metric string) int {
	switch metric {
	case "cosine":
		return vecMetricCosine
	case "l2":
		return vecMetricL2
	case "manhattan":
		return vecMetricManhattan
	case "dot":
		return vecMetricDot
	default:
		return vecMetricUnknown
	}
}

// vecHNSWScorer computes ranking distances (search.VectorRankingDistance,
// argument order included, so values are bit-identical to the flat scan's)
// from one query to graph rows, with the metric resolved once instead of per
// edge. A NaN distance counts as unscorable, as it does in pushTopK.
type vecHNSWScorer struct {
	rows      vecRowResolver
	kind      int
	query     []float64
	queryNorm float64
}

func newVecHNSWScorer(metric string, query []float64, queryNorm float64, rows vecRowResolver) vecHNSWScorer {
	return vecHNSWScorer{rows: rows, kind: vecMetricKind(metric), query: query, queryNorm: queryNorm}
}

func (s *vecHNSWScorer) distance(row int) (float64, bool) {
	vec, norm, ok := s.rows.resolve(row)
	if !ok {
		return 0, false
	}
	return vecRankingDistanceKind(s.kind, vec, s.query, norm, s.queryNorm)
}

// distanceBetween scores row against another row's already-resolved vector.
func (s *vecHNSWScorer) distanceBetween(vec []float64, norm float64, row int) (float64, bool) {
	other, otherNorm, ok := s.rows.resolve(row)
	if !ok {
		return 0, false
	}
	return vecRankingDistanceKind(s.kind, other, vec, otherNorm, norm)
}

func vecRankingDistanceKind(kind int, a, b []float64, normA, normB float64) (float64, bool) {
	if len(a) != len(b) {
		return 0, false
	}
	var d float64
	switch kind {
	case vecMetricCosine:
		if normA == 0 || normB == 0 {
			return 0, false
		}
		d = 1.0 - search.VectorDot(a, b)/(normA*normB)
	case vecMetricL2:
		d = search.VectorL2Squared(a, b)
	case vecMetricManhattan:
		d = search.VectorL1Distance(a, b)
	case vecMetricDot:
		d = -search.VectorDot(a, b)
	default:
		return 0, false
	}
	if d != d {
		return 0, false
	}
	return d, true
}

func validVectorRows(cache vecSearchColumnCacheEntry, dims int) []int {
	rows := make([]int, 0, cache.rowCount())
	for i := 0; i < cache.rowCount(); i++ {
		if validCacheRow(cache, i, dims) {
			rows = append(rows, i)
		}
	}
	return rows
}

func validCacheRow(cache vecSearchColumnCacheEntry, rowIdx int, dims int) bool {
	return rowIdx >= 0 && rowIdx < cache.rowCount() && cache.validAt(rowIdx) && len(cache.vector(rowIdx)) == dims
}

func rowNorm(cache vecSearchColumnCacheEntry, rowIdx int) float64 {
	if rowIdx >= 0 && rowIdx < cache.rowCount() && cache.normsReady {
		return cache.normAt(rowIdx)
	}
	return vectorL2Norm(cache.vector(rowIdx))
}

// metricNeedsNorms reports whether the metric consumes vector norms.
// Only cosine does; computing norms for l2/manhattan/dot would double the
// per-row work in index build and search paths.
func metricNeedsNorms(metric string) bool {
	return metric == "cosine"
}

// rowNormFor returns the cached/computed norm only when the metric needs it.
func rowNormFor(metric string, cache vecSearchColumnCacheEntry, rowIdx int) float64 {
	if !metricNeedsNorms(metric) {
		return 0
	}
	return rowNorm(cache, rowIdx)
}

// rowDistance returns a ranking-only distance (see search.VectorRankingDistance):
// every call site is internal graph/list traversal, never a value exposed
// directly to a caller without first passing through
// vecSearchTopKWithIndex's finalize step.
//
// It resolves the row through resolveRow instead of validAt+vector+normAt so
// HNSW's searchLayer (called once per neighbor edge visited) and IVF's list
// scan pay for one overrides/segment lookup per candidate instead of three.
func rowDistance(metric string, query []float64, queryNorm float64, cache vecSearchColumnCacheEntry, rowIdx int) (float64, bool) {
	vec, norm, valid := cache.resolveRow(rowIdx, metricNeedsNorms(metric))
	if !valid {
		return 0, false
	}
	return search.VectorRankingDistance(metric, vec, query, norm, queryNorm)
}

func centroidNorms(centroids [][]float64) []float64 {
	norms := make([]float64, len(centroids))
	for i, c := range centroids {
		norms[i] = vectorL2Norm(c)
	}
	return norms
}

// centroidNormsFor is centroidNorms gated by metricNeedsNorms: idx.centroidNorms
// is indexed unconditionally by nearestCentroid/search (idx.centroidNorms[i]),
// so it must always have one entry per centroid, but only cosine ever reads
// the values — l2/manhattan/dot ignore normA/normB entirely (search.VectorDistance).
// Returning a zero-filled slice of the same length for those metrics skips
// vectorL2Norm's sqrt per centroid per k-means iteration for no behavior
// change, mirroring rowNormFor's same gating for per-row norms.
func centroidNormsFor(metric string, centroids [][]float64) []float64 {
	if !metricNeedsNorms(metric) {
		return make([]float64, len(centroids))
	}
	return centroidNorms(centroids)
}

// nearestCentroid only ever returns an index, never the distance itself, so
// it uses the cheaper ranking-only distance (see search.VectorRankingDistance).
func nearestCentroid(metric string, vec []float64, vecNorm float64, centroids [][]float64, norms []float64) int {
	best := 0
	bestDist := math.MaxFloat64
	for i, c := range centroids {
		dist, ok := search.VectorRankingDistance(metric, c, vec, norms[i], vecNorm)
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

// chooseHNSWEfSearch picks the graph-search beam width (ef) for a
// requested top-k: k itself, but at least vecHNSWEfSearchMin so small k
// still gets a useful exploration budget (a floor of 16 lost measurable
// recall at k=10 on clustered corpora, for little latency saved).
//
// There is deliberately no ceiling. searchLayer returns at most ef rows, so
// an ef below k would leave search() short of k results on every call and
// send it to the full flat-scan fallback — paying for the graph traversal
// *and* the scan, with the index providing no benefit for top-100/200
// reranking queries.
func chooseHNSWEfSearch(k int) int {
	return max(k, vecHNSWEfSearchMin)
}
