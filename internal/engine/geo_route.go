// ROUTE_SHORTEST_PATH and ROUTE_DISTANCE run Dijkstra's algorithm over an
// ordinary edge-list table -- exactly the shape ImportRoutingGraph
// (internal/importer/routing_graph.go) already produces (source/target/
// cost columns, one row per edge) but not tied to that importer: any table
// with a source column, a target column and a non-negative numeric weight
// column works.
//
// This follows GEO_SEARCH's table-valued-function convention (table name
// and column names passed as string arguments, not identifiers the parser
// resolves) rather than adding graph traversal to the query planner, which
// only ever recognizes per-row predicates -- see spatial_index.go's doc
// comment for the same reasoning GEO_SEARCH already gives. Like GEO_SEARCH,
// the derived adjacency structure is cached by tenant, table version, column
// set and direction. Repeated routes therefore pay only the shortest-path
// search, while writes invalidate through Table.Version and DROP/rollback
// purge the derived graph eagerly.
//
// Only non-negative edge weights are accepted (Dijkstra's own precondition);
// a graph with negative weights needs Bellman-Ford, which is out of scope
// here the same way a general reprojection engine is out of scope for
// ST_TRANSFORM (geo_transform.go) -- both are real features with a real
// algorithmic cost this codebase has no present need to carry.
package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func getRouteFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"ROUTE_DISTANCE":       evalRouteDistance,
		"ROUTE_DISTANCE_ASTAR": evalRouteDistanceAStar,
		"ROUTE_AIR_DISTANCE":   evalRouteAirDistance,
	}
}

type routeGraph struct {
	table      *storage.Table
	nodeIndex  routeNodeIndex
	nodeValues []any
	// The graph is a structure-of-arrays CSR. Outgoing edge indices for node n
	// are offsets[n]:offsets[n+1]. Separating destination, weight and source-row
	// data lets cost-only searches avoid reading row indices and reduces a
	// 64-bit route edge from the former 24-byte struct to 16 bytes of arrays.
	offsets []uint32
	to      []uint32
	weights []float64
	rowIdx  []uint32
	// The graph itself is version-scoped, so cached distance answers become
	// unreachable automatically when a table mutation replaces this graph.
	distances routeDistanceCache
	// Coordinate bindings are built only for explicit A* calls. The key is a
	// version-scoped coordinate index pointer, so a node-table write naturally
	// creates a new binding without touching ordinary Dijkstra searches.
	coordinateMu       sync.Mutex
	coordinateBindings map[*routeCoordinates]*routeGraphCoordinates
}

const routeGraphCacheMaxEntries = 64
const routeGraphCoordinateBindingsMaxEntries = 8

type routeGraphCacheKey struct {
	tenant                          string
	table                           string
	sourceCol, targetCol, weightCol string
	direction                       string
}

type routeGraphCacheEntry struct {
	table   *storage.Table
	version int
	graph   *routeGraph
}

type routeGraphBuildCall struct {
	done  chan struct{}
	graph *routeGraph
	err   error
}

var routeGraphCacheState = struct {
	sync.RWMutex
	entries map[routeGraphCacheKey]routeGraphCacheEntry
	builds  map[routeGraphCacheKey]*routeGraphBuildCall
}{
	entries: make(map[routeGraphCacheKey]routeGraphCacheEntry),
	builds:  make(map[routeGraphCacheKey]*routeGraphBuildCall),
}

func routeGraphKey(tenant string, table *storage.Table, sourceCol, targetCol, weightCol, direction string) routeGraphCacheKey {
	return routeGraphCacheKey{
		tenant: tenant, table: strings.ToLower(table.Name),
		sourceCol: strings.ToLower(sourceCol), targetCol: strings.ToLower(targetCol),
		weightCol: strings.ToLower(weightCol), direction: direction,
	}
}

// getRouteGraph returns an immutable adjacency graph for the current table
// version and coalesces concurrent cold builds for the same routing shape.
func getRouteGraph(ctx context.Context, tenant string, table *storage.Table, sourceCol, targetCol, weightCol, direction string) (*routeGraph, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := routeGraphKey(tenant, table, sourceCol, targetCol, weightCol, direction)

	routeGraphCacheState.RLock()
	entry, ok := routeGraphCacheState.entries[key]
	routeGraphCacheState.RUnlock()
	if ok && entry.table == table && entry.version == table.Version {
		return entry.graph, nil
	}

	routeGraphCacheState.Lock()
	entry, ok = routeGraphCacheState.entries[key]
	if ok && entry.table == table && entry.version == table.Version {
		routeGraphCacheState.Unlock()
		return entry.graph, nil
	}
	if call := routeGraphCacheState.builds[key]; call != nil {
		routeGraphCacheState.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			// A canceled first caller must not make every healthy waiter fail.
			// Retry so one of them becomes the next builder under its own context.
			if call.err != nil && ctx.Err() == nil {
				return getRouteGraph(ctx, tenant, table, sourceCol, targetCol, weightCol, direction)
			}
			return call.graph, call.err
		}
	}
	call := &routeGraphBuildCall{done: make(chan struct{})}
	routeGraphCacheState.builds[key] = call
	routeGraphCacheState.Unlock()

	version := table.Version
	call.graph, call.err = buildRouteGraph(ctx, table, sourceCol, targetCol, weightCol, direction)

	routeGraphCacheState.Lock()
	delete(routeGraphCacheState.builds, key)
	if call.err == nil && table.Version == version {
		if _, exists := routeGraphCacheState.entries[key]; !exists {
			evictOverCap(routeGraphCacheState.entries, routeGraphCacheMaxEntries)
		}
		routeGraphCacheState.entries[key] = routeGraphCacheEntry{table: table, version: version, graph: call.graph}
	}
	close(call.done)
	routeGraphCacheState.Unlock()
	return call.graph, call.err
}

func purgeRouteGraphCachesFor(tenant, table string) {
	if tenant == "" {
		tenant = "default"
	}
	table = strings.ToLower(table)
	routeGraphCacheState.Lock()
	for key := range routeGraphCacheState.entries {
		if key.tenant == tenant && key.table == table {
			delete(routeGraphCacheState.entries, key)
		}
	}
	routeGraphCacheState.Unlock()
	purgeRouteCoordinateCachesFor(tenant, table)
}

// routeNodeID is the allocation-free canonical form used by the graph index.
// A type tag keeps text "5" distinct from numeric 5. Numeric values use their
// IEEE representation after geoFloat normalization, so int64(5) and
// float64(5) still identify the same node without formatting strings per edge.
type routeNodeID struct {
	kind   uint8
	number uint64
	text   string
}

// Separate maps keep numeric keys at eight bytes instead of making every map
// bucket carry the larger tagged string-capable routeNodeID struct.
type routeNodeIndex struct {
	numbers  map[uint64]int
	texts    map[string]int
	capacity int
}

func newRouteNodeIndex(capacity int) routeNodeIndex {
	return routeNodeIndex{capacity: capacity}
}

func (idx *routeNodeIndex) get(key routeNodeID) (int, bool) {
	if key.kind == 1 {
		value, ok := idx.texts[key.text]
		return value, ok
	}
	value, ok := idx.numbers[key.number]
	return value, ok
}

func (idx *routeNodeIndex) put(key routeNodeID, value int) {
	if key.kind == 1 {
		if idx.texts == nil {
			idx.texts = make(map[string]int, idx.capacity)
		}
		idx.texts[key.text] = value
		return
	}
	if idx.numbers == nil {
		idx.numbers = make(map[uint64]int, idx.capacity)
	}
	idx.numbers[key.number] = value
}

func routeNodeKey(v any) (routeNodeID, error) {
	if v == nil {
		return routeNodeID{}, fmt.Errorf("node id must not be NULL")
	}
	if s, ok := v.(string); ok {
		return routeNodeID{kind: 1, text: s}, nil
	}
	if f, err := geoFloat(v); err == nil {
		bits := math.Float64bits(f)
		if math.IsNaN(f) {
			// FormatFloat previously canonicalized every NaN as the same key.
			bits = math.Float64bits(math.NaN())
		}
		return routeNodeID{kind: 2, number: bits}, nil
	}
	return routeNodeID{}, fmt.Errorf("unsupported node id type %T", v)
}

// buildRouteGraph scans the table once, building an adjacency list keyed by a
// dense integer node index (nodeValues[i] holds that node's
// original id value, in whichever type it was first seen as, for reporting
// back in results). direction "undirected" adds both directions of travel
// for each edge row; "directed" (the default) adds only source->target.
func buildRouteGraph(ctx context.Context, table *storage.Table, sourceCol, targetCol, weightCol, direction string) (*routeGraph, error) {
	if uint64(len(table.Rows)) > uint64(math.MaxUint32) {
		return nil, fmt.Errorf("routing table has %d rows; maximum supported is %d", len(table.Rows), uint64(math.MaxUint32))
	}
	srcIdx, err := table.ColIndex(sourceCol)
	if err != nil {
		return nil, fmt.Errorf("source column: %w", err)
	}
	tgtIdx, err := table.ColIndex(targetCol)
	if err != nil {
		return nil, fmt.Errorf("target column: %w", err)
	}
	wIdx, err := table.ColIndex(weightCol)
	if err != nil {
		return nil, fmt.Errorf("weight column: %w", err)
	}

	nodeCapacity := len(table.Rows)
	if nodeCapacity <= int(^uint(0)>>1)-2 {
		nodeCapacity += 2
	}
	edgeCapacity := len(table.Rows)
	if direction == "undirected" && edgeCapacity <= int(^uint(0)>>1)/2 {
		edgeCapacity *= 2
	}
	g := &routeGraph{
		table: table, nodeIndex: newRouteNodeIndex(nodeCapacity),
		nodeValues: make([]any, 0, nodeCapacity), offsets: make([]uint32, 0, nodeCapacity+1),
		to: make([]uint32, 0, edgeCapacity), weights: make([]float64, 0, edgeCapacity), rowIdx: make([]uint32, 0, edgeCapacity),
	}
	// First this stores each edge's source. It is later overwritten with the
	// stable CSR destination position and becomes the in-place permutation.
	sourceOrPosition := make([]uint32, 0, edgeCapacity)
	nodeIdxFor := func(v any) (int, error) {
		key, err := routeNodeKey(v)
		if err != nil {
			return 0, err
		}
		if idx, ok := g.nodeIndex.get(key); ok {
			return idx, nil
		}
		idx := len(g.nodeValues)
		if uint64(idx) >= uint64(math.MaxUint32) {
			return 0, fmt.Errorf("routing graph has more than %d nodes", uint64(math.MaxUint32))
		}
		g.nodeIndex.put(key, idx)
		g.nodeValues = append(g.nodeValues, v)
		g.offsets = append(g.offsets, 0)
		return idx, nil
	}
	appendEdge := func(source, target, row int, weight float64) error {
		if uint64(len(sourceOrPosition)) >= uint64(math.MaxUint32) {
			return fmt.Errorf("routing graph has more than %d directed edges", uint64(math.MaxUint32))
		}
		if g.offsets[source] == math.MaxUint32 {
			return fmt.Errorf("routing node has more than %d outgoing edges", uint64(math.MaxUint32))
		}
		g.offsets[source]++
		sourceOrPosition = append(sourceOrPosition, uint32(source))
		g.to = append(g.to, uint32(target))
		g.weights = append(g.weights, weight)
		g.rowIdx = append(g.rowIdx, uint32(row))
		return nil
	}
	// Validate and decode every row exactly once. The payload arrays are already
	// final; only one uint32 source position per directed edge is temporary.
	for rowIdx, r := range table.Rows {
		if rowIdx&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if srcIdx >= len(r) || tgtIdx >= len(r) || wIdx >= len(r) {
			continue
		}
		srcVal, tgtVal, wVal := r[srcIdx], r[tgtIdx], r[wIdx]
		if srcVal == nil || tgtVal == nil || wVal == nil {
			continue // NULL endpoint or weight: not a usable edge
		}
		weight, err := geoFloat(wVal)
		if err != nil {
			return nil, fmt.Errorf("row %d: weight column %q: %w", rowIdx, weightCol, err)
		}
		if weight < 0 || weight != weight { // negative or NaN
			return nil, fmt.Errorf("row %d: edge weight %v is negative or not-a-number; Dijkstra requires non-negative weights", rowIdx, wVal)
		}
		srcNode, err := nodeIdxFor(srcVal)
		if err != nil {
			return nil, fmt.Errorf("row %d: source id: %w", rowIdx, err)
		}
		tgtNode, err := nodeIdxFor(tgtVal)
		if err != nil {
			return nil, fmt.Errorf("row %d: target id: %w", rowIdx, err)
		}
		if err := appendEdge(srcNode, tgtNode, rowIdx, weight); err != nil {
			return nil, err
		}
		if direction == "undirected" {
			if err := appendEdge(tgtNode, srcNode, rowIdx, weight); err != nil {
				return nil, err
			}
		}
	}

	// Turn degrees into offsets in the same backing array, then assign stable
	// bucket positions in input order. This preserves equal-cost tie behavior.
	g.offsets = append(g.offsets, uint32(len(sourceOrPosition)))
	var prefix uint32
	for node := 0; node < len(g.nodeValues); node++ {
		degree := g.offsets[node]
		g.offsets[node] = prefix
		prefix += degree
	}
	g.offsets[len(g.nodeValues)] = prefix
	cursors := append([]uint32(nil), g.offsets[:len(g.nodeValues)]...)
	for edge, source := range sourceOrPosition {
		sourceOrPosition[edge] = cursors[source]
		cursors[source]++
	}
	// Apply the position permutation to all structure-of-arrays payloads in
	// place, avoiding a second graph-sized edge allocation.
	for edge := range sourceOrPosition {
		for int(sourceOrPosition[edge]) != edge {
			destination := int(sourceOrPosition[edge])
			g.to[edge], g.to[destination] = g.to[destination], g.to[edge]
			g.weights[edge], g.weights[destination] = g.weights[destination], g.weights[edge]
			g.rowIdx[edge], g.rowIdx[destination] = g.rowIdx[destination], g.rowIdx[edge]
			sourceOrPosition[edge], sourceOrPosition[destination] = sourceOrPosition[destination], sourceOrPosition[edge]
		}
	}
	return g, nil
}

func normalizeRouteDirection(name, raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "directed":
		return "directed", nil
	case "undirected":
		return "undirected", nil
	default:
		return "", fmt.Errorf("%s: unknown direction %q (supported: directed, undirected)", name, raw)
	}
}

// routeHeapEntry/routeHeap replicate container/heap's up/down algorithm
// directly on a concrete type instead of going through heap.Interface, the
// same trade this codebase already makes for VEC_SEARCH's candidate heap
// (see vecScoredHeap in vector_search.go): Dijkstra's relaxation loop can
// push one entry per edge considered, and boxing each one into `any` for
// heap.Push/Pop would be an avoidable allocation per edge on a large graph.
type routeHeapEntry struct {
	node int
	cost float64
}

type routeHeap []routeHeapEntry

func (h routeHeap) Len() int { return len(h) }
func routeHeapEntryLess(a, b routeHeapEntry) bool {
	if a.cost == b.cost {
		return a.node < b.node
	}
	return a.cost < b.cost
}

func routeHeapPush(h *routeHeap, v routeHeapEntry) {
	*h = append(*h, v)
	a := *h
	hole := len(a) - 1
	for hole > 0 {
		parent := (hole - 1) / 2
		if !routeHeapEntryLess(v, a[parent]) {
			break
		}
		a[hole] = a[parent]
		hole = parent
	}
	a[hole] = v
}

func routeHeapPop(h *routeHeap) routeHeapEntry {
	a := *h
	root := a[0]
	lastIndex := len(a) - 1
	last := a[lastIndex]
	a = a[:lastIndex]
	if len(a) != 0 {
		hole := 0
		for {
			left := 2*hole + 1
			if left >= len(a) {
				break
			}
			child := left
			if right := left + 1; right < len(a) && routeHeapEntryLess(a[right], a[left]) {
				child = right
			}
			if !routeHeapEntryLess(a[child], last) {
				break
			}
			a[hole] = a[child]
			hole = child
		}
		a[hole] = last
	}
	*h = a
	return root
}

// routeStep is one node in a reconstructed shortest path: the node index,
// the edge-table row that was traveled to reach it (-1 for the start node),
// and the cumulative cost from the start through this node.
type routeStep struct {
	node       int
	viaRowIdx  int
	cumulative float64
}

// routeSearchScratch owns Dijkstra's O(nodes) working memory. The graph is
// immutable and shared, but distances/predecessors/visited state are private to
// one query. Pooling these arrays avoids allocating several full-graph slices
// for every warm route; generation marks make reset O(1), as in Karte.Bayern's
// routing scratch implementation.
type routeSearchScratch struct {
	dist               []float64
	prevNode, prevEdge []int
	generation         []uint32
	mark               uint32
	heap               routeHeap
}

const routeScratchHotMaxBytes = 64 << 20

var routeSearchScratchPool = sync.Pool{New: func() any { return &routeSearchScratch{} }}
var routeSearchScratchHot struct {
	sync.Mutex
	scratch *routeSearchScratch
}

const routeDistanceCacheMaxEntries = 4096

type routeDistanceCacheKey struct {
	start int
	end   int
}

type routeDistanceCacheValue struct {
	distance float64
	found    bool
}

// routeDistanceCache uses bounded FIFO replacement. Repeated map requests
// often ask for the same origin/destination pair, and a cached unreachable
// answer is just as useful as a cached distance.
type routeDistanceCache struct {
	sync.RWMutex
	entries map[routeDistanceCacheKey]routeDistanceCacheValue
	order   []routeDistanceCacheKey
	next    int
}

func (c *routeDistanceCache) load(key routeDistanceCacheKey) (routeDistanceCacheValue, bool) {
	c.RLock()
	value, ok := c.entries[key]
	c.RUnlock()
	return value, ok
}

func (c *routeDistanceCache) store(key routeDistanceCacheKey, value routeDistanceCacheValue) {
	c.Lock()
	defer c.Unlock()
	if c.entries == nil {
		// Most graphs see a small hot set. Grow on demand instead of charging
		// every cold graph for all 4096 possible cache slots up front.
		c.entries = make(map[routeDistanceCacheKey]routeDistanceCacheValue)
	}
	if _, exists := c.entries[key]; exists {
		c.entries[key] = value
		return
	}
	if len(c.order) < routeDistanceCacheMaxEntries {
		c.order = append(c.order, key)
	} else {
		delete(c.entries, c.order[c.next])
		c.order[c.next] = key
		c.next++
		if c.next == len(c.order) {
			c.next = 0
		}
	}
	c.entries[key] = value
}

func acquireRouteSearchScratch(nodes int, needParents bool) *routeSearchScratch {
	routeSearchScratchHot.Lock()
	scratch := routeSearchScratchHot.scratch
	routeSearchScratchHot.scratch = nil
	routeSearchScratchHot.Unlock()
	if scratch == nil {
		scratch = routeSearchScratchPool.Get().(*routeSearchScratch)
	}
	if cap(scratch.dist) < nodes {
		scratch.dist = make([]float64, nodes)
		scratch.generation = make([]uint32, nodes)
	} else {
		scratch.dist = scratch.dist[:nodes]
		scratch.generation = scratch.generation[:nodes]
	}
	if needParents {
		if cap(scratch.prevNode) < nodes {
			scratch.prevNode = make([]int, nodes)
		} else {
			scratch.prevNode = scratch.prevNode[:nodes]
		}
		if cap(scratch.prevEdge) < nodes {
			scratch.prevEdge = make([]int, nodes)
		} else {
			scratch.prevEdge = scratch.prevEdge[:nodes]
		}
	} else {
		scratch.prevNode = scratch.prevNode[:0]
		scratch.prevEdge = scratch.prevEdge[:0]
	}
	scratch.heap = scratch.heap[:0]
	scratch.mark++
	if scratch.mark == 0 {
		clear(scratch.generation)
		scratch.mark = 1
	}
	return scratch
}

func releaseRouteSearchScratch(scratch *routeSearchScratch) {
	scratch.heap = scratch.heap[:0]
	if routeSearchScratchBytes(scratch) <= routeScratchHotMaxBytes {
		routeSearchScratchHot.Lock()
		if routeSearchScratchHot.scratch == nil || routeSearchScratchBytes(scratch) > routeSearchScratchBytes(routeSearchScratchHot.scratch) {
			pooled := routeSearchScratchHot.scratch
			routeSearchScratchHot.scratch = scratch
			routeSearchScratchHot.Unlock()
			if pooled != nil {
				routeSearchScratchPool.Put(pooled)
			}
			return
		}
		routeSearchScratchHot.Unlock()
	}
	routeSearchScratchPool.Put(scratch)
}

func routeSearchScratchBytes(scratch *routeSearchScratch) int {
	if scratch == nil {
		return 0
	}
	return cap(scratch.dist)*8 + cap(scratch.prevNode)*8 + cap(scratch.prevEdge)*8 +
		cap(scratch.generation)*4 + cap(scratch.heap)*16
}

func (scratch *routeSearchScratch) distance(node int, inf float64) float64 {
	if scratch.generation[node] != scratch.mark {
		return inf
	}
	return scratch.dist[node]
}

func (scratch *routeSearchScratch) setDistance(node int, distance float64) {
	scratch.generation[node] = scratch.mark
	scratch.dist[node] = distance
}

// dijkstraShortestPath returns the shortest path from start to end as a
// sequence of steps (including both endpoints), or found=false if end is
// unreachable from start. Search stops as soon as end is popped off the
// heap (its shortest distance is then final), the standard early-exit that
// avoids exploring the rest of the graph once the answer is known.
func dijkstraShortestPath(g *routeGraph, start, end int) (path []routeStep, found bool) {
	path, _, found = dijkstraSearch(g, start, end, true)
	return path, found
}

// dijkstraShortestDistance is the cost-only counterpart used by
// ROUTE_DISTANCE. Karte.Bayern uses the same separation for matrix/cost
// queries: without a requested path there is no reason to write parents or
// allocate and reverse thousands of route steps.
func dijkstraShortestDistance(g *routeGraph, start, end int) (distance float64, found bool) {
	key := routeDistanceCacheKey{start: start, end: end}
	if cached, ok := g.distances.load(key); ok {
		return cached.distance, cached.found
	}
	_, distance, found = dijkstraSearch(g, start, end, false)
	g.distances.store(key, routeDistanceCacheValue{distance: distance, found: found})
	return distance, found
}

func dijkstraSearch(g *routeGraph, start, end int, wantPath bool) (path []routeStep, distance float64, found bool) {
	n := len(g.nodeValues)
	const inf = 1e308 // effectively +Inf but still comparable/subtractable without producing NaN
	scratch := acquireRouteSearchScratch(n, wantPath)
	defer releaseRouteSearchScratch(scratch)
	dist, prevNode, prevEdge := scratch.dist, scratch.prevNode, scratch.prevEdge
	scratch.setDistance(start, 0)
	if wantPath {
		prevNode[start], prevEdge[start] = -1, -1
	}

	scratch.heap = append(scratch.heap, routeHeapEntry{node: start, cost: 0})
	reached := false
	for len(scratch.heap) > 0 {
		cur := routeHeapPop(&scratch.heap)
		if cur.cost != scratch.distance(cur.node, inf) {
			continue
		}
		if cur.node == end {
			reached = true
			break
		}
		for edge := g.offsets[cur.node]; edge < g.offsets[cur.node+1]; edge++ {
			to := int(g.to[edge])
			nd := dist[cur.node] + g.weights[edge]
			if nd < scratch.distance(to, inf) {
				scratch.setDistance(to, nd)
				if wantPath {
					prevNode[to] = cur.node
					prevEdge[to] = int(g.rowIdx[edge])
				}
				routeHeapPush(&scratch.heap, routeHeapEntry{node: to, cost: nd})
			}
		}
	}

	if !reached {
		return nil, 0, false
	}
	distance = dist[end]
	if !wantPath {
		return nil, distance, true
	}
	return reconstructRoutePath(scratch, start, end), distance, true
}

func reconstructRoutePath(scratch *routeSearchScratch, start, end int) []routeStep {
	var rev []routeStep
	for at := end; at != -1; at = scratch.prevNode[at] {
		rev = append(rev, routeStep{node: at, viaRowIdx: scratch.prevEdge[at], cumulative: scratch.dist[at]})
		if at == start {
			break
		}
	}
	path := make([]routeStep, len(rev))
	for i, s := range rev {
		path[len(rev)-1-i] = s
	}
	return path
}

// routeArgError names which positional argument failed, matching this
// file's shared 6-or-7-argument shape across both the table function and
// the scalar convenience function.
func routeArgError(name string, idx int, err error) error {
	return fmt.Errorf("%s arg%d: %w", name, idx+1, err)
}

// evalRouteGraphArgs evaluates the shared argument prefix both
// ROUTE_SHORTEST_PATH and ROUTE_DISTANCE take:
// (table, source_col, target_col, weight_col, start_id, end_id [, direction]).
func evalRouteGraphArgs(env ExecEnv, name string, args []Expr, row Row) (table *storage.Table, sourceCol, targetCol, weightCol string, startKey, endKey routeNodeID, direction string, err error) {
	if len(args) < 6 || len(args) > 7 {
		err = fmt.Errorf("%s requires (table, source_col, target_col, weight_col, start_id, end_id [, direction]), got %d arguments", name, len(args))
		return
	}
	strArg := func(idx int) (string, error) {
		v, evErr := evalExpr(env, args[idx], row)
		if evErr != nil {
			return "", evErr
		}
		s, ok := v.(string)
		if !ok {
			return "", routeArgError(name, idx, fmt.Errorf("expected a string, got %T", v))
		}
		return s, nil
	}
	tableName, err := strArg(0)
	if err != nil {
		return
	}
	sourceCol, err = strArg(1)
	if err != nil {
		return
	}
	targetCol, err = strArg(2)
	if err != nil {
		return
	}
	weightCol, err = strArg(3)
	if err != nil {
		return
	}
	startVal, evErr := evalExpr(env, args[4], row)
	if evErr != nil {
		err = evErr
		return
	}
	startKey, err = routeNodeKey(startVal)
	if err != nil {
		err = routeArgError(name, 4, err)
		return
	}
	endVal, evErr := evalExpr(env, args[5], row)
	if evErr != nil {
		err = evErr
		return
	}
	endKey, err = routeNodeKey(endVal)
	if err != nil {
		err = routeArgError(name, 5, err)
		return
	}
	direction = "directed"
	if len(args) == 7 {
		raw, evErr2 := strArg(6)
		if evErr2 != nil {
			err = evErr2
			return
		}
		direction, err = normalizeRouteDirection(name, raw)
		if err != nil {
			return
		}
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err = env.db.Get(tenant, tableName)
	if err != nil {
		err = fmt.Errorf("%s: table %q not found: %w", name, tableName, err)
		return
	}
	return
}

// ── ROUTE_SHORTEST_PATH (table-valued function) ─────────────────────────

type RouteShortestPathTableFunc struct{}

func (f *RouteShortestPathTableFunc) Name() string { return "ROUTE_SHORTEST_PATH" }

func (f *RouteShortestPathTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 6 || len(args) > 7 {
		return fmt.Errorf("ROUTE_SHORTEST_PATH requires (table, source_col, target_col, weight_col, start_id, end_id [, direction]), got %d arguments", len(args))
	}
	return nil
}

func (f *RouteShortestPathTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	table, sourceCol, targetCol, weightCol, startKey, endKey, direction, err := evalRouteGraphArgs(env, "ROUTE_SHORTEST_PATH", args, row)
	if err != nil {
		return nil, err
	}
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	if ctx == nil {
		ctx = env.ctx
	}
	g, err := getRouteGraph(ctx, tenant, table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, fmt.Errorf("ROUTE_SHORTEST_PATH: %w", err)
	}
	startIdx, ok := g.nodeIndex.get(startKey)
	if !ok {
		return nil, fmt.Errorf("ROUTE_SHORTEST_PATH: start id not found among %q/%q values", sourceCol, targetCol)
	}
	endIdx, ok := g.nodeIndex.get(endKey)
	if !ok {
		return nil, fmt.Errorf("ROUTE_SHORTEST_PATH: end id not found among %q/%q values", sourceCol, targetCol)
	}

	resultCols := []string{"seq", "node_id", "edge_id", "leg_cost", "total_cost", "geometry"}
	edgeIDIdx, hasEdgeID := -1, false
	if i, err := table.ColIndex("edge_id"); err == nil {
		edgeIDIdx, hasEdgeID = i, true
	}
	geomIdx, hasGeom := -1, false
	if i, err := table.ColIndex("geometry"); err == nil {
		geomIdx, hasGeom = i, true
	}

	path, found := dijkstraShortestPath(g, startIdx, endIdx)
	if !found {
		return &ResultSet{Cols: resultCols, Rows: nil}, nil
	}

	rows := make([]Row, 0, len(path))
	prevCost := 0.0
	for seq, step := range path {
		r := make(Row, len(resultCols))
		r["seq"] = seq
		r["node_id"] = g.nodeValues[step.node]
		r["total_cost"] = step.cumulative
		if step.viaRowIdx < 0 {
			r["edge_id"] = nil
			r["leg_cost"] = nil
			r["geometry"] = nil
		} else {
			r["leg_cost"] = step.cumulative - prevCost
			if hasEdgeID && edgeIDIdx < len(table.Rows[step.viaRowIdx]) {
				r["edge_id"] = table.Rows[step.viaRowIdx][edgeIDIdx]
			} else {
				r["edge_id"] = nil
			}
			if hasGeom && geomIdx < len(table.Rows[step.viaRowIdx]) {
				r["geometry"] = table.Rows[step.viaRowIdx][geomIdx]
			} else {
				r["geometry"] = nil
			}
		}
		prevCost = step.cumulative
		rows = append(rows, r)
	}
	return &ResultSet{Cols: resultCols, Rows: rows}, nil
}

func init() {
	RegisterTableFunc(&RouteShortestPathTableFunc{})
}

// ── ROUTE_DISTANCE (scalar convenience function) ────────────────────────

// evalRouteDistance runs the same Dijkstra search as ROUTE_SHORTEST_PATH but
// returns just the total cost (or NULL if unreachable), for the common case
// of wanting a distance/duration value in an ordinary SELECT list without
// needing the full path.
func evalRouteDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	table, sourceCol, targetCol, weightCol, startKey, endKey, direction, err := evalRouteGraphArgs(env, ex.Name, ex.Args, row)
	if err != nil {
		return nil, err
	}
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	g, err := getRouteGraph(env.ctx, tenant, table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	startIdx, ok := g.nodeIndex.get(startKey)
	if !ok {
		return nil, fmt.Errorf("%s: start id not found among %q/%q values", ex.Name, sourceCol, targetCol)
	}
	endIdx, ok := g.nodeIndex.get(endKey)
	if !ok {
		return nil, fmt.Errorf("%s: end id not found among %q/%q values", ex.Name, sourceCol, targetCol)
	}
	distance, found := dijkstraShortestDistance(g, startIdx, endIdx)
	if !found {
		return nil, nil
	}
	return distance, nil
}
