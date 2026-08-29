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
// comment for the same reasoning GEO_SEARCH already gives. Unlike
// GEO_SEARCH, the edge list is scanned and its adjacency structure rebuilt
// on every call rather than cached: a persistent, version-invalidated graph
// cache (mirroring geoGridCache/vecIndexCache) would be the natural
// follow-up if repeated queries against the same large graph show up as a
// bottleneck, but building it up front would be premature for a first
// implementation with no evidence that scan cost dominates.
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
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func getRouteFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"ROUTE_DISTANCE": evalRouteDistance,
	}
}

// routeGraphEdge is one directed adjacency-list entry: travel to node index
// `to` costs `weight`, and doing so uses the edge table's row `rowIdx` (kept
// so a reconstructed path can report which edge -- and its edge_id/geometry
// columns, if the table has them -- was actually taken).
type routeGraphEdge struct {
	to     int
	weight float64
	rowIdx int
}

type routeGraph struct {
	table      *storage.Table
	nodeIndex  map[string]int
	nodeValues []any
	adjacency  [][]routeGraphEdge
}

// routeNodeKey canonicalizes a node id value into a comparable string key.
// Numeric ids are reformatted through geoFloat so that, e.g., an int64(5)
// source column and a float64(5) target column (a common mismatch when one
// side comes from a literal and the other from a stored column) collapse to
// the same node rather than silently becoming two different graph nodes.
func routeNodeKey(v any) (string, error) {
	if v == nil {
		return "", fmt.Errorf("node id must not be NULL")
	}
	if s, ok := v.(string); ok {
		return "s:" + s, nil
	}
	if f, err := geoFloat(v); err == nil {
		return "n:" + strconv.FormatFloat(f, 'g', -1, 64), nil
	}
	return "", fmt.Errorf("unsupported node id type %T", v)
}

// buildRouteGraph scans every row of table once, building an adjacency list
// keyed by a dense integer node index (nodeValues[i] holds that node's
// original id value, in whichever type it was first seen as, for reporting
// back in results). direction "undirected" adds both directions of travel
// for each edge row; "directed" (the default) adds only source->target.
func buildRouteGraph(table *storage.Table, sourceCol, targetCol, weightCol, direction string) (*routeGraph, error) {
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

	g := &routeGraph{table: table, nodeIndex: make(map[string]int)}
	nodeIdxFor := func(v any) (int, error) {
		key, err := routeNodeKey(v)
		if err != nil {
			return 0, err
		}
		if idx, ok := g.nodeIndex[key]; ok {
			return idx, nil
		}
		idx := len(g.nodeValues)
		g.nodeIndex[key] = idx
		g.nodeValues = append(g.nodeValues, v)
		g.adjacency = append(g.adjacency, nil)
		return idx, nil
	}

	for rowIdx, r := range table.Rows {
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
		g.adjacency[srcNode] = append(g.adjacency[srcNode], routeGraphEdge{to: tgtNode, weight: weight, rowIdx: rowIdx})
		if direction == "undirected" {
			g.adjacency[tgtNode] = append(g.adjacency[tgtNode], routeGraphEdge{to: srcNode, weight: weight, rowIdx: rowIdx})
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
func (h routeHeap) Less(i, j int) bool {
	if h[i].cost == h[j].cost {
		return h[i].node < h[j].node
	}
	return h[i].cost < h[j].cost
}
func (h routeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func routeHeapPush(h *routeHeap, v routeHeapEntry) {
	*h = append(*h, v)
	routeHeapUp(*h, len(*h)-1)
}

func routeHeapPop(h *routeHeap) routeHeapEntry {
	old := *h
	n := len(old) - 1
	old.Swap(0, n)
	routeHeapDown(old[:n], 0)
	v := old[n]
	*h = old[:n]
	return v
}

func routeHeapUp(h routeHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func routeHeapDown(h routeHeap, i0 int) {
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

// routeStep is one node in a reconstructed shortest path: the node index,
// the edge-table row that was traveled to reach it (-1 for the start node),
// and the cumulative cost from the start through this node.
type routeStep struct {
	node       int
	viaRowIdx  int
	cumulative float64
}

// dijkstraShortestPath returns the shortest path from start to end as a
// sequence of steps (including both endpoints), or found=false if end is
// unreachable from start. Search stops as soon as end is popped off the
// heap (its shortest distance is then final), the standard early-exit that
// avoids exploring the rest of the graph once the answer is known.
func dijkstraShortestPath(g *routeGraph, start, end int) (path []routeStep, found bool) {
	n := len(g.nodeValues)
	const inf = 1e308 // effectively +Inf but still comparable/subtractable without producing NaN
	dist := make([]float64, n)
	prevNode := make([]int, n)
	prevEdge := make([]int, n)
	visited := make([]bool, n)
	for i := range dist {
		dist[i] = inf
		prevNode[i] = -1
		prevEdge[i] = -1
	}
	dist[start] = 0

	h := routeHeap{{node: start, cost: 0}}
	for len(h) > 0 {
		cur := routeHeapPop(&h)
		if visited[cur.node] {
			continue
		}
		visited[cur.node] = true
		if cur.node == end {
			break
		}
		for _, e := range g.adjacency[cur.node] {
			if visited[e.to] {
				continue
			}
			nd := dist[cur.node] + e.weight
			if nd < dist[e.to] {
				dist[e.to] = nd
				prevNode[e.to] = cur.node
				prevEdge[e.to] = e.rowIdx
				routeHeapPush(&h, routeHeapEntry{node: e.to, cost: nd})
			}
		}
	}

	if !visited[end] {
		return nil, false
	}
	var rev []routeStep
	for at := end; at != -1; at = prevNode[at] {
		rev = append(rev, routeStep{node: at, viaRowIdx: prevEdge[at], cumulative: dist[at]})
		if at == start {
			break
		}
	}
	path = make([]routeStep, len(rev))
	for i, s := range rev {
		path[len(rev)-1-i] = s
	}
	return path, true
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
func evalRouteGraphArgs(env ExecEnv, name string, args []Expr, row Row) (table *storage.Table, sourceCol, targetCol, weightCol string, startKey, endKey string, direction string, err error) {
	if len(args) < 6 || len(args) > 7 {
		return nil, "", "", "", "", "", "", fmt.Errorf("%s requires (table, source_col, target_col, weight_col, start_id, end_id [, direction]), got %d arguments", name, len(args))
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
	g, err := buildRouteGraph(table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, fmt.Errorf("ROUTE_SHORTEST_PATH: %w", err)
	}
	startIdx, ok := g.nodeIndex[startKey]
	if !ok {
		return nil, fmt.Errorf("ROUTE_SHORTEST_PATH: start id not found among %q/%q values", sourceCol, targetCol)
	}
	endIdx, ok := g.nodeIndex[endKey]
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
	g, err := buildRouteGraph(table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	startIdx, ok := g.nodeIndex[startKey]
	if !ok {
		return nil, fmt.Errorf("%s: start id not found among %q/%q values", ex.Name, sourceCol, targetCol)
	}
	endIdx, ok := g.nodeIndex[endKey]
	if !ok {
		return nil, fmt.Errorf("%s: end id not found among %q/%q values", ex.Name, sourceCol, targetCol)
	}
	path, found := dijkstraShortestPath(g, startIdx, endIdx)
	if !found {
		return nil, nil
	}
	return path[len(path)-1].cumulative, nil
}
