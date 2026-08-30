package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

// routeTargetDistances resolves a set of destinations with one Dijkstra run.
// Pair-cache hits are removed before the search, and the heap stops as soon as
// every remaining target is finalized. This is substantially cheaper than one
// ROUTE_DISTANCE invocation per matrix cell when destinations share a source.
func routeTargetDistances(ctx context.Context, g *routeGraph, start int, targets []int) (map[int]routeDistanceCacheValue, error) {
	results := make(map[int]routeDistanceCacheValue, len(targets))
	pending := make(map[int]struct{}, len(targets))
	for _, target := range targets {
		key := routeDistanceCacheKey{start: start, end: target}
		if cached, ok := g.distances.load(key); ok {
			results[target] = cached
		} else {
			pending[target] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return results, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	const inf = 1e308
	scratch := acquireRouteSearchScratch(len(g.nodeValues), false)
	defer releaseRouteSearchScratch(scratch)
	scratch.setDistance(start, 0)
	routeHeapPush(&scratch.heap, routeHeapEntry{node: start, cost: 0})
	pops := 0
	for len(scratch.heap) > 0 {
		current := routeHeapPop(&scratch.heap)
		if current.cost != scratch.distance(current.node, inf) {
			continue
		}
		pops++
		if (pops-1)&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if _, wanted := pending[current.node]; wanted {
			value := routeDistanceCacheValue{distance: current.cost, found: true}
			results[current.node] = value
			g.distances.store(routeDistanceCacheKey{start: start, end: current.node}, value)
			delete(pending, current.node)
			if len(pending) == 0 {
				return results, nil
			}
		}
		for edge := g.offsets[current.node]; edge < g.offsets[current.node+1]; edge++ {
			to := int(g.to[edge])
			candidate := current.cost + g.weights[edge]
			if candidate >= scratch.distance(to, inf) {
				continue
			}
			scratch.setDistance(to, candidate)
			routeHeapPush(&scratch.heap, routeHeapEntry{node: to, cost: candidate})
		}
	}
	for target := range pending {
		value := routeDistanceCacheValue{found: false}
		results[target] = value
		g.distances.store(routeDistanceCacheKey{start: start, end: target}, value)
	}
	return results, nil
}

type routeReachableNode struct {
	node     int
	distance float64
}

func routeReachable(ctx context.Context, g *routeGraph, start int, maxCost float64, limit int) ([]routeReachableNode, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	const inf = 1e308
	scratch := acquireRouteSearchScratch(len(g.nodeValues), false)
	defer releaseRouteSearchScratch(scratch)
	scratch.setDistance(start, 0)
	routeHeapPush(&scratch.heap, routeHeapEntry{node: start, cost: 0})
	result := make([]routeReachableNode, 0, min(len(g.nodeValues), reachableCapacityHint(limit)))
	for len(scratch.heap) > 0 {
		current := routeHeapPop(&scratch.heap)
		if current.cost != scratch.distance(current.node, inf) {
			continue
		}
		if current.cost > maxCost {
			break
		}
		if len(result)&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		result = append(result, routeReachableNode{node: current.node, distance: current.cost})
		if limit > 0 && len(result) >= limit {
			break
		}
		for edge := g.offsets[current.node]; edge < g.offsets[current.node+1]; edge++ {
			to := int(g.to[edge])
			candidate := current.cost + g.weights[edge]
			if candidate > maxCost || candidate >= scratch.distance(to, inf) {
				continue
			}
			scratch.setDistance(to, candidate)
			routeHeapPush(&scratch.heap, routeHeapEntry{node: to, cost: candidate})
		}
	}
	return result, nil
}

func reachableCapacityHint(limit int) int {
	if limit > 0 {
		return limit
	}
	return 256
}

func evalRouteString(env ExecEnv, name string, args []Expr, row Row, index int) (string, error) {
	value, err := evalExpr(env, args[index], row)
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", routeArgError(name, index, fmt.Errorf("expected a string, got %T", value))
	}
	return text, nil
}

func prepareRouteBatchGraph(ctx context.Context, env ExecEnv, name string, args []Expr, row Row, directionIndex int) (*routeGraph, error) {
	tableName, err := evalRouteString(env, name, args, row, 0)
	if err != nil {
		return nil, err
	}
	sourceCol, err := evalRouteString(env, name, args, row, 1)
	if err != nil {
		return nil, err
	}
	targetCol, err := evalRouteString(env, name, args, row, 2)
	if err != nil {
		return nil, err
	}
	weightCol, err := evalRouteString(env, name, args, row, 3)
	if err != nil {
		return nil, err
	}
	direction := "directed"
	if directionIndex >= 0 && len(args) > directionIndex {
		raw, directionErr := evalRouteString(env, name, args, row, directionIndex)
		if directionErr != nil {
			return nil, directionErr
		}
		direction, err = normalizeRouteDirection(name, raw)
		if err != nil {
			return nil, err
		}
	}
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("%s: table %q not found: %w", name, tableName, err)
	}
	if ctx == nil {
		ctx = env.ctx
	}
	graph, _, err := getRouteGraph(ctx, tenant, table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return graph, nil
}

func parseRouteNodeList(value any) ([]routeNodeID, error) {
	var values []any
	switch typed := value.(type) {
	case string:
		decoder := json.NewDecoder(bytes.NewBufferString(typed))
		decoder.UseNumber()
		var decoded any
		if err := decoder.Decode(&decoded); err != nil {
			return nil, fmt.Errorf("invalid JSON node array: %w", err)
		}
		var ok bool
		values, ok = decoded.([]any)
		if !ok {
			return nil, fmt.Errorf("expected a JSON node array, got %T", decoded)
		}
		if err := ensureRouteJSONEOF(decoder); err != nil {
			return nil, err
		}
	case []any:
		values = typed
	case []string:
		values = make([]any, len(typed))
		for i := range typed {
			values[i] = typed[i]
		}
	default:
		return nil, fmt.Errorf("expected a JSON array string or array, got %T", value)
	}
	keys := make([]routeNodeID, len(values))
	for index, node := range values {
		key, err := routeNodeKey(node)
		if err != nil {
			return nil, fmt.Errorf("node %d: %w", index, err)
		}
		keys[index] = key
	}
	return keys, nil
}

func ensureRouteJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return fmt.Errorf("invalid JSON node array: %w", err)
	}
	return nil
}

func resolveRouteNodes(name, role string, graph *routeGraph, keys []routeNodeID) ([]int, error) {
	nodes := make([]int, len(keys))
	for index, key := range keys {
		node, ok := graph.nodeIndex.get(key)
		if !ok {
			return nil, fmt.Errorf("%s: %s node %d not found in graph", name, role, index)
		}
		nodes[index] = node
	}
	return nodes, nil
}

// ROUTE_DISTANCE_MATRIX computes the Cartesian product of source and target
// JSON arrays. Sources are grouped so each distinct source performs at most one
// search, and repeated matrix calls reuse the ordinary pair-distance cache.
type RouteDistanceMatrixTableFunc struct{}

func (f *RouteDistanceMatrixTableFunc) Name() string { return "ROUTE_DISTANCE_MATRIX" }

func (f *RouteDistanceMatrixTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 6 || len(args) > 7 {
		return fmt.Errorf("ROUTE_DISTANCE_MATRIX requires (table, source_col, target_col, weight_col, source_ids_json, target_ids_json [, direction])")
	}
	return nil
}

func (f *RouteDistanceMatrixTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = env.ctx
	}
	graph, err := prepareRouteBatchGraph(ctx, env, f.Name(), args, row, 6)
	if err != nil {
		return nil, err
	}
	sourceValue, err := evalExpr(env, args[4], row)
	if err != nil {
		return nil, err
	}
	targetValue, err := evalExpr(env, args[5], row)
	if err != nil {
		return nil, err
	}
	sourceKeys, err := parseRouteNodeList(sourceValue)
	if err != nil {
		return nil, routeArgError(f.Name(), 4, err)
	}
	targetKeys, err := parseRouteNodeList(targetValue)
	if err != nil {
		return nil, routeArgError(f.Name(), 5, err)
	}
	sources, err := resolveRouteNodes(f.Name(), "source", graph, sourceKeys)
	if err != nil {
		return nil, err
	}
	targets, err := resolveRouteNodes(f.Name(), "target", graph, targetKeys)
	if err != nil {
		return nil, err
	}
	columns := []string{"source_id", "target_id", "total_cost", "reachable"}
	if len(sources) == 0 || len(targets) == 0 {
		return &ResultSet{Cols: columns}, nil
	}
	if len(sources) > math.MaxInt/len(targets) {
		return nil, fmt.Errorf("%s: matrix result is too large", f.Name())
	}
	bySource := make(map[int]map[int]routeDistanceCacheValue, len(sources))
	for _, source := range sources {
		if _, exists := bySource[source]; exists {
			continue
		}
		values, searchErr := routeTargetDistances(ctx, graph, source, targets)
		if searchErr != nil {
			return nil, searchErr
		}
		bySource[source] = values
	}
	rows := make([]Row, 0, len(sources)*len(targets))
	for _, source := range sources {
		for _, target := range targets {
			value := bySource[source][target]
			result := Row{
				"source_id": graph.nodeValues[source], "target_id": graph.nodeValues[target],
				"reachable": value.found, "total_cost": nil,
			}
			if value.found {
				result["total_cost"] = value.distance
			}
			rows = append(rows, result)
		}
	}
	return &ResultSet{Cols: columns, Rows: rows}, nil
}

// ROUTE_REACHABLE returns nodes in nondecreasing shortest-cost order, bounded
// by max_cost and optionally by result count. It is the graph-side primitive
// for service areas, isochrones and "nearest by travel time" queries.
type RouteReachableTableFunc struct{}

func (f *RouteReachableTableFunc) Name() string { return "ROUTE_REACHABLE" }

func (f *RouteReachableTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 6 || len(args) > 8 {
		return fmt.Errorf("ROUTE_REACHABLE requires (table, source_col, target_col, weight_col, start_id, max_cost [, direction [, limit]])")
	}
	return nil
}

func (f *RouteReachableTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = env.ctx
	}
	graph, err := prepareRouteBatchGraph(ctx, env, f.Name(), args, row, 6)
	if err != nil {
		return nil, err
	}
	startValue, err := evalExpr(env, args[4], row)
	if err != nil {
		return nil, err
	}
	startKey, err := routeNodeKey(startValue)
	if err != nil {
		return nil, routeArgError(f.Name(), 4, err)
	}
	start, ok := graph.nodeIndex.get(startKey)
	if !ok {
		return nil, fmt.Errorf("%s: start node not found in graph", f.Name())
	}
	maxCostValue, err := evalExpr(env, args[5], row)
	if err != nil {
		return nil, err
	}
	maxCost, err := geoFloat(maxCostValue)
	if err != nil || math.IsNaN(maxCost) || math.IsInf(maxCost, 0) || maxCost < 0 {
		return nil, routeArgError(f.Name(), 5, fmt.Errorf("max_cost must be a finite non-negative number"))
	}
	limit := 0
	if len(args) == 8 {
		limitValue, evalErr := evalExpr(env, args[7], row)
		if evalErr != nil {
			return nil, evalErr
		}
		limitFloat, floatErr := geoFloat(limitValue)
		if floatErr != nil || math.IsNaN(limitFloat) || math.IsInf(limitFloat, 0) || limitFloat < 0 || math.Trunc(limitFloat) != limitFloat || limitFloat > float64(math.MaxInt) {
			return nil, routeArgError(f.Name(), 7, fmt.Errorf("limit must be a non-negative integer"))
		}
		limit = int(limitFloat)
	}
	reachable, err := routeReachable(ctx, graph, start, maxCost, limit)
	if err != nil {
		return nil, err
	}
	columns := []string{"rank", "node_id", "total_cost"}
	rows := make([]Row, len(reachable))
	for rank, item := range reachable {
		rows[rank] = Row{"rank": rank + 1, "node_id": graph.nodeValues[item.node], "total_cost": item.distance}
	}
	return &ResultSet{Cols: columns, Rows: rows}, nil
}

func init() {
	RegisterTableFunc(&RouteDistanceMatrixTableFunc{})
	RegisterTableFunc(&RouteReachableTableFunc{})
}
