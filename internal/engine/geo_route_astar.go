package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const routeCoordinateCacheMaxEntries = 64

type routeCoordinateCacheKey struct {
	tenant                string
	table                 string
	idCol, latCol, lonCol string
}

type routeCoordinates struct {
	table     *storage.Table
	version   int
	nodeIndex routeNodeIndex
	lat       []float64
	lon       []float64
}

type routeCoordinateCacheEntry struct {
	table   *storage.Table
	version int
	coords  *routeCoordinates
}

type routeCoordinateBuildCall struct {
	done   chan struct{}
	coords *routeCoordinates
	err    error
}

var routeCoordinateCacheState = struct {
	sync.RWMutex
	entries map[routeCoordinateCacheKey]routeCoordinateCacheEntry
	builds  map[routeCoordinateCacheKey]*routeCoordinateBuildCall
}{
	entries: make(map[routeCoordinateCacheKey]routeCoordinateCacheEntry),
	builds:  make(map[routeCoordinateCacheKey]*routeCoordinateBuildCall),
}

func routeCoordinateKey(tenant string, table *storage.Table, idCol, latCol, lonCol string) routeCoordinateCacheKey {
	return routeCoordinateCacheKey{
		tenant: tenant, table: strings.ToLower(table.Name),
		idCol: strings.ToLower(idCol), latCol: strings.ToLower(latCol), lonCol: strings.ToLower(lonCol),
	}
}

func purgeRouteCoordinateCachesFor(tenant, table string) {
	if tenant == "" {
		tenant = "default"
	}
	table = strings.ToLower(table)
	routeCoordinateCacheState.Lock()
	for key := range routeCoordinateCacheState.entries {
		if key.tenant == tenant && key.table == table {
			delete(routeCoordinateCacheState.entries, key)
		}
	}
	routeCoordinateCacheState.Unlock()
}

func getRouteCoordinates(ctx context.Context, tenant string, table *storage.Table, idCol, latCol, lonCol string) (*routeCoordinates, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := routeCoordinateKey(tenant, table, idCol, latCol, lonCol)
	for {
		routeCoordinateCacheState.RLock()
		entry, ok := routeCoordinateCacheState.entries[key]
		routeCoordinateCacheState.RUnlock()
		if ok && entry.table == table && entry.version == table.Version {
			return entry.coords, nil
		}

		routeCoordinateCacheState.Lock()
		entry, ok = routeCoordinateCacheState.entries[key]
		if ok && entry.table == table && entry.version == table.Version {
			routeCoordinateCacheState.Unlock()
			return entry.coords, nil
		}
		if call := routeCoordinateCacheState.builds[key]; call != nil {
			routeCoordinateCacheState.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-call.done:
				if call.err != nil && ctx.Err() == nil {
					continue
				}
				return call.coords, call.err
			}
		}
		call := &routeCoordinateBuildCall{done: make(chan struct{})}
		routeCoordinateCacheState.builds[key] = call
		routeCoordinateCacheState.Unlock()

		version := table.Version
		call.coords, call.err = buildRouteCoordinates(ctx, table, idCol, latCol, lonCol)

		routeCoordinateCacheState.Lock()
		delete(routeCoordinateCacheState.builds, key)
		if call.err == nil && table.Version == version {
			if _, exists := routeCoordinateCacheState.entries[key]; !exists {
				evictOverCap(routeCoordinateCacheState.entries, routeCoordinateCacheMaxEntries)
			}
			routeCoordinateCacheState.entries[key] = routeCoordinateCacheEntry{table: table, version: version, coords: call.coords}
		}
		close(call.done)
		routeCoordinateCacheState.Unlock()
		return call.coords, call.err
	}
}

func buildRouteCoordinates(ctx context.Context, table *storage.Table, idCol, latCol, lonCol string) (*routeCoordinates, error) {
	idPos, err := table.ColIndex(idCol)
	if err != nil {
		return nil, fmt.Errorf("node id column: %w", err)
	}
	latPos, err := table.ColIndex(latCol)
	if err != nil {
		return nil, fmt.Errorf("latitude column: %w", err)
	}
	lonPos, err := table.ColIndex(lonCol)
	if err != nil {
		return nil, fmt.Errorf("longitude column: %w", err)
	}
	coords := &routeCoordinates{
		table: table, version: table.Version, nodeIndex: newRouteNodeIndex(len(table.Rows)),
		lat: make([]float64, 0, len(table.Rows)), lon: make([]float64, 0, len(table.Rows)),
	}
	for rowIdx, raw := range table.Rows {
		if rowIdx&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if idPos >= len(raw) || latPos >= len(raw) || lonPos >= len(raw) || raw[idPos] == nil || raw[latPos] == nil || raw[lonPos] == nil {
			return nil, fmt.Errorf("node row %d has NULL or missing id/latitude/longitude", rowIdx)
		}
		key, err := routeNodeKey(raw[idPos])
		if err != nil {
			return nil, fmt.Errorf("node row %d id: %w", rowIdx, err)
		}
		if _, exists := coords.nodeIndex.get(key); exists {
			return nil, fmt.Errorf("node row %d duplicates node id", rowIdx)
		}
		lat, err := geoFloat(raw[latPos])
		if err != nil || math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90 || lat > 90 {
			return nil, fmt.Errorf("node row %d has invalid latitude %v", rowIdx, raw[latPos])
		}
		lon, err := geoFloat(raw[lonPos])
		if err != nil || math.IsNaN(lon) || math.IsInf(lon, 0) || lon < -180 || lon > 180 {
			return nil, fmt.Errorf("node row %d has invalid longitude %v", rowIdx, raw[lonPos])
		}
		index := len(coords.lat)
		coords.nodeIndex.put(key, index)
		coords.lat = append(coords.lat, lat)
		coords.lon = append(coords.lon, lon)
	}
	return coords, nil
}

type routeGraphCoordinates struct {
	lat                []float64
	lon                []float64
	complete           bool
	maxAdmissibleScale float64
}

func bindRouteGraphCoordinates(g *routeGraph, coords *routeCoordinates) (*routeGraphCoordinates, error) {
	g.coordinateMu.Lock()
	defer g.coordinateMu.Unlock()
	if bound := g.coordinateBindings[coords]; bound != nil {
		return bound, nil
	}
	bound := &routeGraphCoordinates{
		lat: make([]float64, len(g.nodeValues)), lon: make([]float64, len(g.nodeValues)),
		complete: true, maxAdmissibleScale: math.Inf(1),
	}
	for graphNode, value := range g.nodeValues {
		key, err := routeNodeKey(value)
		if err != nil {
			return nil, err
		}
		coordinateNode, ok := coords.nodeIndex.get(key)
		if !ok {
			bound.complete = false
			continue
		}
		bound.lat[graphNode] = coords.lat[coordinateNode]
		bound.lon[graphNode] = coords.lon[coordinateNode]
	}
	if bound.complete {
		for from := range g.nodeValues {
			for edge := g.offsets[from]; edge < g.offsets[from+1]; edge++ {
				to := int(g.to[edge])
				meters := haversineMeters(bound.lat[from], bound.lon[from], bound.lat[to], bound.lon[to])
				if meters > 0 {
					ratio := g.weights[edge] / meters
					if ratio < bound.maxAdmissibleScale {
						bound.maxAdmissibleScale = ratio
					}
				}
			}
		}
	}
	if g.coordinateBindings == nil {
		g.coordinateBindings = make(map[*routeCoordinates]*routeGraphCoordinates)
	} else if len(g.coordinateBindings) >= routeGraphCoordinateBindingsMaxEntries {
		// Coordinate tables can change independently of the edge table. Keep a
		// small hot set without letting one long-lived graph retain every former
		// coordinate-table version indefinitely.
		for old := range g.coordinateBindings {
			delete(g.coordinateBindings, old)
			break
		}
	}
	g.coordinateBindings[coords] = bound
	return bound, nil
}

func (coords *routeGraphCoordinates) heuristic(node, goal int, scale float64) float64 {
	if scale == 0 {
		return 0
	}
	return haversineMeters(coords.lat[node], coords.lon[node], coords.lat[goal], coords.lon[goal]) * scale
}

func validateRouteHeuristicScale(coords *routeGraphCoordinates, scale float64) error {
	if math.IsNaN(scale) || math.IsInf(scale, 0) || scale < 0 {
		return fmt.Errorf("minimum cost per metre must be a finite non-negative number")
	}
	if scale == 0 {
		return nil
	}
	if !coords.complete {
		return fmt.Errorf("positive A* heuristic requires coordinates for every graph node")
	}
	if scale > coords.maxAdmissibleScale && !almostEqual(scale, coords.maxAdmissibleScale) {
		return fmt.Errorf("minimum cost per metre %.9g exceeds graph-safe maximum %.9g", scale, coords.maxAdmissibleScale)
	}
	return nil
}

func almostEqual(a, b float64) bool {
	delta := math.Abs(a - b)
	return delta <= 1e-12*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func aStarSearch(g *routeGraph, coords *routeGraphCoordinates, start, end int, scale float64, wantPath bool) (path []routeStep, distance float64, found bool) {
	if start == end {
		if wantPath {
			return []routeStep{{node: start, viaRowIdx: -1}}, 0, true
		}
		return nil, 0, true
	}
	const inf = 1e308
	scratch := acquireRouteSearchScratch(len(g.nodeValues), wantPath)
	defer releaseRouteSearchScratch(scratch)
	scratch.setDistance(start, 0)
	if wantPath {
		scratch.prevNode[start], scratch.prevEdge[start] = -1, -1
	}
	routeHeapPush(&scratch.heap, routeHeapEntry{node: start, cost: coords.heuristic(start, end, scale)})
	for len(scratch.heap) > 0 {
		current := routeHeapPop(&scratch.heap)
		currentDistance := scratch.distance(current.node, inf)
		if current.cost != currentDistance+coords.heuristic(current.node, end, scale) {
			continue
		}
		if current.node == end {
			distance = currentDistance
			if !wantPath {
				return nil, distance, true
			}
			return reconstructRoutePath(scratch, start, end), distance, true
		}
		for edge := g.offsets[current.node]; edge < g.offsets[current.node+1]; edge++ {
			to := int(g.to[edge])
			candidate := currentDistance + g.weights[edge]
			if candidate >= scratch.distance(to, inf) {
				continue
			}
			scratch.setDistance(to, candidate)
			if wantPath {
				scratch.prevNode[to] = current.node
				scratch.prevEdge[to] = int(g.rowIdx[edge])
			}
			routeHeapPush(&scratch.heap, routeHeapEntry{node: to, cost: candidate + coords.heuristic(to, end, scale)})
		}
	}
	return nil, 0, false
}

func evalRouteAStarArgs(env ExecEnv, name string, args []Expr, row Row) (table *storage.Table, coords *routeCoordinates, sourceCol, targetCol, weightCol string, startKey, endKey routeNodeID, scale float64, direction string, err error) {
	if len(args) < 11 || len(args) > 12 {
		err = fmt.Errorf("%s requires (edge_table, source_col, target_col, weight_col, node_table, node_id_col, lat_col, lon_col, start_id, end_id, min_cost_per_metre [, direction]), got %d arguments", name, len(args))
		return
	}
	stringAt := func(index int) (string, error) {
		value, evalErr := evalExpr(env, args[index], row)
		if evalErr != nil {
			return "", evalErr
		}
		text, ok := value.(string)
		if !ok {
			return "", routeArgError(name, index, fmt.Errorf("expected a string, got %T", value))
		}
		return text, nil
	}
	edgeTableName, err := stringAt(0)
	if err != nil {
		return
	}
	sourceCol, err = stringAt(1)
	if err != nil {
		return
	}
	targetCol, err = stringAt(2)
	if err != nil {
		return
	}
	weightCol, err = stringAt(3)
	if err != nil {
		return
	}
	nodeTableName, err := stringAt(4)
	if err != nil {
		return
	}
	nodeIDCol, err := stringAt(5)
	if err != nil {
		return
	}
	latCol, err := stringAt(6)
	if err != nil {
		return
	}
	lonCol, err := stringAt(7)
	if err != nil {
		return
	}
	startValue, evalErr := evalExpr(env, args[8], row)
	if evalErr != nil {
		err = evalErr
		return
	}
	startKey, err = routeNodeKey(startValue)
	if err != nil {
		err = routeArgError(name, 8, err)
		return
	}
	endValue, evalErr := evalExpr(env, args[9], row)
	if evalErr != nil {
		err = evalErr
		return
	}
	endKey, err = routeNodeKey(endValue)
	if err != nil {
		err = routeArgError(name, 9, err)
		return
	}
	scaleValue, evalErr := evalExpr(env, args[10], row)
	if evalErr != nil {
		err = evalErr
		return
	}
	scale, err = geoFloat(scaleValue)
	if err != nil {
		err = routeArgError(name, 10, err)
		return
	}
	direction = "directed"
	if len(args) == 12 {
		raw, directionErr := stringAt(11)
		if directionErr != nil {
			err = directionErr
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
	table, err = env.db.Get(tenant, edgeTableName)
	if err != nil {
		err = fmt.Errorf("%s: table %q not found: %w", name, edgeTableName, err)
		return
	}
	nodeTable, nodeErr := env.db.Get(tenant, nodeTableName)
	if nodeErr != nil {
		err = fmt.Errorf("%s: node table %q not found: %w", name, nodeTableName, nodeErr)
		return
	}
	coords, err = getRouteCoordinates(env.ctx, tenant, nodeTable, nodeIDCol, latCol, lonCol)
	return
}

func prepareAStar(env ExecEnv, name string, args []Expr, row Row) (*routeGraph, *routeGraphCoordinates, *storage.Table, int, int, float64, error) {
	table, coordinateIndex, sourceCol, targetCol, weightCol, startKey, endKey, scale, direction, err := evalRouteAStarArgs(env, name, args, row)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	graph, err := getRouteGraph(env.ctx, tenant, table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, fmt.Errorf("%s: %w", name, err)
	}
	start, ok := graph.nodeIndex.get(startKey)
	if !ok {
		return nil, nil, nil, 0, 0, 0, fmt.Errorf("%s: start id not found among %q/%q values", name, sourceCol, targetCol)
	}
	end, ok := graph.nodeIndex.get(endKey)
	if !ok {
		return nil, nil, nil, 0, 0, 0, fmt.Errorf("%s: end id not found among %q/%q values", name, sourceCol, targetCol)
	}
	bound, err := bindRouteGraphCoordinates(graph, coordinateIndex)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, fmt.Errorf("%s: bind coordinates: %w", name, err)
	}
	if err := validateRouteHeuristicScale(bound, scale); err != nil {
		return nil, nil, nil, 0, 0, 0, fmt.Errorf("%s: %w", name, err)
	}
	return graph, bound, table, start, end, scale, nil
}

func evalRouteDistanceAStar(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	graph, coords, _, start, end, scale, err := prepareAStar(env, ex.Name, ex.Args, row)
	if err != nil {
		return nil, err
	}
	key := routeDistanceCacheKey{start: start, end: end}
	if cached, ok := graph.distances.load(key); ok {
		if !cached.found {
			return nil, nil
		}
		return cached.distance, nil
	}
	_, distance, found := aStarSearch(graph, coords, start, end, scale, false)
	graph.distances.store(key, routeDistanceCacheValue{distance: distance, found: found})
	if !found {
		return nil, nil
	}
	return distance, nil
}

func evalRouteAirDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 6 {
		return nil, fmt.Errorf("%s requires (node_table, node_id_col, lat_col, lon_col, start_id, end_id), got %d arguments", ex.Name, len(ex.Args))
	}
	stringAt := func(index int) (string, error) {
		value, err := evalExpr(env, ex.Args[index], row)
		if err != nil {
			return "", err
		}
		text, ok := value.(string)
		if !ok {
			return "", routeArgError(ex.Name, index, fmt.Errorf("expected a string, got %T", value))
		}
		return text, nil
	}
	tableName, err := stringAt(0)
	if err != nil {
		return nil, err
	}
	idCol, err := stringAt(1)
	if err != nil {
		return nil, err
	}
	latCol, err := stringAt(2)
	if err != nil {
		return nil, err
	}
	lonCol, err := stringAt(3)
	if err != nil {
		return nil, err
	}
	startValue, err := evalExpr(env, ex.Args[4], row)
	if err != nil {
		return nil, err
	}
	endValue, err := evalExpr(env, ex.Args[5], row)
	if err != nil {
		return nil, err
	}
	startKey, err := routeNodeKey(startValue)
	if err != nil {
		return nil, routeArgError(ex.Name, 4, err)
	}
	endKey, err := routeNodeKey(endValue)
	if err != nil {
		return nil, routeArgError(ex.Name, 5, err)
	}
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("%s: node table %q not found: %w", ex.Name, tableName, err)
	}
	coords, err := getRouteCoordinates(env.ctx, tenant, table, idCol, latCol, lonCol)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	start, ok := coords.nodeIndex.get(startKey)
	if !ok {
		return nil, fmt.Errorf("%s: start node id not found", ex.Name)
	}
	end, ok := coords.nodeIndex.get(endKey)
	if !ok {
		return nil, fmt.Errorf("%s: end node id not found", ex.Name)
	}
	return haversineMeters(coords.lat[start], coords.lon[start], coords.lat[end], coords.lon[end]), nil
}

type RouteShortestPathAStarTableFunc struct{}

func (f *RouteShortestPathAStarTableFunc) Name() string { return "ROUTE_SHORTEST_PATH_ASTAR" }

func (f *RouteShortestPathAStarTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 11 || len(args) > 12 {
		return fmt.Errorf("ROUTE_SHORTEST_PATH_ASTAR requires 11-12 arguments, got %d", len(args))
	}
	return nil
}

func (f *RouteShortestPathAStarTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if ctx != nil {
		env.ctx = ctx
	}
	graph, coords, table, start, end, scale, err := prepareAStar(env, f.Name(), args, row)
	if err != nil {
		return nil, err
	}
	if !coords.complete {
		return nil, fmt.Errorf("%s: path output requires coordinates for every graph node", f.Name())
	}
	path, _, found := aStarSearch(graph, coords, start, end, scale, true)
	resultCols := []string{"seq", "node_id", "edge_id", "leg_cost", "total_cost", "air_distance_to_goal_m", "geometry"}
	if !found {
		return &ResultSet{Cols: resultCols}, nil
	}
	edgeIDPos, edgeIDErr := table.ColIndex("edge_id")
	geometryPos, geometryErr := table.ColIndex("geometry")
	rows := make([]Row, 0, len(path))
	previousCost := 0.0
	for sequence, step := range path {
		result := make(Row, len(resultCols))
		result["seq"] = sequence
		result["node_id"] = graph.nodeValues[step.node]
		result["total_cost"] = step.cumulative
		result["air_distance_to_goal_m"] = haversineMeters(coords.lat[step.node], coords.lon[step.node], coords.lat[end], coords.lon[end])
		if step.viaRowIdx < 0 {
			result["edge_id"], result["leg_cost"], result["geometry"] = nil, nil, nil
		} else {
			result["leg_cost"] = step.cumulative - previousCost
			if edgeIDErr == nil && edgeIDPos < len(table.Rows[step.viaRowIdx]) {
				result["edge_id"] = table.Rows[step.viaRowIdx][edgeIDPos]
			}
			if geometryErr == nil && geometryPos < len(table.Rows[step.viaRowIdx]) {
				result["geometry"] = table.Rows[step.viaRowIdx][geometryPos]
			}
		}
		previousCost = step.cumulative
		rows = append(rows, result)
	}
	return &ResultSet{Cols: resultCols, Rows: rows}, nil
}

func init() {
	RegisterTableFunc(&RouteShortestPathAStarTableFunc{})
}
