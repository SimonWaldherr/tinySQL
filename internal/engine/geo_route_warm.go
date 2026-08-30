package engine

import (
	"context"
	"fmt"
	"time"
)

// ROUTE_WARM eagerly builds the versioned CSR graph after imports or bulk
// updates, moving cold-build latency out of the first user-facing request.
type RouteWarmTableFunc struct{}

func (f *RouteWarmTableFunc) Name() string { return "ROUTE_WARM" }

func (f *RouteWarmTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 4 || len(args) > 5 {
		return fmt.Errorf("ROUTE_WARM requires (table, source_col, target_col, weight_col [, direction])")
	}
	return nil
}

func (f *RouteWarmTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	tableName, err := evalRouteString(env, f.Name(), args, row, 0)
	if err != nil {
		return nil, err
	}
	sourceCol, err := evalRouteString(env, f.Name(), args, row, 1)
	if err != nil {
		return nil, err
	}
	targetCol, err := evalRouteString(env, f.Name(), args, row, 2)
	if err != nil {
		return nil, err
	}
	weightCol, err := evalRouteString(env, f.Name(), args, row, 3)
	if err != nil {
		return nil, err
	}
	direction := "directed"
	if len(args) == 5 {
		raw, directionErr := evalRouteString(env, f.Name(), args, row, 4)
		if directionErr != nil {
			return nil, directionErr
		}
		direction, err = normalizeRouteDirection(f.Name(), raw)
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
		return nil, fmt.Errorf("%s: table %q not found: %w", f.Name(), tableName, err)
	}
	key := routeGraphKey(tenant, table, sourceCol, targetCol, weightCol, direction)
	routeGraphCacheState.RLock()
	entry, cacheHit := routeGraphCacheState.entries[key]
	cacheHit = cacheHit && entry.table == table && entry.version == table.Version
	routeGraphCacheState.RUnlock()
	if ctx == nil {
		ctx = env.ctx
	}
	started := time.Now()
	graph, err := getRouteGraph(ctx, tenant, table, sourceCol, targetCol, weightCol, direction)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	buildMillis := float64(time.Since(started).Nanoseconds()) / 1e6
	columns := []string{"table_name", "direction", "node_count", "edge_count", "cache_hit", "elapsed_ms"}
	result := Row{
		"table_name": table.Name, "direction": direction,
		"node_count": len(graph.nodeValues), "edge_count": len(graph.to),
		"cache_hit": cacheHit, "elapsed_ms": buildMillis,
	}
	return &ResultSet{Cols: columns, Rows: []Row{result}}, nil
}

func init() {
	RegisterTableFunc(&RouteWarmTableFunc{})
}
