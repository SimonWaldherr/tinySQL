package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupRouteEdges(t *testing.T, db *storage.DB) {
	t.Helper()
	execSQL(t, db, `CREATE TABLE edges (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64)`)
	rows := [][4]string{
		{"e1", "A", "B", "1"},
		{"e2", "B", "C", "2"},
		{"e3", "A", "C", "10"},
		{"e4", "C", "D", "1"},
	}
	for _, r := range rows {
		execSQL(t, db, `INSERT INTO edges (edge_id, source, target, cost) VALUES ('`+r[0]+`','`+r[1]+`','`+r[2]+`',`+r[3]+`)`)
	}
}

func TestRouteShortestPathPicksCheaperRoute(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)

	rs := execSQL(t, db, `SELECT * FROM ROUTE_SHORTEST_PATH('edges','source','target','cost','A','C')`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected a 3-node path (A,B,C), got %d rows: %v", len(rs.Rows), rs.Rows)
	}
	wantNodes := []string{"A", "B", "C"}
	for i, row := range rs.Rows {
		if row["node_id"] != wantNodes[i] {
			t.Errorf("step %d: node_id = %v, want %v", i, row["node_id"], wantNodes[i])
		}
	}
	if row0 := rs.Rows[0]; row0["edge_id"] != nil || row0["leg_cost"] != nil {
		t.Errorf("start node should have nil edge_id/leg_cost: %v", row0)
	}
	last := rs.Rows[len(rs.Rows)-1]
	total, ok := last["total_cost"].(float64)
	if !ok || total != 3 {
		t.Errorf("total_cost at destination: got %v, want 3 (via A->B->C, not the direct cost-10 edge)", last["total_cost"])
	}
}

func TestRouteDistanceMatchesPathCost(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','D') AS v`)
	f, ok := dist.(float64)
	if !ok || f != 4 { // A->B->C->D = 1+2+1
		t.Errorf("ROUTE_DISTANCE A->D: got %v, want 4", dist)
	}
}

func TestRouteDistanceUnreachable(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	execSQL(t, db, `INSERT INTO edges (edge_id, source, target, cost) VALUES ('e5','X','Y',1)`)
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','Y') AS v`)
	if dist != nil {
		t.Errorf("unreachable target should be NULL, got %v", dist)
	}
}

func TestRouteDistanceUnknownNode(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','NoSuchNode') AS v`)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','NoSuchNode','A') AS v`)
}

func TestRouteDirectionality(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE oneway (source TEXT, target TEXT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO oneway (source, target, cost) VALUES ('X','Y',5)`)

	if dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('oneway','source','target','cost','Y','X') AS v`); dist != nil {
		t.Errorf("directed graph should not allow reverse travel: got %v", dist)
	}
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('oneway','source','target','cost','Y','X','undirected') AS v`)
	if f, ok := dist.(float64); !ok || f != 5 {
		t.Errorf("undirected mode should allow reverse travel: got %v", dist)
	}
}

func TestRouteNegativeWeightRejected(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE bad (source TEXT, target TEXT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO bad (source, target, cost) VALUES ('A','B',-1)`)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE('bad','source','target','cost','A','B') AS v`)
}

func TestRouteSameStartAndEnd(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','A') AS v`)
	if f, ok := dist.(float64); !ok || f != 0 {
		t.Errorf("distance from a node to itself should be 0, got %v", dist)
	}
}

func setupAStarRouteTables(t *testing.T, db *storage.DB) {
	t.Helper()
	execSQL(t, db, `CREATE TABLE roads_geo (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO roads_geo VALUES
		('ab','A','B',120000),
		('bc','B','C',120000),
		('ac','A','C',300000)`)
	execSQL(t, db, `CREATE TABLE roads_geo_nodes (node_id TEXT, lat FLOAT64, lon FLOAT64)`)
	execSQL(t, db, `INSERT INTO roads_geo_nodes VALUES
		('A',0,0), ('B',0,1), ('C',0,2)`)
	t.Cleanup(func() {
		purgeRouteGraphCachesFor("default", "roads_geo")
		purgeRouteCoordinateCachesFor("default", "roads_geo_nodes")
	})
}

func TestRouteAirDistanceByNodeID(t *testing.T) {
	db := storage.NewDB()
	setupAStarRouteTables(t, db)

	got := execScalar(t, db, `SELECT ROUTE_AIR_DISTANCE(
		'roads_geo_nodes','node_id','lat','lon','A','C') AS v`)
	distance, ok := got.(float64)
	if !ok || distance < 222000 || distance > 223000 {
		t.Fatalf("ROUTE_AIR_DISTANCE A->C = %v, want about 222.4 km", got)
	}
}

func TestRouteDistanceAStarMatchesDijkstra(t *testing.T) {
	db := storage.NewDB()
	setupAStarRouteTables(t, db)

	query := `SELECT ROUTE_DISTANCE_ASTAR(
		'roads_geo','source','target','cost',
		'roads_geo_nodes','node_id','lat','lon',
		'A','C',1) AS v`
	if got := execScalar(t, db, query); got != float64(240000) {
		t.Fatalf("ROUTE_DISTANCE_ASTAR A->C = %v, want 240000", got)
	}
	if got := execScalar(t, db, `SELECT ROUTE_DISTANCE(
		'roads_geo','source','target','cost','A','C') AS v`); got != float64(240000) {
		t.Fatalf("ROUTE_DISTANCE A->C = %v, want 240000", got)
	}
}

func TestRouteShortestPathAStarReportsAirDistance(t *testing.T) {
	db := storage.NewDB()
	setupAStarRouteTables(t, db)

	rs := execSQL(t, db, `SELECT * FROM ROUTE_SHORTEST_PATH_ASTAR(
		'roads_geo','source','target','cost',
		'roads_geo_nodes','node_id','lat','lon',
		'A','C',1)`)
	if len(rs.Rows) != 3 {
		t.Fatalf("A* path rows = %d, want A/B/C: %#v", len(rs.Rows), rs.Rows)
	}
	if rs.Rows[1]["node_id"] != "B" || rs.Rows[2]["total_cost"] != float64(240000) {
		t.Fatalf("A* path = %#v, want A/B/C at cost 240000", rs.Rows)
	}
	if got, ok := rs.Rows[2]["air_distance_to_goal_m"].(float64); !ok || got != 0 {
		t.Fatalf("goal air distance = %v, want 0", rs.Rows[2]["air_distance_to_goal_m"])
	}
}

func TestRouteAStarRejectsInadmissibleScale(t *testing.T) {
	db := storage.NewDB()
	setupAStarRouteTables(t, db)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE_ASTAR(
		'roads_geo','source','target','cost',
		'roads_geo_nodes','node_id','lat','lon',
		'A','C',2) AS v`)
}

func TestRouteAStarCoordinateCacheInvalidates(t *testing.T) {
	db := storage.NewDB()
	setupAStarRouteTables(t, db)
	query := `SELECT ROUTE_AIR_DISTANCE(
		'roads_geo_nodes','node_id','lat','lon','A','C') AS v`
	before := execScalar(t, db, query).(float64)
	execSQL(t, db, `UPDATE roads_geo_nodes SET lon = 3 WHERE node_id = 'C'`)
	after := execScalar(t, db, query).(float64)
	if after <= before*1.4 {
		t.Fatalf("coordinate cache stayed stale: before=%v after=%v", before, after)
	}
}

func TestRouteDistanceMatrixUsesCartesianInputOrder(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)

	rs := execSQL(t, db, `SELECT * FROM ROUTE_DISTANCE_MATRIX(
		'edges','source','target','cost','["A","B"]','["C","D"]')`)
	if len(rs.Rows) != 4 {
		t.Fatalf("matrix rows = %d, want 4: %#v", len(rs.Rows), rs.Rows)
	}
	wantSources := []any{"A", "A", "B", "B"}
	wantTargets := []any{"C", "D", "C", "D"}
	wantCosts := []any{float64(3), float64(4), float64(2), float64(3)}
	for index, result := range rs.Rows {
		if result["source_id"] != wantSources[index] || result["target_id"] != wantTargets[index] || result["total_cost"] != wantCosts[index] || result["reachable"] != true {
			t.Errorf("matrix row %d = %#v, want %v -> %v at %v", index, result, wantSources[index], wantTargets[index], wantCosts[index])
		}
	}
}

func TestRouteDistanceMatrixReportsUnreachablePairs(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	execSQL(t, db, `INSERT INTO edges VALUES ('xy','X','Y',1)`)

	rs := execSQL(t, db, `SELECT * FROM ROUTE_DISTANCE_MATRIX(
		'edges','source','target','cost','["A"]','["D","Y"]')`)
	if len(rs.Rows) != 2 || rs.Rows[0]["total_cost"] != float64(4) || rs.Rows[1]["total_cost"] != nil || rs.Rows[1]["reachable"] != false {
		t.Fatalf("matrix reachability = %#v, want A->D=4 and A->Y unreachable", rs.Rows)
	}
}

func TestRouteReachableHonorsCostAndLimit(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)

	rs := execSQL(t, db, `SELECT * FROM ROUTE_REACHABLE(
		'edges','source','target','cost','A',3)`)
	if len(rs.Rows) != 3 {
		t.Fatalf("reachable rows = %d, want A/B/C: %#v", len(rs.Rows), rs.Rows)
	}
	wantNodes := []any{"A", "B", "C"}
	wantCosts := []any{float64(0), float64(1), float64(3)}
	for index, result := range rs.Rows {
		if result["rank"] != index+1 || result["node_id"] != wantNodes[index] || result["total_cost"] != wantCosts[index] {
			t.Errorf("reachable row %d = %#v", index, result)
		}
	}

	limited := execSQL(t, db, `SELECT * FROM ROUTE_REACHABLE(
		'edges','source','target','cost','A',100,'directed',2)`)
	if len(limited.Rows) != 2 || limited.Rows[1]["node_id"] != "B" {
		t.Fatalf("limited reachable = %#v, want A/B", limited.Rows)
	}
}

func TestRouteBatchNumericNodeIDs(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE numeric_edges (source INT, target INT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO numeric_edges VALUES (1,2,1), (2,3,1)`)
	rs := execSQL(t, db, `SELECT * FROM ROUTE_DISTANCE_MATRIX(
		'numeric_edges','source','target','cost','[1]','[2,3]')`)
	if len(rs.Rows) != 2 {
		t.Fatalf("numeric matrix rows = %#v", rs.Rows)
	}
	firstTarget, _ := geoFloat(rs.Rows[0]["target_id"])
	if firstTarget != 2 || rs.Rows[1]["total_cost"] != float64(2) {
		t.Fatalf("numeric matrix = %#v", rs.Rows)
	}
}

func TestRouteBatchRejectsInvalidBoundsAndLists(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	execExpectError(t, db, `SELECT * FROM ROUTE_DISTANCE_MATRIX(
		'edges','source','target','cost','null','["C"]')`)
	execExpectError(t, db, `SELECT * FROM ROUTE_REACHABLE(
		'edges','source','target','cost','A',-1)`)
	execExpectError(t, db, `SELECT * FROM ROUTE_REACHABLE(
		'edges','source','target','cost','A',10,'directed',1.5)`)
}

func TestRouteColdBuildPreservesStableEdgeOrder(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE ordered_edges (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO ordered_edges VALUES
		('b1','B','X',1), ('a1','A','X',1), ('c1','C','X',1), ('a2','A','Y',1), ('b2','B','Y',1), ('a3','A','Z',1)`)
	table, err := db.Get("default", "ordered_edges")
	if err != nil {
		t.Fatal(err)
	}
	graph, err := buildRouteGraph(context.Background(), table, "source", "target", "cost", "directed")
	if err != nil {
		t.Fatal(err)
	}
	aNode, ok := graph.nodeIndex.get(routeNodeID{kind: 1, text: "A"})
	if !ok {
		t.Fatal("A missing from graph")
	}
	got := graph.rowIdx[graph.offsets[aNode]:graph.offsets[aNode+1]]
	want := []uint32{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("A edge rows = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("A edge rows = %v, want stable %v", got, want)
		}
	}
	undirected, err := buildRouteGraph(context.Background(), table, "source", "target", "cost", "undirected")
	if err != nil {
		t.Fatal(err)
	}
	xNode, ok := undirected.nodeIndex.get(routeNodeID{kind: 1, text: "X"})
	if !ok {
		t.Fatal("X missing from undirected graph")
	}
	gotReverse := undirected.rowIdx[undirected.offsets[xNode]:undirected.offsets[xNode+1]]
	wantReverse := []uint32{0, 1, 2}
	if len(gotReverse) != len(wantReverse) {
		t.Fatalf("X reverse edge rows = %v, want %v", gotReverse, wantReverse)
	}
	for index := range wantReverse {
		if gotReverse[index] != wantReverse[index] {
			t.Fatalf("X reverse edge rows = %v, want stable %v", gotReverse, wantReverse)
		}
	}
}

func TestRouteWarmBuildsAndReusesGraph(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	purgeRouteGraphCachesFor("default", "edges")

	query := `SELECT * FROM ROUTE_WARM('edges','source','target','cost')`
	first := execSQL(t, db, query)
	if len(first.Rows) != 1 || first.Rows[0]["cache_hit"] != false || first.Rows[0]["node_count"] != 4 || first.Rows[0]["edge_count"] != 4 {
		t.Fatalf("first ROUTE_WARM = %#v", first.Rows)
	}
	second := execSQL(t, db, query)
	if len(second.Rows) != 1 || second.Rows[0]["cache_hit"] != true {
		t.Fatalf("second ROUTE_WARM = %#v", second.Rows)
	}
}

func TestRouteWarmConcurrentCacheHitIsConsistent(t *testing.T) {
	// Regression test: ROUTE_WARM used to compute cache_hit from a peek taken
	// before calling getRouteGraph, so under concurrent cold-cache calls a
	// waiter that reused another goroutine's in-flight build (near-zero
	// elapsed_ms) could still report cache_hit:false from its earlier, now
	// stale peek -- a self-contradictory result. getRouteGraph now reports
	// hit/miss itself, so exactly one of N concurrent cold-cache callers
	// should ever see cache_hit:false: the one that actually built the graph.
	db := storage.NewDB()
	setupRouteEdges(t, db)
	purgeRouteGraphCachesFor("default", "edges")

	const callers = 16
	type result struct {
		hit bool
		err error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT * FROM ROUTE_WARM('edges','source','target','cost')`))
			if err != nil {
				results <- result{err: err}
				return
			}
			hit, _ := rs.Rows[0]["cache_hit"].(bool)
			results <- result{hit: hit}
		}()
	}
	wg.Wait()
	close(results)

	missCount := 0
	for r := range results {
		if r.err != nil {
			t.Fatalf("ROUTE_WARM call failed: %v", r.err)
		}
		if !r.hit {
			missCount++
		}
	}
	if missCount != 1 {
		t.Fatalf("got %d cache_hit:false results among %d concurrent ROUTE_WARM calls, want exactly 1", missCount, callers)
	}
}

func TestRouteGraphCacheReuseInvalidationAndDropPurge(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	table, err := db.Get("default", "edges")
	if err != nil {
		t.Fatal(err)
	}
	key := routeGraphKey("default", table, "source", "target", "cost", "directed")
	t.Cleanup(func() { purgeRouteGraphCachesFor("default", "edges") })

	query := `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','D') AS v`
	if got := execScalar(t, db, query); got != float64(4) {
		t.Fatalf("first route distance = %v, want 4", got)
	}
	routeGraphCacheState.RLock()
	first, ok := routeGraphCacheState.entries[key]
	routeGraphCacheState.RUnlock()
	if !ok || first.graph == nil || first.version != table.Version {
		t.Fatalf("first route did not populate a current cache entry: %#v", first)
	}

	if got := execScalar(t, db, query); got != float64(4) {
		t.Fatalf("cached route distance = %v, want 4", got)
	}
	routeGraphCacheState.RLock()
	second := routeGraphCacheState.entries[key]
	routeGraphCacheState.RUnlock()
	if second.graph != first.graph {
		t.Fatal("unchanged route table rebuilt its cached adjacency graph")
	}

	execSQL(t, db, `INSERT INTO edges VALUES ('fast','A','D',0.5)`)
	if got := execScalar(t, db, query); got != float64(0.5) {
		t.Fatalf("route after INSERT = %v, want new direct cost 0.5", got)
	}
	routeGraphCacheState.RLock()
	updated := routeGraphCacheState.entries[key]
	routeGraphCacheState.RUnlock()
	if updated.graph == first.graph || updated.version != table.Version {
		t.Fatalf("INSERT did not replace the stale graph: old=%p new=%p version=%d table=%d",
			first.graph, updated.graph, updated.version, table.Version)
	}

	execSQL(t, db, `DROP TABLE edges`)
	routeGraphCacheState.RLock()
	_, remains := routeGraphCacheState.entries[key]
	routeGraphCacheState.RUnlock()
	if remains {
		t.Fatal("DROP TABLE left the routing graph cached")
	}
}

func TestRouteGraphCacheConcurrentColdBuild(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	purgeRouteGraphCachesFor("default", "edges")
	t.Cleanup(func() { purgeRouteGraphCachesFor("default", "edges") })
	stmt := mustParse(`SELECT ROUTE_DISTANCE(
		'edges','source','target','cost','A','D') AS distance`)

	const callers = 24
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs, err := Execute(context.Background(), db, "default", stmt)
			if err == nil && (len(rs.Rows) != 1 || rs.Rows[0]["distance"] != float64(4)) {
				err = fmt.Errorf("route result = %#v, want distance 4", rs)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	table, err := db.Get("default", "edges")
	if err != nil {
		t.Fatal(err)
	}
	routeGraphCacheState.RLock()
	entries := 0
	for _, entry := range routeGraphCacheState.entries {
		if entry.table == table {
			entries++
		}
	}
	builds := len(routeGraphCacheState.builds)
	routeGraphCacheState.RUnlock()
	if entries != 1 || builds != 0 {
		t.Fatalf("route cache after concurrent build: entries=%d builds=%d, want 1/0", entries, builds)
	}
}

func benchmarkRouteDB(b *testing.B, edges int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	table := storage.NewTable("bench_routes", []storage.Column{
		{Name: "source", Type: storage.IntType},
		{Name: "target", Type: storage.IntType},
		{Name: "cost", Type: storage.Float64Type},
	}, false)
	table.Rows = make([][]any, edges)
	for i := 0; i < edges; i++ {
		table.Rows[i] = []any{i, i + 1, float64(1)}
	}
	table.Version++
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { purgeRouteGraphCachesFor("default", table.Name) })
	return db
}

func BenchmarkRouteDistanceGraphCache(b *testing.B) {
	const edges = 20000
	db := benchmarkRouteDB(b, edges)
	table, err := db.Get("default", "bench_routes")
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`SELECT ROUTE_DISTANCE(
		'bench_routes','source','target','cost',0,16) AS distance`)
	ctx := context.Background()

	b.Run("warm", func(b *testing.B) {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := Execute(ctx, db, "default", stmt); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("warm_long", func(b *testing.B) {
		longStmt := mustParse(`SELECT ROUTE_DISTANCE(
			'bench_routes','source','target','cost',0,20000) AS distance`)
		if _, err := Execute(ctx, db, "default", longStmt); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := Execute(ctx, db, "default", longStmt); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cold", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			table.Version++
			b.StartTimer()
			if _, err := Execute(ctx, db, "default", stmt); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRouteOneToManyCore(b *testing.B) {
	const edges = 5000
	db := benchmarkRouteDB(b, edges)
	table, err := db.Get("default", "bench_routes")
	if err != nil {
		b.Fatal(err)
	}
	graph, _, err := getRouteGraph(context.Background(), "default", table, "source", "target", "cost", "directed")
	if err != nil {
		b.Fatal(err)
	}
	targets := make([]int, 32)
	for index := range targets {
		targets[index] = edges - len(targets) + index + 1
	}

	b.Run("shared_search", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			graph.distances = routeDistanceCache{}
			b.StartTimer()
			if _, err := routeTargetDistances(context.Background(), graph, 0, targets); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("repeated_searches", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			for _, target := range targets {
				if _, _, found := dijkstraSearch(graph, 0, target, false); !found {
					b.Fatal("target unexpectedly unreachable")
				}
			}
		}
	})
}
