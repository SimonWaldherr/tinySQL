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
