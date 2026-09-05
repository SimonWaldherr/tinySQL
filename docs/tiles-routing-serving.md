# Using tinySQL for tile serving and routing

## Supported scope

| Use case | Available now | Application/preprocessing work still required |
| --- | --- | --- |
| Tile server | `cmd/tinyTiles`: immutable artifacts, pooled readers, XYZ HTTP, TileJSON, revisioned TMS sync, ETags and a bounded payload cache | Generate styled raster/MVT tiles; configure the application's deployment |
| Routing engine | Directed/undirected weighted graphs, Dijkstra, validated A*, shortest-path steps, distances, distance matrices, reachable nodes and warm-up | Use the new `routing` package and `cmd/tinyroute` for OSM snapping, car/bicycle/foot profiles, turn restrictions and HTTP; see [OSM routing](osm-routing.md) |

A tile artifact contains rendering output, not a routable road network. Build
and version the route graph separately, ideally recording the same source
revision. tinyTiles does not become a routing server merely by loading a map.
The current routing graph is held in memory; there is no CH/MLD preprocessing
or validated country-scale navigation performance claim.

The OSM routing implementation and its supported scope are documented in
[OSM routing API](osm-routing.md). It computes fresh answers on every request.

## Run the tile server

`cmd/tinyTiles` is a separate Go module within this workspace. From the tinySQL
repository root:

```sh
cd cmd/tinyTiles
go build -tags sqliteimport -o /tmp/tinytiles ./cmd/tinytiles
go build -o /tmp/tinytiles-server ./cmd/tinytiles-server
/tmp/tinytiles import source.mbtiles region.ttiles/
/tmp/tinytiles-server -artifact region.ttiles/ -dataset region \
  -addr 127.0.0.1:8080 -public-base http://127.0.0.1:8080 \
  -readers 4 -max-memory 16777216 -tile-cache 33554432
```

The importer needs `sqliteimport`; the serving binary does not. Point a map
client at `/tilejson.json`, whose tile URLs include the dataset revision.
`/tiles/{z}/{x}/{y}.mvt` is XYZ; `/sync/tiles/{revision}/{z}/{x}/{y}` is TMS.
The row is flipped exactly once at the XYZ boundary.

The standalone server's shared payload/checksum cache defaults to 32 MiB and
4096 entries. `-tile-cache 0` disables it. In the embeddable `server.Config`,
`TileCacheBytes: 0` is disabled; explicitly opt in to the desired budget.
The payload budget excludes map/LRU bookkeeping, which is bounded by the entry
cap. It is additional to each reader's page-cache budget and other process
memory. Large tiles exceeding the budget bypass the payload cache.

The cache is scoped to a Server and its immutable dataset revision; XYZ and
TMS hits reuse the same raw bytes and checksum. Restart/replace the Server
when publishing a new artifact. Do not modify an artifact being served.
Revisioned URLs get immutable HTTP caching, while bare XYZ URLs revalidate.
Requests naming an obsolete revision return 404. Cache hits do not avoid
network transfer unless the client's ETag permits a 304 response.

## Route over an explicit graph

This minimal directed example uses metres as edge weights:

```sql
CREATE TABLE roads (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64);
INSERT INTO roads VALUES
  ('ab', 'A', 'B', 120000),
  ('bc', 'B', 'C', 120000),
  ('ac', 'A', 'C', 300000);
CREATE TABLE road_nodes (node_id TEXT, lat FLOAT64, lon FLOAT64);
INSERT INTO road_nodes VALUES ('A',0,0), ('B',0,1), ('C',0,2);

SELECT * FROM ROUTE_WARM('roads','source','target','cost','directed');
SELECT * FROM ROUTE_SHORTEST_PATH_ASTAR(
  'roads','source','target','cost',
  'road_nodes','node_id','lat','lon','A','C',1);
SELECT * FROM ROUTE_DISTANCE_MATRIX(
  'roads','source','target','cost','["A","B"]','["B","C"]');
SELECT * FROM ROUTE_REACHABLE('roads','source','target','cost','A',240000);
```

The A* scale is a lower bound on cost per metre. With travel time in seconds,
use a sound seconds-per-metre lower bound, not `1`. The implementation checks
it against edge costs and coordinates; `0` disables the heuristic. Directed
edges preserve one-way topology only when the input models it correctly.
Reachability returns nodes/costs, not an isochrone polygon.

Call `ROUTE_WARM` after ingestion/bulk changes, and execute a representative
A* query before admitting traffic to populate coordinate bindings. Keep the DB
open across requests, bind origin/destination parameters through the driver,
and use `ROUTE_DISTANCE_MATRIX` for shared-origin batches rather than one SQL
call per matrix cell. Graph/coordinate version changes invalidate the relevant
caches. Distance-only queries reuse a bounded pair cache; shortest paths still
run the search.

This optimization memoizes the goal heuristic per discovered node inside
query-private pooled scratch memory. Changed origins, goals, scales, graphs
and concurrent requests do not reuse another query's estimates. A* needs an
additional eight bytes per node in its scratch buffer; Dijkstra does not
allocate that buffer itself. Path reconstruction reverses its owned slice in
place instead of allocating a second path.

## Measured serving improvements

Local Apple M2 Max, Go 1.27.1, darwin/arm64; medians of three 700ms runs:

| Benchmark | Before / cache disabled | After / cache enabled |
| --- | ---: | ---: |
| A* core, 10,000-node four-neighbour grid, distance only | 2.748 ms | 1.998 ms |
| A* core, same grid, path included | 2.902 ms | 2.107 ms |
| Path result allocation | 17,104 bytes; 9 allocations | 12,240 bytes; 8 allocations |
| HTTP handler, repeated 36,000-byte tile | 60.461 µs | 2.279 µs |
| HTTP handler allocation | 41,985 bytes | 896 bytes |

Routing measurements bypass the pair-result cache. Tile measurements compare
cache disabled/enabled against the same warm artifact; the HTTP writer discards
the body and excludes network latency. Cold requests and workloads with little
tile reuse will not see the cache-hit speedup. Neither benchmark represents
real-world road routing or a deployment load test.

```sh
# Repository root
go test ./internal/engine -run '^$' -bench '^BenchmarkRouteServingAStar$' \
  -benchmem -benchtime=700ms -count=3
# Nested module
cd cmd/tinyTiles
go test ./server -run '^$' -bench '^BenchmarkTileServerCache$' \
  -benchmem -benchtime=700ms -count=3
```
