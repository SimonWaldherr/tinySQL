# OSM routing API

`routing` compiles immutable road graphs from OSM XML or a normalized tinySQL
OSM table. `cmd/tinyroute` serves car, bicycle and pedestrian routes. Neither
route answers nor snapping results are cached. Each request owns fresh search
scratch; graph topology, turn rules and the spatial index are prepared at load.

## Run

From the repository root:

```sh
go build -o /tmp/tinyroute ./cmd/tinyroute
/tmp/tinyroute -osm region.osm -profiles car,bicycle,foot -addr 127.0.0.1:8081
# Alternatively, a tinySQL snapshot containing importer.ImportOSM's table:
/tmp/tinyroute -db region.tsql -table osm -profiles car
```

Use a complete regional extract containing referenced nodes, ways and restriction
relations. PBF needs conversion to OSM XML first. This is a snapshot loader,
not an OSM replication/diff consumer. Rebuild and replace the handler when data
changes. Build errors name unsupported rules; do not delete restrictions merely
to make an extract load. The server listens on loopback by default.

```sh
curl 'http://127.0.0.1:8081/route?from=11.57,48.14&to=11.58,48.15&profile=car'
curl 'http://127.0.0.1:8081/nearest?point=11.57,48.14&profile=bicycle&max_snap_meters=100'
curl 'http://127.0.0.1:8081/profiles'
curl -X POST http://127.0.0.1:8081/route -H 'Content-Type: application/json' \
  -d '{"from":{"lon":11.57,"lat":48.14},"to":{"lon":11.58,"lat":48.15},"profile":"foot","max_snap_meters":100}'
```

Coordinates are longitude,latitude (WGS84). Replies contain `distance_meters`,
`duration_seconds`, start/end snaps and a GeoJSON LineString. GET and POST route
requests default to car and a 100 m snap radius; the maximum radius is 5 km.
POST requires both endpoints, rejects unknown fields and accepts at most 8 KiB.
`/healthz` returns 204. Errors are JSON: 400 invalid input/profile, 404 no nearby
street/no route, 408 canceled/timed out, 503 search-state limit. Responses carry
`Cache-Control: no-store`. Route searches have a 10-second HTTP deadline;
`-max-states` defaults to 1,000,000. These are per-request bounds, not a global
concurrency/memory budget. Use deployment-level admission control for public traffic.

## Supported routing semantics

- Exact nearest-point projection onto spherical road segments, including the date
  line and poles; start/end costs include partial segments. Snapping chooses the
  nearest accessible segment for the profile, not the nearest connected alternative.
- Directed edges for one-way roads, reverse one-way roads, roundabouts, bicycle
  exceptions, mode-specific and directional access. Generic one-way rules do not
  constrain pedestrians. Shared OSM node IDs define connectivity; crossing lines
  without a shared node do not become junctions.
- `no_left_turn`, `no_right_turn`, `no_straight_on`, `no_u_turn`, `no_entry`,
  `no_exit`, and corresponding supported `only_*` turns. Via-node and ordered
  via-way relations match directed edge sequences, retaining intermediate shape
  nodes. Mode-specific restrictions and `except` are respected. Restrictions
  also apply when a route starts or finishes partway along an involved road.
- Car, bicycle and foot access policies, barriers, bicycle dismount and numeric
  car speed limits in km/h or mph. Estimated speeds are capped road-class defaults,
  not live traffic predictions. Private/destination/customer/delivery-only access
  is conservatively excluded, even for a destination on that road.

Tag semantics follow the OSM documentation for
[turn restrictions](https://wiki.openstreetmap.org/wiki/Relation:restriction) and
[access tags](https://wiki.openstreetmap.org/wiki/Access_tags). Defaults here are
an explicit simplified policy, not jurisdiction-specific legal defaults.
Conditional tags, vehicle dimension limits, symbolic speed limits, and unsupported
restriction forms produce build errors. There are no departure-time rules, lane
instructions, elevation/surface penalties, ferry timetables, spoken maneuvers,
traffic feeds, map matching or CH/MLD preprocessing. Via-way compilation is bounded
and models simple directed traversals. This is a regional routing foundation,
not a validated replacement for a worldwide navigation service.

## Embed

```go
data, err := routing.ReadOSM(ctx, input)
// Handle err.
car, err := routing.Build(ctx, data, routing.Car, routing.Options{})
// Handle err.
handler := routing.Handler(map[routing.Profile]*routing.Router{routing.Car: car})
// Mount handler on an HTTP server, or call car.Route(ctx, request).
```

`routing.FromDB` builds directly from `importer.ImportOSM`'s normalized columns
(`osm_type`, `osm_id`, `lat`, `lon`, `tags`, `refs`, `members`). A built router
is independent of subsequent DB mutations and supports concurrent requests.

## Performance without answer caches

The spherical BVH prunes road segments before exact projection. CSR adjacency,
a typed binary heap and dense IDs avoid interface boxing and repeated graph
construction in each request. Only states with relevant turn history need sparse
lookup. A* uses a distance/max-speed lower bound; its estimates live only inside
the current search. Isolated OSM nodes are removed before allocating query scratch.

Benchmarks compare exact full-scan snapping against the BVH and Dijkstra against
A*, with no answer reuse. The graph/index is built outside the timed loop. They
measure in-process queries, not disk startup or HTTP/network time. Both diagonal
and axis-aligned grid routes are included: A* can add overhead when its lower bound
does not prune enough nodes. Run separately from other test workloads:

```sh
go test ./routing -run '^$' -bench 'Benchmark(Route|Snap)NoResultCache' \
  -benchmem -benchtime=700ms -count=3
go test -race ./routing
```

Tests cover restriction detours, via-way traversal, partial endpoints, one-way and
access profiles, barriers, date-line/polar snapping, A*/Dijkstra equivalence,
resource limits, cancellation, concurrent queries, XML/table parity and HTTP errors.

Tile rendering remains in the separate `cmd/tinyTiles` service. Disable its
payload cache with `-tile-cache 0`; this does not disable its storage page cache.
See [tile and graph serving](tiles-routing-serving.md) for commands and the
separate cache-on/cache-off measurements. No new uncached tile speedup is claimed
by these routing benchmarks.

Local Apple M2 Max, Go 1.27.1, medians of three 700 ms runs (timings varied
between runs; these are synthetic workloads):

| Workload | Baseline | Indexed / A* |
| --- | ---: | ---: |
| Nearest segment, 19,800 segments | 2.127 ms full scan | 4.196 µs BVH |
| 60×60 grid, axis route | 345.554 µs Dijkstra | 72.036 µs A* |
| Same grid, opposite corners | 1.566 ms Dijkstra | 1.402 ms A* |
| Axis route allocation | 197,555 bytes | 123,825 bytes |

Snapping allocates zero bytes in both variants. These are algorithm comparisons,
not a before/after measurement of a previously existing OSM HTTP service.
