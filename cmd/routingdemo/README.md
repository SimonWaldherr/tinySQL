# Routing demo — no setup required

From the repository root:

```sh
go run ./cmd/routingdemo
go run ./cmd/routingdemo -iterations 1000
go run ./cmd/routingdemo -json

# Select one profile and choose points on or near the embedded network.
go run ./cmd/routingdemo -profile bicycle \
  -from 10.9995,48.00001 -to 11,48.001 -max-snap-meters 20 -json
```

The embedded, synthetic five-node OSM network requires no downloads, model,
API key or tile server. A car must detour around a turn restriction. Bicycles
are explicitly exempt, and pedestrians are unaffected by this vehicle restriction.
Both endpoints are off-road and snap onto partial street segments. Output compares
metres, estimated travel seconds, graph build time and mean query time for all
three profiles. `-json` includes the GeoJSON geometry and snap details.

`-from` and `-to` use **longitude,latitude** in WGS84. The default profile
`all` compares car, bicycle, and foot; `-profile` can select one of them.
`-max-snap-meters` sets the maximum distance to an eligible road, from more
than 0 up to 5000 metres (default 10). Points outside that radius return an
error; the example network covers only the coordinates shown above.

Every iteration computes a fresh route. Graph construction is timed separately;
these tiny-network timings include snapping and are an execution demonstration,
not evidence of regional or country-scale throughput.

To try the **same data over HTTP**:

```sh
go run ./cmd/tinyroute -osm cmd/routingdemo/network.osm
# In a second terminal:
curl 'http://127.0.0.1:8081/route?from=10.9995,48.00001&to=11.0005,48.00001&profile=car&max_snap_meters=10'
curl 'http://127.0.0.1:8081/route?from=10.9995,48.00001&to=11.0005,48.00001&profile=bicycle&max_snap_meters=10'
```

See [OSM routing](../../docs/osm-routing.md) for API details and supported rules.
