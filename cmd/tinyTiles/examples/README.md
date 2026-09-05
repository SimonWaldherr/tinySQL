# Demos

The examples are intentionally small reference integrations. They do not add
authentication, rate limiting or deployment policy.

## 1. Serve a `.ttiles` artifact

```bash
make build-server
./dist/tinytiles-server \
  -artifact /path/to/dataset.ttiles \
  -dataset dach \
  -cors http://localhost:8081
```

The server exposes:

- `GET /sync/manifest.json` — current immutable revision and tile URL template;
- `GET /sync/tiles/{revision}/{z}/{x}/{y}` — one raw TMS tile, cacheable by revision;
- `GET /tiles/{z}/{x}/{y}.mvt` — standard XYZ tile endpoint for map clients;
- `GET /tilejson.json` and `GET /metadata` — generic map metadata;
- `GET /healthz` — liveness endpoint.

`-cors` is intentionally opt-in. Use an explicit browser origin in real
deployments; `*` is suitable only for a local demo. Behind a proxy, supply
`-public-base https://tiles.example` so the manifest never derives its public
URL from an inbound Host header.

## 2. Persist a native offline range

```bash
go run ./examples/native-client \
  -manifest http://localhost:8080/sync/manifest.json \
  -cache ./demo-cache \
  -dataset dach \
  -z 8 -xmin 137 -xmax 138 -ymin 167 -ymax 168
```

The client keeps old and new revisions in distinct namespaces and switches its
active manifest only after every requested tile is present.

## 3. Browser / WASM

In one terminal, start the demo server with a CORS origin matching port 8081.
In a second terminal:

```bash
make serve-wasm
```

Open `http://localhost:8081`. The browser demo uses IndexedDB, never
localStorage, and its `tinyTiles` JavaScript API returns Promises. The server
must use `-cors http://localhost:8081` (or an equivalent allowed origin) so
the browser can read the checksum and raw tile-encoding headers.

The standalone binary is intentionally a small integration. Do not expose it directly
to the internet without authentication, TLS, resource limits, rate limiting
and deployment-specific observability.

The WASM demo downloads its module once, including when the server does not set
`application/wasm`. Actions remain disabled until initialization and while another
operation is running. Open the cache before syncing or reading. The demo validates
integer coordinates, zoom bounds, worker count and a maximum of 1,024 tiles per
sync before starting network work, and displays elapsed operation time. A dataset
name is required for reading a tile. The offline storage is intentional; disable
the server's separate payload cache with `-tile-cache 0` when comparing serving
without that cache.

Test the demo controller without a browser or WASM build:

```sh
node --test examples/wasm/app.test.cjs
```
