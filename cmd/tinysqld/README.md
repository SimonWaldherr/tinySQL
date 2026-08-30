# tinySQL DBMS daemon (`tinysqld`)

Part of [tinySQL](../../README.md). See the root storage and map-tile sections
before deploying this durable server profile.

The enterprise DBMS entry point, separate from `cmd/server` so the older
HTTP/gRPC API server stays compatible. It opens the `OpenEnterprise` product
profile, requires durable storage, starts the job scheduler through that
profile, exposes a minimal HTTP DBMS API for DB health, storage, scheduler, WAL
and recovery state, and shuts down gracefully on a signal.

```bash
go build ./cmd/tinysqld

./tinysqld -data ./tinysqld-data -storage disk -tenant default -http 127.0.0.1:8088
```

## Flags

| Flag | Default | Description |
|---|---|---|
| `-data` | — | Durable database path or directory |
| `-storage` | `disk` | Storage mode: `memory`, `disk`, `json`, `hybrid`, `index`, `wal`, `advanced_wal`, `paged_index` |
| `-tenant` | `default` | Default tenant |
| `-http` | `127.0.0.1:8088` | HTTP listen address; empty disables HTTP |
| `-auth` | — | Optional bearer token for API endpoints |
| `-request-timeout` | `30s` | Maximum SQL request duration |
| `-http-read-timeout` | `10s` | HTTP read timeout |
| `-http-write-timeout` | `30s` | HTTP write timeout |
| `-shutdown-timeout` | `10s` | Graceful shutdown timeout |
| `-analytics` | `false` | Enable the vector-cache analytics endpoint and event window |
| `-vector-cache-entries` | `0` | `VEC_SEARCH` result-cache entries (0 disables) |
| `-vector-cache-ttl` | `30s` | `VEC_SEARCH` result-cache TTL once entries are enabled |
| `-tiles` | `false` | Enable public XYZ, TileJSON, and metadata tile routes |
| `-check` | `false` | Open the runtime, print status, then exit |

## Tilesets

`-tiles` enables an intentionally public map endpoint for an MBTiles-shaped
table named `{tileset}` and its optional `{tileset}_metadata` table:

```text
GET /tiles/{tileset}/{z}/{x}/{y}.{ext}
GET /tiles/{tileset}.json
GET /tiles/{tileset}/metadata
```

The tile route accepts XYZ coordinates and converts the row to MBTiles/TMS
internally. It sends the appropriate content type/encoding from tileset
metadata, ETags, and `Cache-Control` headers. Keep it disabled unless needed;
put an authenticating proxy in front when a tileset must not be public.

SQL clients can calculate TileMatrix positions, WMS bounding boxes, and CRS
axis order with `TILE_MATRIX_BBOX`, `TILE_MATRIX_POSITION`, `WMS_BBOX`, and
the CRS identifier functions. See the
[geospatial standards guide](../../docs/geospatial-standards.md) for supported
profiles and coordinate conventions.

For a tileset larger than memory, use `-storage paged_index` for the published
tile artifact. See the [root MBTiles guide](../../README.md#map-tiles-and-mbtiles)
for the table shape, import/export, and indexing workflow.

## HTTP API

Unauthenticated:

- `GET /healthz` — DB health snapshot; `503` if storage is closed or closing.
- `GET /readyz` — readiness plus health; `503` while not ready or unhealthy.

Authenticated when `-auth` is set, via `Authorization: Bearer <token>` or
`X-tinySQL-Auth: <token>`:

- `GET /api/status` — runtime status, backend stats, DB health.
- `GET /api/analytics/vector` — vector-cache analytics (requires `-analytics`).
- `POST /api/exec`, `POST /api/query`
- `GET /api/catalog/tables`
- `GET /api/catalog/columns` (real table schemas from `sys.columns`)
- `GET /api/jobs`, `GET /api/job-history`
- `POST /api/jobs/run`

SQL request body; `POST /api/jobs/run` takes `name` instead of `sql`:

```json
{
  "tenant": "default",
  "sql": "SELECT name FROM users",
  "timeout_ms": 5000
}
```

Status responses include a `health` object:

```json
{
  "ok": true,
  "storage_mode": "disk",
  "scheduler_running": true,
  "wal_active": false,
  "advanced_wal_active": false,
  "recovery": {
    "mode": "memory",
    "recovered_transactions": 0,
    "recovered_operations": 0,
    "truncated": false
  }
}
```
