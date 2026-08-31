# tinySQL HTTP / gRPC server (`server`)

Part of [tinySQL](../../README.md). See the root guide for the engine feature
set and [tinysqld](../tinysqld/README.md) for the durable DBMS profile.

Serves a tinySQL database over HTTP (JSON/NDJSON REST) and gRPC (Protocol
Buffers, with the earlier JSON codec retained), with optional bearer-token
auth, TLS, size/timeout limits, trusted proxies, and peer-to-peer federation
for read fan-out.

```bash
cd cmd/server && go build -o server .

./server -http :8080 -dsn "mem://?tenant=default"
./server -http :8080 -grpc :9090 -auth "my-secret-token" \
         -dsn "file:/var/lib/tinysql/data.db?tenant=main&autosave=1" \
         -peers "node2:9090,node3:9090"
```

## Flags

### Core

| Flag | Default | Description |
|---|---|---|
| `-dsn` | `mem://?tenant=default` | Storage DSN (`mem://` or `file:/path.db?tenant=...&autosave=1`) |
| `-http` | `:8080` | HTTP listen address (empty disables HTTP) |
| `-grpc` | `:9090` | gRPC listen address (empty disables gRPC) |
| `-auth` | — | Bearer token for HTTP and gRPC |
| `-tenant` | `default` | Default tenant if a request omits one |
| `-peers` | — | Comma-separated `host:grpcPort` peers for federation |
| `-analytics` | `false` | Collect vector-cache analytics and expose vector cache metrics via `/metrics` and `/api/status` (matches `cmd/tinysqld`) |
| `-vector-cache-entries` | `0` | `VEC_SEARCH` result-cache entries (0 disables the cache) |
| `-vector-cache-ttl` | `30s` | `VEC_SEARCH` result-cache TTL once entries are enabled |
| `-v` | `false` | Verbose logging |

### TLS

| Flag | Description |
|---|---|
| `-tls-min-version` | Minimum TLS version, `1.2` (default) or `1.3` |
| `-http-tls-cert` / `-http-tls-key` | HTTP certificate/key; setting both enables HTTPS |
| `-grpc-tls-cert` / `-grpc-tls-key` | gRPC certificate/key |
| `-peer-tls` | Use TLS when calling federation peers |
| `-peer-tls-ca` | CA bundle for peer TLS verification |
| `-peer-tls-server-name` | Server name override for peer TLS verification |
| `-peer-tls-skip-verify` | Skip peer TLS certificate verification (unsafe) |

### Limits

| Flag | Default | Description |
|---|---|---|
| `-max-body-bytes` | `1048576` | Max HTTP request body size (1 MiB) |
| `-max-sql-bytes` | `262144` | Max SQL statement size (256 KiB) |
| `-max-response-rows` | `100000` | Max rows in a query response before truncation (0 = unlimited); a federated query caps the combined total across all peers, not each source independently |
| `-max-response-bytes` | `67108864` | Max approximate JSON-encoded size of a response's rows before truncation (64 MiB; 0 = unlimited) |
| `-max-concurrent-queries` | `32` | Max concurrent Exec/Query executions across HTTP and gRPC (0 = unlimited); bounds engine load independently of `internal/driver`'s reader pool (default 4), which is sized for a single embedded connection |
| `-grpc-max-recv-bytes` | `4194304` | gRPC max receive message size (4 MiB) |
| `-grpc-max-send-bytes` | `4194304` | gRPC max send message size (4 MiB) |

### Timeouts

| Flag | Default | Description |
|---|---|---|
| `-request-timeout` | `30s` | Per-request execution timeout |
| `-peer-timeout` | `10s` | Timeout for federated peer calls |
| `-shutdown-timeout` | `15s` | Graceful shutdown deadline |

### HTTP hardening

| Flag | Default | Description |
|---|---|---|
| `-trusted-proxies` | — | Comma-separated trusted proxy CIDRs/IPs for `X-Forwarded-For` handling |
| `-http-read-timeout` | `15s` | HTTP read timeout |
| `-http-read-header-timeout` | `5s` | HTTP header read timeout |
| `-http-write-timeout` | `30s` | HTTP write timeout |
| `-http-idle-timeout` | `120s` | Keep-alive idle timeout |
| `-http-max-header-bytes` | `1048576` | Max HTTP header size (1 MiB) |

## Authentication

When `-auth` is set, every endpoint except `/healthz` and `/readyz` requires the
token in the `Authorization` header — including `/metrics`. Federated peer calls
send the same token as gRPC `authorization` metadata.

## HTTP API (JSON bodies)

- `POST /api/exec` — DML/DDL; `{"tenant","sql"}` in, `{"rows_affected",
  "elapsed_ms"}` out.
- `POST /api/query` — SELECT with rows; `{"tenant","sql","timeout_ms"}` in
  (`timeout_ms` overrides `-request-timeout`), `{"columns","rows","elapsed_ms"}`
  out.
- `POST /api/query/stream` — local query as an NDJSON result stream; accepts
  the same `{"tenant","sql","timeout_ms"}` body as `/api/query`.
- `POST /api/federated/query` — fan out a read to all peers and merge results;
  accepts `timeout_ms` and `peer_timeout_ms` overrides.
- `GET /api/status` — version, uptime, tenant list.
- `GET /api/cluster/status` — peer health, reachability, response duration.
- `GET /metrics` — Prometheus-compatible metrics (auth-protected).
- `GET /healthz`, `GET /readyz` — probes, `200 OK` when healthy; never
  auth-protected.

### Streaming queries

`POST /api/query/stream` is the preferred tool-facing API for large result
sets and for time-to-first-row-sensitive queries. It returns
`application/x-ndjson`, flushes each record, and uses the normal bearer-token,
request-timeout, execution-slot, `-max-response-rows`, and
`-max-response-bytes` policies.

```bash
curl -N -X POST http://127.0.0.1:8080/api/query/stream \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret-token' \
  --data '{"tenant":"main","sql":"SELECT id, name FROM users"}'
```

Each line is one JSON object. A successful stream sends one `header`, zero or
more `row` records, then one `end` record:

```json
{"type":"header","sql":"SELECT id, name FROM users","columns":["id","name"]}
{"type":"row","row":{"id":1,"name":"Ada"}}
{"type":"end","count":1,"duration":"421µs"}
```

If a response limit is reached, `end.truncated` is `true` and the server
cancels the producer promptly. A malformed request, authentication failure, or query failure
before the header is a normal JSON HTTP error with its HTTP status. If an
execution error occurs after records have begun, HTTP remains `200 OK` and the
final record is `{"type":"error","error":"..."}`. Consumers should process
records incrementally and treat `error` as terminal.

`QueryStream` is the matching gRPC server-streaming RPC at
`/tinysql.TinySQL/QueryStream`. Its schema is
[`tinysql.proto`](./tinysql.proto). Query rows are individual JSON byte fields
inside the protobuf message because SQL cells are dynamically typed; snapshot
and WAL payloads remain raw bytes, avoiding JSON/Base64 expansion. Existing
clients can continue selecting the `json` content subtype. Internal federation
and replication use protobuf by default. Go clients can import the generated
[`protocol`](./protocol) package. A terminal `error` record is followed by a
non-OK gRPC status; startup/validation errors return only that status. Clients
canceling the gRPC context or closing the HTTP response stop the engine stream
and release its execution slot promptly.

Simple single-table scans, filters, index seeks, projections, `LIMIT`, and
`OFFSET` can deliver rows while the scan is still running. Query shapes that
need the full input first—such as `ORDER BY`, aggregates, `DISTINCT`, joins,
and set operations—retain exact semantics and begin streaming only after their
result has been materialized. The streaming endpoints are local only;
federated queries remain materialized at `/api/federated/query`.

## Load testing

From `cmd/server`, `go build -o ../../bin/tinysql-loadtest ./loadtest`; options
in [loadtest/README.md](loadtest/README.md).
