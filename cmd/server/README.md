# tinySQL HTTP / gRPC server (`server`)

Serves a tinySQL database over HTTP (JSON REST) and gRPC (JSON codec), with
optional bearer-token auth, TLS, size/timeout limits, trusted proxies, and
peer-to-peer federation for read fan-out.

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
- `POST /api/federated/query` — fan out a read to all peers and merge results;
  accepts `timeout_ms` and `peer_timeout_ms` overrides.
- `GET /api/status` — version, uptime, tenant list.
- `GET /api/cluster/status` — peer health, reachability, response duration.
- `GET /metrics` — Prometheus-compatible metrics (auth-protected).
- `GET /healthz`, `GET /readyz` — probes, `200 OK` when healthy; never
  auth-protected.

## Load testing

From `cmd/server`, `go build -o ../../bin/tinysql-loadtest ./loadtest`; options
in [loadtest/README.md](loadtest/README.md).
