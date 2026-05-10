# tinySQL HTTP / gRPC Server (`server`)

A production-oriented server that exposes a tinySQL database over HTTP (JSON
REST API) and gRPC (JSON codec). Supports optional bearer-token authentication,
TLS on both transports, request size and timeout limits, trusted-proxy
configuration, and peer-to-peer federation for read fan-out across multiple
instances.

## Build

```bash
go build -o server ./cmd/server
```

## Quick start

```bash
# In-memory, no auth, HTTP only
./server -http :8080 -dsn "mem://?tenant=default"

# File-backed with auth token
./server -http :8080 -dsn "file:/var/lib/tinysql/data.db?tenant=main&autosave=1" \
         -auth "my-secret-token"

# HTTP + gRPC, with peer federation
./server -http :8080 -grpc :9090 \
         -dsn "mem://?tenant=default" \
         -peers "node2:9090,node3:9090"
```

## Flags

### Core

| Flag | Description | Default |
|------|-------------|---------|
| `-dsn` | Storage DSN | `mem://?tenant=default` |
| `-http` | HTTP listen address | `:8080` |
| `-grpc` | gRPC listen address (disabled if empty) | — |
| `-auth` | Bearer token for all requests | — |
| `-tenant` | Default tenant name | `default` |
| `-peers` | Comma-separated `host:grpcPort` peers for federation | — |
| `-v` | Verbose logging | `false` |

### TLS

| Flag | Description |
|------|-------------|
| `-tls-min-version` | Minimum TLS version (`1.2` or `1.3`) |
| `-http-tls-cert` / `-http-tls-key` | Certificate/key for HTTP TLS |
| `-grpc-tls-cert` / `-grpc-tls-key` | Certificate/key for gRPC TLS |
| `-peer-tls` | Enable TLS when connecting to peers |
| `-peer-tls-ca` | CA certificate for peer TLS |
| `-peer-tls-server-name` | Override server name for peer TLS verification |
| `-peer-tls-skip-verify` | Disable peer TLS certificate verification |

### Limits

| Flag | Default | Description |
|------|---------|-------------|
| `-max-body-bytes` | `1048576` | Maximum HTTP request body size |
| `-max-sql-bytes` | `65536` | Maximum SQL statement size |
| `-grpc-max-recv-bytes` | `4194304` | gRPC max receive message size |
| `-grpc-max-send-bytes` | `4194304` | gRPC max send message size |

### Timeouts

| Flag | Default | Description |
|------|---------|-------------|
| `-request-timeout` | `30s` | Per-request execution timeout |
| `-peer-timeout` | `5s` | Timeout for federated peer calls |
| `-shutdown-timeout` | `10s` | Graceful shutdown deadline |

### HTTP hardening

| Flag | Default | Description |
|------|---------|-------------|
| `-trusted-proxies` | — | Comma-separated CIDR ranges of trusted proxies |
| `-http-read-timeout` | `15s` | HTTP server read timeout |
| `-http-read-header-timeout` | `5s` | HTTP header read timeout |
| `-http-write-timeout` | `30s` | HTTP server write timeout |
| `-http-idle-timeout` | `60s` | HTTP keep-alive idle timeout |
| `-http-max-header-bytes` | `8192` | Maximum HTTP header size |

## HTTP API

All request and response bodies are JSON.

### `POST /api/exec`

Execute a DML/DDL statement (no result rows returned).

```json
{ "tenant": "default", "sql": "CREATE TABLE t (id INT, name TEXT)" }
```

Response: `{ "rows_affected": 0, "elapsed_ms": 1 }`

### `POST /api/query`

Execute a SELECT and return rows.

```json
{ "tenant": "default", "sql": "SELECT * FROM t" }
```

Optional request-level timeout override:

```json
{ "tenant": "default", "sql": "SELECT * FROM t", "timeout_ms": 5000 }
```

Response:

```json
{
  "columns": ["id", "name"],
  "rows": [[1, "Alice"]],
  "elapsed_ms": 2
}
```

### `GET /api/status`

Returns server version, uptime, and tenant list.

### `GET /api/cluster/status`

Returns cluster health information for configured federation peers, including
per-peer reachability and response duration.

### `POST /api/federated/query`

Fan-out a read query to all configured peers and merge results.
Supports optional `timeout_ms` and `peer_timeout_ms` overrides in the request
body.

### `GET /healthz` / `GET /readyz`

Liveness and readiness probes (return `200 OK` when healthy).

### `GET /metrics`

Prometheus-compatible metrics endpoint.

## Load testing

A built-in load generator lives in [`loadtest/`](loadtest/):

```bash
go build -o bin/tinysql-loadtest ./cmd/server/loadtest

./bin/tinysql-loadtest \
  -url http://127.0.0.1:8080/api/query \
  -tenant default \
  -sql "SELECT 1" \
  -requests 10000 \
  -concurrency 100
```

See [loadtest/README.md](loadtest/README.md) for full options.

## Authentication

Pass the bearer token in the `Authorization` header:

```bash
curl -H "Authorization: Bearer my-secret-token" \
     -d '{"tenant":"default","sql":"SELECT 1"}' \
     http://localhost:8080/api/query
```
