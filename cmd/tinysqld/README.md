# tinySQL DBMS Daemon (`tinysqld`)

`tinysqld` is the future enterprise DBMS entry point. It is intentionally
separate from the existing `cmd/server` command so current HTTP/gRPC server
behavior remains compatible while the DBMS runtime grows.

## Current Scope

- Opens the `OpenEnterprise` product profile.
- Requires durable storage.
- Starts the job scheduler through the enterprise profile.
- Exposes a minimal HTTP DBMS API.
- Waits for a shutdown signal and shuts down gracefully.

Use `cmd/server` for the older HTTP/gRPC API server while `tinysqld` grows into
the enterprise DBMS entry point.

## Build

```bash
go build ./cmd/tinysqld
```

## Run

```bash
./tinysqld -data ./tinysqld-data -storage disk -tenant default -http 127.0.0.1:8088
```

## Check Configuration

```bash
./tinysqld -data ./tinysqld-data -storage disk -check
```

## Flags

- `-data`: durable database path or directory.
- `-storage`: storage mode; one of `disk`, `hybrid`, `index`, `wal`, `advanced_wal`.
- `-tenant`: default tenant, `default` if omitted.
- `-http`: HTTP listen address, `127.0.0.1:8088` by default. Empty disables HTTP.
- `-auth`: optional bearer token for API endpoints.
- `-request-timeout`: maximum SQL request duration.
- `-http-read-timeout`: HTTP read timeout.
- `-http-write-timeout`: HTTP write timeout.
- `-shutdown-timeout`: graceful shutdown timeout.
- `-check`: open the runtime, print status, then exit.

## HTTP API

Unauthenticated:

- `GET /healthz`
- `GET /readyz`

Authenticated when `-auth` is set:

- `GET /api/status`
- `POST /api/exec`
- `POST /api/query`

SQL request body:

```json
{
  "tenant": "default",
  "sql": "SELECT name FROM users",
  "timeout_ms": 5000
}
```

Auth accepts either:

```text
Authorization: Bearer <token>
X-TinySQL-Auth: <token>
```
