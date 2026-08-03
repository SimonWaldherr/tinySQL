# tinySQL server loadtest

Companion to [`cmd/server`](../README.md) in [TinySQL](../../../README.md).

`cmd/server` query load tester. `go build -o tinysql-loadtest .`

| Flag | Default | Description |
|---|---|---|
| `-url` | `http://127.0.0.1:8080/api/query` | Endpoint |
| `-auth` | — | Bearer token (omit if server has none) |
| `-tenant` | `default` | Tenant |
| `-sql` | `SELECT 1` | Query |
| `-requests` | `1000` | Requests |
| `-concurrency` | `20` | Workers |
| `-timeout` | `5s` | Client timeout |

Reports duration, RPS, errors, latency (avg, p50/p95/p99, max).
