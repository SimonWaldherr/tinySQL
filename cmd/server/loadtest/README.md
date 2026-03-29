# tinySQL Server Loadtest

Small built-in load generator for the `cmd/server` HTTP query endpoint.

## Build

```bash
go build -o bin/tinysql-loadtest ./cmd/server/loadtest
```

## Run

```bash
./bin/tinysql-loadtest \
  -url http://127.0.0.1:8080/api/query \
  -tenant default \
  -sql "SELECT 1" \
  -requests 10000 \
  -concurrency 100
```

Optional auth:

```bash
./bin/tinysql-loadtest -auth "$TOKEN" ...
```

The tool prints:

- total duration
- achieved RPS
- transport vs HTTP errors
- latency: average, p50, p95, p99, max
