# tinySQL Command Variants

This repo ships multiple binaries under `cmd/` for different use-cases.

- demo
  - A simple demo that creates tables, inserts sample data, and runs example queries.
  - Build: `go build ./cmd/demo`
  - Run: `./demo -dsn "mem://?tenant=default"`

- repl
  - Interactive SQL REPL on top of database/sql.
  - Build: `go build ./cmd/repl`
  - Run: `./repl -dsn "mem://?tenant=default"`

- server
  - HTTP JSON API and gRPC (JSON codec) server with optional federation across peers.
  - Build: `go build ./cmd/server`
  - Run: `./server -http :8080 -grpc :9090 -dsn "mem://?tenant=default" -peers "host1:9090,host2:9090"`
  - HTTP Endpoints:
    - POST /api/exec {tenant, sql}
    - POST /api/query {tenant, sql}
    - GET  /api/status
    - POST /api/federated/query {tenant, sql}

- wasm_browser
  - Builds tinySQL to WebAssembly for browsers. A modern UI is provided in `web/`.
  - Build: `cd cmd/wasm_browser && ./build.sh`
  - Serve: `cd web && python3 -m http.server 8080` then open http://localhost:8080

- wasm_node
  - Builds tinySQL to WebAssembly for Node.js and provides a Node runner.
  - Build: `cd cmd/wasm_node && GOOS=js GOARCH=wasm go build -o tinySQL.wasm . && cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./`
  - Run: `node wasm_runner.js` or `node wasm_runner.js query "SELECT 1"`

Notes:
- The in-memory DSN is `mem://?tenant=default`. Files can be persisted using the storage APIs and driver settings.
- The server and REPL both rely on the internal driver registration (`_ ".../internal/driver"`).
