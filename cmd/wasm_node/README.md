# tinySQL WASM Node.js (`wasm_node`)

Part of [TinySQL](../../README.md). See the [command index](../README.md) for
browser and server variants.

Compiles tinySQL to WebAssembly for Node.js. `wasm_runner.js` bootstraps the
module and exposes the engine to Node scripts and the command line.

## Build

```bash
cd cmd/wasm_node

# Build only (produces tinySQL.wasm + wasm_exec.js)
./build.sh --build-only

# Build and run the built-in demo
./build.sh --run
./build.sh --run --query "SELECT 42"

# Run existing assets without rebuilding
./build.sh --skip-build --run
```

Manual equivalent:

```bash
cd cmd/wasm_node
GOOS=js GOARCH=wasm go build -o tinySQL.wasm .
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./
```

## Run

```bash
node wasm_runner.js                                             # status (default)
node wasm_runner.js query "SELECT 1 AS n, 'hello' AS greeting"
node wasm_runner.js exec "CREATE TABLE t (id INT)"
```

The runner starts the Go runtime asynchronously and waits until the global
`tinySQL` API is ready, then opens an in-memory database, runs the command, and
closes it again.

## Embed in your own Node script

Use `wasm_runner.js` as the bootstrap reference: instantiate `tinySQL.wasm`, call
`go.run(instance)` without awaiting its never-ending runtime promise, and wait
for `global.tinySQL`. The runner is a CLI helper, not a CommonJS module.

## Notes

- Requires Node.js 18+ (WebAssembly support).
- The database is in-memory and does not persist between Node process runs.
- Builds use stripped symbols and optionally `wasm-opt` when Binaryen is
  installed.
- For the browser variant see [`../wasm_browser/`](../wasm_browser/).
