# tinySQL WASM Node.js (`wasm_node`)

Compiles tinySQL to **WebAssembly** for use in Node.js. A small
`wasm_runner.js` script bootstraps the WASM module and exposes the tinySQL
engine to Node.js scripts and the command line.

## Build

### All-in-one script

```bash
cd cmd/wasm_node

# Build only (produces tinySQL.wasm + wasm_exec.js)
./build.sh --build-only

# Build and run the built-in demo
./build.sh --run

# Run existing assets without rebuilding
./build.sh --skip-build --run
```

### Manual build

```bash
cd cmd/wasm_node
GOOS=js GOARCH=wasm go build -o tinySQL.wasm .
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./
```

## Run

```bash
# Default status command
node wasm_runner.js

# One-shot query
node wasm_runner.js query "SELECT 1 AS n, 'hello' AS greeting"

# Execute one statement
node wasm_runner.js exec "CREATE TABLE t (id INT)"
```

The runner starts the Go runtime asynchronously and waits until the global
`tinySQL` API is ready; it then opens an in-memory database, runs the selected
command, and closes it again. `status` is the default command.

## Embed in your own Node script

Use `wasm_runner.js` as the bootstrap reference: instantiate `tinySQL.wasm`,
call `go.run(instance)` without awaiting its never-ending runtime promise, and
wait for `global.tinySQL`. The runner is a CLI helper, not a CommonJS module.

## Notes

- Requires Node.js 18+ (WebAssembly support).
- The database is **in-memory** and does not persist between Node.js process
  runs.
- Builds use stripped symbols and optionally `wasm-opt` when Binaryen is
  installed.
- For the browser variant see [`../wasm_browser/`](../wasm_browser/).
