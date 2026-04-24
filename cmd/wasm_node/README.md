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
```

### Manual build

```bash
cd cmd/wasm_node
GOOS=js GOARCH=wasm go build -o tinySQL.wasm .
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./
```

## Run

```bash
# Interactive mode — reads SQL from stdin
node wasm_runner.js

# One-shot query
node wasm_runner.js query "SELECT 1 AS n, 'hello' AS greeting"

# Multiple statements
node wasm_runner.js query "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t"
```

## Use in Node scripts

```js
const { loadTinySQL } = require('./wasm_runner');

async function main() {
    const db = await loadTinySQL();
    db.exec("CREATE TABLE users (id INT, name TEXT)");
    db.exec("INSERT INTO users VALUES (1, 'Alice')");
    const result = db.query("SELECT * FROM users");
    console.log(result);
}
main();
```

## Notes

- Requires Node.js 18+ (WebAssembly support).
- The database is **in-memory** and does not persist between Node.js process
  runs.
- For the browser variant see [`../wasm_browser/`](../wasm_browser/).