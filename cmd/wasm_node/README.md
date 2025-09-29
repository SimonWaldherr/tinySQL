# tinySQL WASM (Node.js)

This command builds the same WebAssembly module and provides a Node.js runner.

Build:
- GOOS=js GOARCH=wasm go build -o tinySQL.wasm .
- Copy wasm_exec.js: cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./

Run:
- node wasm_runner.js
- node wasm_runner.js query "SELECT 1"