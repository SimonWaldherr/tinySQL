# tinySQL WASM (Browser)

This command builds the tinySQL WebAssembly module for browsers and serves a simple web UI.

Usage:
- Build the WASM: GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm .
- Copy the Go WASM runtime: cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/
- Serve the web folder: cd web && python3 -m http.server 8080