#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Copy wasm_exec.js"
mkdir -p web
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/

echo "Build WASM module"
GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm .

echo "Done. Serve ./web to test (e.g., python3 -m http.server 8080)"
