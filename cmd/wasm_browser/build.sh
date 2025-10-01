#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Copy wasm_exec.js"
mkdir -p web
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/

echo "Build WASM module"
GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm .

echo "Done. Starting web server on http://localhost:8080"
cd web
python3 -m http.server 8080
