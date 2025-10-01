#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Copy wasm_exec.js"
cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" .

echo "Build WASM module"
GOOS=js GOARCH=wasm go build -o tinySQL.wasm .

echo "Done. Running demo query with Node.js"
node wasm_runner.js query "SELECT 1"
