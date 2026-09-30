#!/usr/bin/env bash
# Validate embedding profiles without overwriting the demo's generated assets.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
command -v node >/dev/null || { echo "node is required for WASM integration checks" >&2; exit 1; }
wasm_tmp="$(mktemp -d "${TMPDIR:-/tmp}/tinysql-wasm-smoke.XXXXXX")"
trap 'rm -rf "$wasm_tmp"' EXIT
wasm_exec="$(go env GOROOT)/lib/wasm/wasm_exec.js"
if [[ ! -f "$wasm_exec" ]]; then
    wasm_exec="$(go env GOROOT)/misc/wasm/wasm_exec.js"
fi
cp "$wasm_exec" "$wasm_tmp/wasm_exec.js"
for target in wasm_browser wasm_node; do
    deps="$(GOOS=js GOARCH=wasm go list -tags=tinysql_minimal -deps -f '{{.ImportPath}}' "./cmd/$target")"
    if forbidden="$(printf '%s\n' "$deps" | grep -E '^(net/http|crypto/tls|html/template|gopkg.in/yaml.v3|github.com/SimonWaldherr/tinySQL/(internal/)?driver)$')"; then
        echo "Minimal $target retains excluded dependencies: $forbidden" >&2
        exit 1
    fi
    for profile in full minimal; do
        tags=""
        if [[ "$profile" == minimal ]]; then tags=tinysql_minimal; fi
        output="$wasm_tmp/$target-$profile.wasm"
        GOOS=js GOARCH=wasm go build -tags="$tags" -trimpath -buildvcs=false -ldflags='-s -w' -o "$output" "./cmd/$target"
        node scripts/wasm-smoke.js "$output" "$wasm_tmp/wasm_exec.js" "$profile"
    done
    full_size="$(wc -c < "$wasm_tmp/$target-full.wasm")"
    minimal_size="$(wc -c < "$wasm_tmp/$target-minimal.wasm")"
    if (( minimal_size >= full_size )); then
        echo "Minimal $target must be smaller than full ($minimal_size >= $full_size)" >&2
        exit 1
    fi
    echo "$target: full=$full_size bytes; minimal=$minimal_size bytes; saved=$((full_size - minimal_size)) bytes"
done
