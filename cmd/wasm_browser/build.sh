#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${PORT:-8080}"
SERVE=false
SKIP_BUILD=false

usage() {
    cat <<'EOF'
Usage:
  ./build.sh                 Build tinySQL browser WASM assets
  ./build.sh --serve         Build and start a local web server
  ./build.sh --build-only    Build only (explicit)
  ./build.sh --skip-build --serve
                             Serve existing assets without rebuilding
EOF
}

for arg in "$@"; do
    case "$arg" in
        --serve|-s)
            SERVE=true
            ;;
        --build-only|-b)
            SERVE=false
            ;;
        --skip-build)
            SKIP_BUILD=true
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown flag: $arg" >&2
            usage >&2
            exit 2
            ;;
    esac
done

find_wasm_exec() {
    local goroot
    goroot="$(go env GOROOT)"
    local candidates=(
        "$goroot/lib/wasm/wasm_exec.js"
        "$goroot/misc/wasm/wasm_exec.js"
    )
    local path
    for path in "${candidates[@]}"; do
        if [[ -f "$path" ]]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

if ! command -v go >/dev/null 2>&1; then
    echo "go toolchain not found" >&2
    exit 1
fi

if [[ "$SKIP_BUILD" == false ]]; then
    echo "Building WASM module"
    mkdir -p web
    WASM_EXEC_PATH="$(find_wasm_exec || true)"
    if [[ -z "$WASM_EXEC_PATH" ]]; then
        echo "wasm_exec.js not found in GOROOT ($(go env GOROOT))" >&2
        exit 1
    fi
    cp "$WASM_EXEC_PATH" web/wasm_exec.js
    GOOS=js GOARCH=wasm go build -trimpath -ldflags "-s -w" -o web/tinySQL.wasm .
fi

echo "Done."
if [[ "$SERVE" == true ]]; then
    if ! command -v python3 >/dev/null 2>&1; then
        echo "python3 not found (required for --serve)" >&2
        exit 1
    fi
    echo "Starting web server on http://localhost:${PORT}"
    cd web
    python3 -m http.server "$PORT"
else
    echo "Assets ready in: $SCRIPT_DIR/web"
fi
