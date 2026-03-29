#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RUN_DEMO=false
SKIP_BUILD=false
QUERY="SELECT 1"

usage() {
    cat <<'EOF'
Usage:
  ./build.sh                           Build tinySQL Node WASM assets
  ./build.sh --run                     Build and run demo query
  ./build.sh --run --query "SELECT 42"
                                       Build and run custom query
  ./build.sh --skip-build --run        Run demo without rebuilding
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run|-r)
            RUN_DEMO=true
            shift
            ;;
        --build-only|-b)
            RUN_DEMO=false
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --query|-q)
            if [[ $# -lt 2 ]]; then
                echo "missing value for $1" >&2
                exit 2
            fi
            QUERY="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown flag: $1" >&2
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
    WASM_EXEC_PATH="$(find_wasm_exec || true)"
    if [[ -z "$WASM_EXEC_PATH" ]]; then
        echo "wasm_exec.js not found in GOROOT ($(go env GOROOT))" >&2
        exit 1
    fi
    cp "$WASM_EXEC_PATH" wasm_exec.js
    GOOS=js GOARCH=wasm go build -trimpath -ldflags "-s -w" -o tinySQL.wasm .
fi

echo "Done."
if [[ "$RUN_DEMO" == true ]]; then
    if ! command -v node >/dev/null 2>&1; then
        echo "node not found (required for --run)" >&2
        exit 1
    fi
    echo "Running demo query: $QUERY"
    node wasm_runner.js query "$QUERY"
else
    echo "Assets ready in: $SCRIPT_DIR"
fi
