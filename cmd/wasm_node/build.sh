#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd -P)"
cd "$SCRIPT_DIR"

RUN_DEMO=false
SKIP_BUILD=false
QUERY="SELECT 1"
WASM_OUT="tinySQL.wasm"
# Set WASM_COMPILER=tinygo to prioritize a substantially smaller artifact.
# The default remains Go so existing builds keep their current toolchain.
WASM_COMPILER="${WASM_COMPILER:-go}"
# Minimal is the embedding default; full restores HTTP, templates and YAML.
WASM_PROFILE="${WASM_PROFILE:-minimal}"
case "$WASM_PROFILE" in
    minimal) WASM_TAGS="tinysql_minimal" ;;
    full) WASM_TAGS="" ;;
    *) echo "unsupported WASM_PROFILE=$WASM_PROFILE (expected minimal or full)" >&2; exit 2 ;;
esac

usage() {
    cat <<'EOF'
Usage (WASM_PROFILE=minimal by default; set full for optional features):
  ./build.sh                           Build tinySQL Node WASM assets
  ./build.sh --run                     Build and run demo query
  ./build.sh --run --query "SELECT 42"
                                       Build and run custom query
  ./build.sh --skip-build --run        Run demo without rebuilding
  WASM_COMPILER=tinygo ./build.sh      Build with TinyGo for a smaller artifact
EOF
}

filesize() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0; }
human() { numfmt --to=iec-i --suffix=B "$1" 2>/dev/null || echo "$1 bytes"; }

optimise_wasm() {
    local best_size variant temp size
    local optimizer_succeeded=false
    if [[ "${WASM_OPTIMIZE:-true}" != "true" ]]; then
        echo "Skipping WASM optimisation (WASM_OPTIMIZE=${WASM_OPTIMIZE})"
        return
    fi
    if ! command -v wasm-opt >/dev/null 2>&1; then
        echo "Tip: install Binaryen (wasm-opt) for additional WASM size optimisation"
        return
    fi
    best_size="$(filesize "$WASM_OUT")"
    # Go 1.27 emits saturating float-to-int instructions; Binaryen must
    # allow that feature or validation rejects every optimization variant.
    for variant in \
        "--enable-bulk-memory --enable-nontrapping-float-to-int -Oz --strip-debug" \
        "--enable-bulk-memory --enable-nontrapping-float-to-int -Oz --strip-debug --converge"; do
        temp="${WASM_OUT}.opt.tmp"
        # shellcheck disable=SC2086
        if wasm-opt $variant -o "$temp" "$WASM_OUT" 2>/dev/null; then
            optimizer_succeeded=true
            size="$(filesize "$temp")"
            if [[ "$size" -gt 0 && "$size" -lt "$best_size" ]]; then
                echo "  wasm-opt $variant: saved $((best_size - size)) bytes"
                mv -f "$temp" "$WASM_OUT"
                best_size="$size"
            else
                rm -f "$temp"
            fi
        else
            rm -f "$temp"
        fi
    done
    if [[ "$optimizer_succeeded" == false ]]; then
        echo "Warning: wasm-opt rejected all variants; keeping the unoptimized module" >&2
    fi
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

build_wasm() {
    case "$WASM_COMPILER" in
        go)
            local wasm_exec_path
            wasm_exec_path="$(find_wasm_exec || true)"
            if [[ -z "$wasm_exec_path" ]]; then
                echo "wasm_exec.js not found in GOROOT ($(go env GOROOT))" >&2
                exit 1
            fi
            cp "$wasm_exec_path" wasm_exec.js
            # shellcheck disable=SC2086
            GOOS=js GOARCH=wasm go build ${GOFLAGS:-} -tags="$WASM_TAGS" -trimpath -buildvcs=false -ldflags "-s -w" -o "$WASM_OUT" .
            ;;
        tinygo)
            if ! command -v tinygo >/dev/null 2>&1; then
                echo "tinygo not found; install TinyGo or use WASM_COMPILER=go" >&2
                exit 1
            fi
            cp "$(tinygo env TINYGOROOT)/targets/wasm_exec.js" wasm_exec.js
            tinygo build -tags="$WASM_TAGS" -target=wasm -no-debug -o "$WASM_OUT" .
            ;;
        *)
            echo "unsupported WASM_COMPILER=$WASM_COMPILER (expected go or tinygo)" >&2
            exit 2
            ;;
    esac
}

if [[ "$WASM_COMPILER" == "go" ]] && ! command -v go >/dev/null 2>&1; then
    echo "go toolchain not found" >&2
    exit 1
fi

if [[ "$SKIP_BUILD" == false ]]; then
    echo "Building WASM module with $WASM_COMPILER ($WASM_PROFILE profile)"
    build_wasm
    optimise_wasm
fi

echo "Done."
if [[ -f "$WASM_OUT" ]]; then
    printf "  %-20s %s\n" "$WASM_OUT" "$(human "$(filesize "$WASM_OUT")")"
fi
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
