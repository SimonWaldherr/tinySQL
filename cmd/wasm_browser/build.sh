#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd -P)"
cd "$SCRIPT_DIR"

PORT="${PORT:-8080}"
SERVE=false
SKIP_BUILD=false
WASM_OUT="web/tinySQL.wasm"
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
  ./build.sh                 Build tinySQL browser WASM assets
  ./build.sh --serve         Build and start a local web server
  ./build.sh --build-only    Build only (explicit)
  ./build.sh --skip-build --serve
                             Serve existing assets without rebuilding
  WASM_COMPILER=tinygo ./build.sh --build-only
                             Build with TinyGo for a smaller artifact
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

build_wasm() {
    case "$WASM_COMPILER" in
        go)
            local wasm_exec_path
            wasm_exec_path="$(find_wasm_exec || true)"
            if [[ -z "$wasm_exec_path" ]]; then
                echo "wasm_exec.js not found in GOROOT ($(go env GOROOT))" >&2
                exit 1
            fi
            cp "$wasm_exec_path" web/wasm_exec.js
            # shellcheck disable=SC2086
            GOOS=js GOARCH=wasm go build ${GOFLAGS:-} -tags="$WASM_TAGS" -trimpath -buildvcs=false -ldflags "-s -w" -o "$WASM_OUT" .
            ;;
        tinygo)
            if ! command -v tinygo >/dev/null 2>&1; then
                echo "tinygo not found; install TinyGo or use WASM_COMPILER=go" >&2
                exit 1
            fi
            cp "$(tinygo env TINYGOROOT)/targets/wasm_exec.js" web/wasm_exec.js
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
    mkdir -p web
    build_wasm
    optimise_wasm
    if command -v gzip >/dev/null 2>&1; then
        gzip -9 -n -c "$WASM_OUT" > "${WASM_OUT}.gz" 2>/dev/null || true
    fi
fi

echo "Done."
if [[ -f "$WASM_OUT" ]]; then
    printf "  %-20s %s\n" "$(basename "$WASM_OUT")" "$(human "$(filesize "$WASM_OUT")")"
fi
if [[ -f "${WASM_OUT}.gz" ]]; then
    printf "  %-20s %s\n" "$(basename "${WASM_OUT}.gz")" "$(human "$(filesize "${WASM_OUT}.gz")")"
fi
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
