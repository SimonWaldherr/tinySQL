#!/bin/bash
# build.sh – compile the TinySQL WASM demo and optionally serve it locally.
#
# Usage:
#   ./build.sh            Build only
#   ./build.sh --serve    Build, then start a local HTTP server on port 8080
#   ./build.sh --clean    Remove generated artefacts and exit
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${PORT:-8080}"
WASM_OUT="query_files.wasm"

# ── helpers ──────────────────────────────────────────────────────────────────
filesize() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0; }
human()    { numfmt --to=iec-i --suffix=B "$1" 2>/dev/null || echo "$1 bytes"; }
elapsed()  { echo "$(( $(date +%s) - $1 ))s"; }

# ── flags ────────────────────────────────────────────────────────────────────
SERVE=false
CLEAN=false
for arg in "$@"; do
    case "$arg" in
        --serve|-s)  SERVE=true ;;
        --clean|-c)  CLEAN=true ;;
        --help|-h)
            sed -n '2,8s/^# //p' "$0"
            exit 0 ;;
    esac
done

if $CLEAN; then
    echo "🧹 Cleaning generated files…"
    rm -f "$WASM_OUT" "${WASM_OUT}.gz" wasm_exec.js
    echo "   Done."
    exit 0
fi

# ── pre-flight checks ───────────────────────────────────────────────────────
echo "🔨 Building TinySQL Query Files WASM…"

if ! command -v go >/dev/null 2>&1; then
    echo "❌ Go toolchain not found. Install Go from https://go.dev/dl/"
    exit 1
fi

GO_VERSION="$(go version)"
echo "   Go: $GO_VERSION"

# ── compile ──────────────────────────────────────────────────────────────────
T0=$(date +%s)
echo "📦 Compiling Go → WASM (stripping debug info)…"
GOOS=js GOARCH=wasm go build -trimpath -ldflags "-s -w" -o "$WASM_OUT"
RAW_SIZE=$(filesize "$WASM_OUT")
echo "   Compiled in $(elapsed $T0)  –  raw size: $(human "$RAW_SIZE")"

# ── copy wasm_exec.js ────────────────────────────────────────────────────────
echo "📋 Copying wasm_exec.js…"
WASM_EXEC="$(go env GOROOT)/misc/wasm/wasm_exec.js"
if [ ! -f "$WASM_EXEC" ]; then
    WASM_EXEC=$(find "$(go env GOROOT)" -name "wasm_exec.js" 2>/dev/null | head -1)
fi
if [ -z "$WASM_EXEC" ] || [ ! -f "$WASM_EXEC" ]; then
    echo "❌ Could not find wasm_exec.js in Go installation (GOROOT=$(go env GOROOT))"
    exit 1
fi
cp "$WASM_EXEC" .
echo "   Copied from $WASM_EXEC"

# ── optional wasm-opt / wasm-strip ──────────────────────────────────────────
if command -v wasm-opt >/dev/null 2>&1; then
    echo "🔧 Optimising with wasm-opt (trying multiple strategies)…"

    VARIANTS=(
        "--enable-bulk-memory -Oz --strip-debug"
        "--enable-bulk-memory -Oz --strip-debug --converge"
        "--enable-bulk-memory -O3 --strip-debug --converge"
    )

    BEST_SIZE=$(filesize "$WASM_OUT")

    for v in "${VARIANTS[@]}"; do
        TMP_OUT="${WASM_OUT}.opt.tmp"
        # shellcheck disable=SC2086
        if wasm-opt $v -o "$TMP_OUT" "$WASM_OUT" 2>/dev/null; then
            sz=$(filesize "$TMP_OUT")
            if [ "$sz" -gt 0 ] && [ "$sz" -lt "$BEST_SIZE" ]; then
                echo "   ✅ $v → $(human "$sz") (saved $(( BEST_SIZE - sz )) bytes)"
                mv -f "$TMP_OUT" "$WASM_OUT"
                BEST_SIZE=$sz
            else
                rm -f "$TMP_OUT"
            fi
        else
            rm -f "$TMP_OUT"
        fi
    done

    echo "   Final optimised size: $(human "$BEST_SIZE")"
elif command -v wasm-strip >/dev/null 2>&1; then
    echo "🔧 Stripping debug sections with wasm-strip…"
    wasm-strip "$WASM_OUT" || true
else
    echo "ℹ️  Tip: install Binaryen (wasm-opt) for further size reduction"
fi

# ── gzip pre-compress ───────────────────────────────────────────────────────
if command -v gzip >/dev/null 2>&1; then
    gzip -9 -c "$WASM_OUT" > "${WASM_OUT}.gz" 2>/dev/null || true
fi

# ── summary ──────────────────────────────────────────────────────────────────
echo ""
echo "📂 Generated files:"
printf "   %-24s %s\n" "$WASM_OUT" "$(human "$(filesize "$WASM_OUT")")"
if [ -f "${WASM_OUT}.gz" ]; then
    printf "   %-24s %s  (pre-compressed)\n" "${WASM_OUT}.gz" "$(human "$(filesize "${WASM_OUT}.gz")")"
fi
printf "   %-24s %s\n" "wasm_exec.js" "$(human "$(filesize wasm_exec.js)")"
echo ""
echo "✅ Build finished in $(elapsed $T0)."

# ── optional local server ───────────────────────────────────────────────────
if $SERVE; then
    echo ""
    echo "🚀 Starting local server on http://localhost:${PORT} …"
    if command -v python3 >/dev/null 2>&1; then
        python3 -m http.server "$PORT"
    elif command -v php >/dev/null 2>&1; then
        php -S "localhost:${PORT}"
    else
        echo "❌ Neither python3 nor php found – please install one to serve locally."
        exit 1
    fi
else
    echo "🚀 To test locally:"
    echo "   ./build.sh --serve"
    echo "   # or: python3 -m http.server $PORT"
    echo "   Then open: http://localhost:${PORT}"
fi
