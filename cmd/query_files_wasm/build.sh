#!/usr/bin/env bash
# build.sh – compile the TinySQL WASM demo and optionally serve it locally.
#
# Usage:
#   ./build.sh            Build only
#   ./build.sh --serve    Build, then start a local HTTP server on port 8080
#   ./build.sh --skip-build --serve
#                         Serve existing artefacts without rebuilding
#   ./build.sh --if-needed Build only when inputs or the Go WASM runtime changed
#   ./build.sh --clean    Remove generated artefacts and exit
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd -P)"
cd "$SCRIPT_DIR"

PORT="${PORT:-8080}"
WASM_OUT="query_files.wasm"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null && pwd -P)"

# ── helpers ──────────────────────────────────────────────────────────────────
filesize() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0; }
human()    { numfmt --to=iec-i --suffix=B "$1" 2>/dev/null || echo "$1 bytes"; }
elapsed()  { echo "$(( $(date +%s) - $1 ))s"; }
validate_wasm() {
    local wasm_file="$1"
    if command -v node >/dev/null 2>&1; then
        node -e '
            const fs = require("fs");
            const file = process.argv[1];
            if (!WebAssembly.validate(fs.readFileSync(file))) {
                console.error(`WebAssembly.validate failed for ${file}`);
                process.exit(1);
            }
        ' "$wasm_file"
    else
        # wasm-opt validates while reading/writing even when no optimization
        # pass is requested.
        wasm-opt --all-features "$wasm_file" -o /dev/null
    fi
}

# ── flags ────────────────────────────────────────────────────────────────────
SERVE=false
CLEAN=false
SKIP_BUILD=false
IF_NEEDED=false
for arg in "$@"; do
    case "$arg" in
        --serve|-s)  SERVE=true ;;
        --build-only|-b) SERVE=false ;;
        --skip-build) SKIP_BUILD=true ;;
        --if-needed) IF_NEEDED=true ;;
        --clean|-c)  CLEAN=true ;;
        --help|-h)
            sed -n '2,10s/^# //p' "$SCRIPT_DIR/build.sh"
            exit 0 ;;
        *)
            echo "Unknown flag: $arg"
            exit 2 ;;
    esac
done

wasm_exec_source() {
    local goroot_path
    goroot_path="$(go env GOROOT)"
    for candidate in \
        "${goroot_path}/lib/wasm/wasm_exec.js" \
        "${goroot_path}/misc/wasm/wasm_exec.js"; do
        if [ -f "$candidate" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

artifacts_are_current() {
    [ -s "$WASM_OUT" ] && [ -s "wasm_exec.js" ] || return 1
    if command -v gzip >/dev/null 2>&1 && [ ! -s "${WASM_OUT}.gz" ]; then
        return 1
    fi
    if find "$REPO_ROOT" \
        -path "$REPO_ROOT/.git" -prune -o \
        -path "$REPO_ROOT/cmd" -prune -o \
        -type f -name '*.go' -newer "$WASM_OUT" -print -quit | grep -q .; then
        return 1
    fi
    for source in "$REPO_ROOT/go.mod" "$REPO_ROOT/go.sum"; do
        [ "$source" -nt "$WASM_OUT" ] && return 1
    done
    if find "$SCRIPT_DIR" -maxdepth 1 -type f \
        \( -name '*.go' -o -name 'go.mod' -o -name 'go.sum' -o -name 'build.sh' \) \
        -newer "$WASM_OUT" -print -quit | grep -q .; then
        return 1
    fi
    local runtime_source
    runtime_source="$(wasm_exec_source)" || return 1
    [ "$runtime_source" -nt "wasm_exec.js" ] && return 1
    return 0
}

if $CLEAN; then
    echo "🧹 Cleaning generated files…"
    rm -f "$WASM_OUT" "${WASM_OUT}.gz" wasm_exec.js
    echo "   Done."
    exit 0
fi

# ── pre-flight checks ───────────────────────────────────────────────────────
if ! $SKIP_BUILD && ! command -v go >/dev/null 2>&1; then
    echo "❌ Go toolchain not found. Install Go from https://go.dev/dl/"
    exit 1
fi

if $IF_NEEDED && ! $SKIP_BUILD && artifacts_are_current; then
    SKIP_BUILD=true
    echo "♻️  WASM artifacts are current; skipping rebuild."
fi

if ! $SKIP_BUILD; then
    echo "🔨 Building TinySQL Query Files WASM…"
else
    echo "⏭️  Skipping build; using existing WASM artefacts…"
fi

if ! $SKIP_BUILD; then
    GO_VERSION="$(go version)"
    echo "   Go: $GO_VERSION"
    if [ -n "${GOFLAGS:-}" ]; then
        echo "   GOFLAGS: ${GOFLAGS}"
    fi
fi

T0=$(date +%s)
if ! $SKIP_BUILD; then
    # ── compile ──────────────────────────────────────────────────────────────
    echo "📦 Compiling Go → WASM (stripping debug info)…"
    # shellcheck disable=SC2086
    GOOS=js GOARCH=wasm go build ${GOFLAGS:-} -trimpath -buildvcs=false -ldflags "-s -w" -o "$WASM_OUT" .
    RAW_SIZE=$(filesize "$WASM_OUT")
    echo "   Compiled in $(elapsed $T0)  –  raw size: $(human "$RAW_SIZE")"

    # ── copy wasm_exec.js ────────────────────────────────────────────────────
    echo "📋 Copying wasm_exec.js…"
    WASM_EXEC="$(wasm_exec_source || true)"
    if [ -z "$WASM_EXEC" ]; then
        echo "❌ Could not find wasm_exec.js in Go installation (GOROOT=$(go env GOROOT))"
        exit 1
    fi
    cp "$WASM_EXEC" .
    echo "   Copied from $WASM_EXEC"

    # ── optional wasm-opt / wasm-strip ──────────────────────────────────────
    if command -v wasm-opt >/dev/null 2>&1; then
        echo "🔧 Optimising with wasm-opt -Oz (single release pass)…"

        ORIGINAL_SIZE=$(filesize "$WASM_OUT")
        TMP_OUT="${WASM_OUT}.opt.tmp"
        # Go 1.26 emits bulk-memory and non-trapping float-to-int operations.
        # --all-features lets Binaryen validate those instructions; the former
        # --enable-bulk-memory-only invocation silently rejected the module.
        if wasm-opt --all-features -Oz --strip-debug -o "$TMP_OUT" "$WASM_OUT"; then
            OPTIMIZED_SIZE=$(filesize "$TMP_OUT")
            if [ "$OPTIMIZED_SIZE" -gt 0 ] &&
               [ "$OPTIMIZED_SIZE" -lt "$ORIGINAL_SIZE" ] &&
               validate_wasm "$TMP_OUT"; then
                mv -f "$TMP_OUT" "$WASM_OUT"
                echo "   ✅ Validated $(human "$OPTIMIZED_SIZE") (saved $(( ORIGINAL_SIZE - OPTIMIZED_SIZE )) bytes)"
            else
                rm -f "$TMP_OUT"
                echo "   ⚠️  Optimized output was invalid or not smaller; keeping the Go build"
            fi
        else
            rm -f "$TMP_OUT"
            echo "   ⚠️  wasm-opt failed; keeping the validated Go build"
        fi

        validate_wasm "$WASM_OUT"
        echo "   Final validated size: $(human "$(filesize "$WASM_OUT")")"
    elif command -v wasm-strip >/dev/null 2>&1; then
        echo "🔧 Stripping debug sections with wasm-strip…"
        wasm-strip "$WASM_OUT" || true
    else
        echo "ℹ️  Tip: install Binaryen (wasm-opt) for further size reduction"
    fi

    # ── gzip pre-compress ───────────────────────────────────────────────────
    if command -v gzip >/dev/null 2>&1; then
        gzip -9 -c "$WASM_OUT" > "${WASM_OUT}.gz" 2>/dev/null || true
    fi
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
