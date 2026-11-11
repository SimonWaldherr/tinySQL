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
FINGERPRINT_FILE=".build-fingerprint"
BUILD_FLAGS=(-tags no_http -trimpath -buildvcs=false -ldflags "-s -w")

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
    elif command -v wasm-opt >/dev/null 2>&1; then
        # wasm-opt validates while reading/writing even when no optimization
        # pass is requested.
        wasm-opt --all-features "$wasm_file" -o /dev/null
    else
        # A Go build does not require Node or Binaryen. Still reject truncated
        # output on machines without a complete WASM validator.
        [ "$(od -An -tx1 -N8 "$wasm_file" | tr -d ' \n')" = "0061736d01000000" ]
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

hash_stream() {
    local digest
    if command -v sha256sum >/dev/null 2>&1; then
        digest="$(sha256sum)" || return 1
    else
        digest="$(shasum -a 256)" || return 1
    fi
    printf '%s\n' "${digest%% *}"
}

build_fingerprint() {
    # Let Go track selected sources, embedded files, dependencies and build
    # flags by content. Timestamps are unreliable after checkouts/cache restores.
    # -export populates the same compilation cache used by the following build.
    GOOS=js GOARCH=wasm go list -deps -export "${BUILD_FLAGS[@]}" \
        -f '{{.ImportPath}} {{.BuildID}}' . > "$BUILD_TMP/dependencies" || return 1
    {
        printf '%s\n' "$GO_VERSION" "${GOFLAGS:-}" "$OPTIMIZER" "$OPTIMIZER_VERSION" "$HAVE_GZIP"
        cat "$BUILD_TMP/dependencies" "$SCRIPT_DIR/build.sh" "$WASM_EXEC"
    } | hash_stream
}

artifact_fingerprint() {
    local directory="$1" file
    for file in "$WASM_OUT" wasm_exec.js; do
        [ -s "$directory/$file" ] || return 1
    done
    if $HAVE_GZIP; then
        [ -s "$directory/${WASM_OUT}.gz" ] || return 1
    fi
    {
        for file in "$WASM_OUT" wasm_exec.js; do
            hash_stream < "$directory/$file" || return 1
        done
        if $HAVE_GZIP; then hash_stream < "$directory/${WASM_OUT}.gz" || return 1; fi
    } | hash_stream
}

artifacts_are_current() {
    local expected artifacts
    [ -s "$FINGERPRINT_FILE" ] || return 1
    artifacts="$(artifact_fingerprint .)" || return 1
    expected="$(printf '%s\n%s' "$INPUT_FINGERPRINT" "$artifacts")"
    [ "$(cat "$FINGERPRINT_FILE")" = "$expected" ]
}

if $CLEAN; then
    echo "🧹 Cleaning generated files…"
    rm -f "$WASM_OUT" "${WASM_OUT}.gz" wasm_exec.js "$FINGERPRINT_FILE"
    echo "   Done."
    exit 0
fi

# ── pre-flight checks ───────────────────────────────────────────────────────
if ! $SKIP_BUILD && ! command -v go >/dev/null 2>&1; then
    echo "❌ Go toolchain not found. Install Go from https://go.dev/dl/"
    exit 1
fi

if ! $SKIP_BUILD; then
    BUILD_TMP="$(mktemp -d "$SCRIPT_DIR/.wasm-build.XXXXXX")"
    trap 'rm -rf "$BUILD_TMP"' EXIT
    GO_VERSION="$(go version)"
    WASM_EXEC="$(wasm_exec_source)" || { echo "Could not find wasm_exec.js in the Go installation" >&2; exit 1; }
    OPTIMIZER="none"
    OPTIMIZER_VERSION=""
    if command -v wasm-opt >/dev/null 2>&1; then
        OPTIMIZER="$(command -v wasm-opt)"
        OPTIMIZER_VERSION="$(wasm-opt --version)"
    elif command -v wasm-strip >/dev/null 2>&1; then
        OPTIMIZER="$(command -v wasm-strip)"
        OPTIMIZER_VERSION="$(wasm-strip --version)"
    fi
    HAVE_GZIP=false
    if command -v gzip >/dev/null 2>&1; then HAVE_GZIP=true; fi
    echo "🔎 Checking Go build inputs…"
    INPUT_FINGERPRINT="$(build_fingerprint)"
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
    echo "   Go: $GO_VERSION"
    if [ -n "${GOFLAGS:-}" ]; then
        echo "   GOFLAGS: ${GOFLAGS}"
    fi
fi

T0=$(date +%s)
if ! $SKIP_BUILD; then
    # ── compile ──────────────────────────────────────────────────────────────
    echo "📦 Compiling Go → WASM (stripping debug info)…"
    # no_http drops the SQL HTTP() builtin and its net/http/crypto/tls
    # dependency graph: this demo's own UI never calls it, and it is the
    # single largest avoidable slice of the compiled binary (~20% smaller
    # raw and gzipped). Scoped to this build only -- see
    # internal/engine/io_functions_http_disabled.go.
    # GOFLAGS is read by Go itself; expanding it here again breaks quoted flags.
    GOOS=js GOARCH=wasm go build "${BUILD_FLAGS[@]}" -o "$BUILD_TMP/$WASM_OUT" .
    RAW_SIZE=$(filesize "$BUILD_TMP/$WASM_OUT")
    echo "   Compiled in $(elapsed $T0)  –  raw size: $(human "$RAW_SIZE")"

    # ── copy wasm_exec.js ────────────────────────────────────────────────────
    echo "📋 Copying wasm_exec.js…"
    cp "$WASM_EXEC" "$BUILD_TMP/wasm_exec.js"
    echo "   Copied from $WASM_EXEC"

    # ── optional wasm-opt / wasm-strip ──────────────────────────────────────
    if command -v wasm-opt >/dev/null 2>&1; then
        echo "🔧 Optimising with wasm-opt -Oz (single release pass)…"

        ORIGINAL_SIZE=$(filesize "$BUILD_TMP/$WASM_OUT")
        TMP_OUT="$BUILD_TMP/optimized.wasm"
        # Go emits bulk-memory and non-trapping float-to-int operations.
        # --all-features lets Binaryen validate those instructions; the former
        # --enable-bulk-memory-only invocation silently rejected the module.
        if wasm-opt --all-features -Oz --strip-debug -o "$TMP_OUT" "$BUILD_TMP/$WASM_OUT"; then
            OPTIMIZED_SIZE=$(filesize "$TMP_OUT")
            if [ "$OPTIMIZED_SIZE" -gt 0 ] &&
               [ "$OPTIMIZED_SIZE" -lt "$ORIGINAL_SIZE" ] &&
               validate_wasm "$TMP_OUT"; then
                mv -f "$TMP_OUT" "$BUILD_TMP/$WASM_OUT"
                echo "   ✅ Validated $(human "$OPTIMIZED_SIZE") (saved $(( ORIGINAL_SIZE - OPTIMIZED_SIZE )) bytes)"
            else
                rm -f "$TMP_OUT"
                echo "   ⚠️  Optimized output was invalid or not smaller; keeping the Go build"
            fi
        else
            rm -f "$TMP_OUT"
            echo "   ⚠️  wasm-opt failed; keeping the validated Go build"
        fi

    elif command -v wasm-strip >/dev/null 2>&1; then
        echo "🔧 Stripping debug sections with wasm-strip…"
        cp "$BUILD_TMP/$WASM_OUT" "$BUILD_TMP/stripped.wasm"
        if wasm-strip "$BUILD_TMP/stripped.wasm" && validate_wasm "$BUILD_TMP/stripped.wasm"; then
            mv -f "$BUILD_TMP/stripped.wasm" "$BUILD_TMP/$WASM_OUT"
        else
            echo "   ⚠️  wasm-strip failed validation; keeping the Go build" >&2
        fi
    else
        echo "ℹ️  Tip: install Binaryen (wasm-opt) for further size reduction"
    fi
    validate_wasm "$BUILD_TMP/$WASM_OUT"
    echo "   Final validated size: $(human "$(filesize "$BUILD_TMP/$WASM_OUT")")"

    # ── gzip pre-compress ───────────────────────────────────────────────────
    if $HAVE_GZIP; then
        gzip -n -9 -c "$BUILD_TMP/$WASM_OUT" > "$BUILD_TMP/${WASM_OUT}.gz"
        gzip -t "$BUILD_TMP/${WASM_OUT}.gz"
    fi
    OUTPUT_FINGERPRINT="$(artifact_fingerprint "$BUILD_TMP")"
    printf '%s\n%s\n' "$INPUT_FINGERPRINT" "$OUTPUT_FINGERPRINT" > "$BUILD_TMP/$FINGERPRINT_FILE"
    # Publish only complete, validated files; write the cache marker last.
    mv -f "$BUILD_TMP/$WASM_OUT" "$WASM_OUT"
    mv -f "$BUILD_TMP/wasm_exec.js" wasm_exec.js
    if $HAVE_GZIP; then mv -f "$BUILD_TMP/${WASM_OUT}.gz" "${WASM_OUT}.gz"; else rm -f "${WASM_OUT}.gz"; fi
    mv -f "$BUILD_TMP/$FINGERPRINT_FILE" "$FINGERPRINT_FILE"
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
