#!/bin/bash

set -e

echo "🔨 Building TinySQL Query Files WASM..."

# Set WASM build flags
export GOOS=js
export GOARCH=wasm

# Build the WASM binary with size-friendly flags
echo "📦 Compiling Go to WASM (stripping debug/symbols)..."
# Use -ldflags to remove symbol table and debug info, and -trimpath to avoid embedding local paths.
go build -trimpath -ldflags "-s -w" -o query_files.wasm

# Copy wasm_exec.js from Go installation
echo "📋 Copying wasm_exec.js..."
WASM_EXEC=$(find $(go env GOROOT) -name "wasm_exec.js" 2>/dev/null | head -1)
if [ -z "$WASM_EXEC" ]; then
    echo "❌ Could not find wasm_exec.js in Go installation"
    exit 1
fi
cp "$WASM_EXEC" .

echo "✅ Build complete!"
if command -v wasm-opt >/dev/null 2>&1; then
    echo "🔧 Found wasm-opt, trying multiple optimization variants (including --converge)..."

    # Candidate flag sets to try. We will pick the smallest resulting file.
    VARIANTS=(
        "--enable-bulk-memory -Oz --strip-debug"
        "--enable-bulk-memory -Oz --strip-debug --converge"
        "--enable-bulk-memory -O3 --strip-debug --converge"
    )

    # Start with the original file as the current best
    BEST_FILE="query_files.wasm"
    BEST_SIZE=$(stat -f%z "$BEST_FILE" 2>/dev/null || stat -c%s "$BEST_FILE" 2>/dev/null || echo 0)

    for v in "${VARIANTS[@]}"; do
        # Produce a filename suffix safe string for this variant
        suffix=$(echo "$v" | tr ' ' '_' | tr -c 'a-zA-Z0-9._-' '_')
        out="query_files.${suffix}.wasm"
        echo "🔁 Running: wasm-opt $v -> $out"
        # shellcheck disable=SC2086
        wasm-opt $v -o "$out" query_files.wasm || { echo "⚠️ wasm-opt failed for variant: $v"; continue; }

        sz=$(stat -f%z "$out" 2>/dev/null || stat -c%s "$out" 2>/dev/null || echo 0)
        echo "   -> size: $sz bytes"
        if [ "$BEST_SIZE" -eq 0 ] || [ "$sz" -lt "$BEST_SIZE" ]; then
            echo "   ✅ New best wasm: $out ($sz bytes)"
            mv -f "$out" query_files.wasm
            BEST_SIZE=$sz
        else
            rm -f "$out"
        fi
    done

    echo "🔎 Final chosen wasm size: $(stat -f%z query_files.wasm 2>/dev/null || stat -c%s query_files.wasm 2>/dev/null) bytes"
elif command -v wasm-strip >/dev/null 2>&1; then
    echo "🔧 Found wasm-strip, stripping debug sections..."
    wasm-strip query_files.wasm || true
else
    echo "ℹ️  wasm-opt/wasm-strip not found; install Binaryen or wabt for further wasm size optimizations"
fi

echo "📏 Resulting file sizes:"
ls -lh query_files.wasm || true
# Produce a pre-compressed gzip version for efficient HTTP delivery
if command -v gzip >/dev/null 2>&1; then
    echo "🔨 Creating pre-compressed gzip .wasm.gz (best compression)..."
    gzip -9 -c query_files.wasm > query_files.wasm.gz || true
    ls -lh query_files.wasm.gz || true
    echo "(Tip: serve the .wasm.gz with Content-Encoding: gzip for smaller downloads)"
else
    echo "(Tip: gzip the .wasm for HTTP delivery; many servers honor pre-compressed .wasm.gz)"
fi
echo ""
echo "📂 Generated files:"
echo "   - query_files.wasm (Go compiled to WebAssembly)"
echo "   - wasm_exec.js (Go WASM runtime)"
echo ""
echo "🚀 To test locally:"
echo "   python3 -m http.server 8080"
echo "   # or"
echo "   php -S localhost:8080"
echo ""
echo "   Then open: http://localhost:8080"
echo ""
echo "📤 To deploy to GitHub Pages:"
echo "   1. Commit all files (*.html, *.js, *.wasm)"
echo "   2. Push to GitHub repository"
echo "   3. Enable GitHub Pages in repository settings"
echo "   4. Select branch and /cmd/query_files_wasm folder"
