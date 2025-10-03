#!/bin/bash

set -e

echo "🔨 Building TinySQL Query Files WASM..."

# Set WASM build flags
export GOOS=js
export GOARCH=wasm

# Build the WASM binary
echo "📦 Compiling Go to WASM..."
go build -o query_files.wasm

# Copy wasm_exec.js from Go installation
echo "📋 Copying wasm_exec.js..."
WASM_EXEC=$(find $(go env GOROOT) -name "wasm_exec.js" 2>/dev/null | head -1)
if [ -z "$WASM_EXEC" ]; then
    echo "❌ Could not find wasm_exec.js in Go installation"
    exit 1
fi
cp "$WASM_EXEC" .

echo "✅ Build complete!"
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
