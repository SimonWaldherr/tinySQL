#!/usr/bin/env bash
# Build from any working directory. Requires macOS, full Xcode and Go.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mode="${1:-all}"
if [[ $# -gt 1 ]]; then echo "Usage: $0 [all|macos]" >&2; exit 2; fi
case "$mode" in all|macos) ;; *) echo "Usage: $0 [all|macos]" >&2; exit 2 ;; esac
if [[ "$(uname -s)" != Darwin ]]; then
    echo "XCFramework builds require macOS and Xcode." >&2
    exit 1
fi
command -v go >/dev/null
command -v xcodebuild >/dev/null
build="$root/bindings/apple/build"
package="$root/bindings/swift"
mkdir -p "$build"
# Use a unique staging directory; retain the last complete framework on failure.
stage="$(mktemp -d "$build/staging.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
cat > "$stage/clang" <<'CLANG'
#!/usr/bin/env bash
exec "$TINYSQL_APPLE_CC" "$@"
CLANG
chmod +x "$stage/clang"
cd "$root"

build_slice() {
    local name="$1" sdk="$2" os="$3" arch="$4" target="$5"
    local sdk_path compiler
    sdk_path="$(xcrun --sdk "$sdk" --show-sdk-path)"
    compiler="$(xcrun --sdk "$sdk" --find clang)"
    mkdir -p "$stage/$name"
    echo "Building $name ($target)"
    TINYSQL_APPLE_CC="$compiler" GOOS="$os" GOARCH="$arch" CGO_ENABLED=1 \
        CC="\"$stage/clang\"" \
        CGO_CFLAGS="-target $target -isysroot \"$sdk_path\"" \
        CGO_LDFLAGS="-target $target -isysroot \"$sdk_path\"" \
        go build -trimpath -buildmode=c-archive \
        -o "$stage/$name/libCTinySQL.a" ./bindings/apple
}

build_slice macos-arm64 macosx darwin arm64 arm64-apple-macos12.0
build_slice macos-amd64 macosx darwin amd64 x86_64-apple-macos12.0
mkdir -p "$stage/macos"
xcrun lipo -create "$stage/macos-arm64/libCTinySQL.a" "$stage/macos-amd64/libCTinySQL.a" \
    -output "$stage/macos/libCTinySQL.a"
args=(-library "$stage/macos/libCTinySQL.a" -headers "$root/bindings/apple/include")
if [[ "$mode" == all ]]; then
    build_slice ios-arm64 iphoneos ios arm64 arm64-apple-ios15.0
    build_slice simulator-arm64 iphonesimulator ios arm64 arm64-apple-ios15.0-simulator
    build_slice simulator-amd64 iphonesimulator ios amd64 x86_64-apple-ios15.0-simulator
    mkdir -p "$stage/simulator"
    xcrun lipo -create "$stage/simulator-arm64/libCTinySQL.a" "$stage/simulator-amd64/libCTinySQL.a" \
        -output "$stage/simulator/libCTinySQL.a"
    args+=(-library "$stage/ios-arm64/libCTinySQL.a" -headers "$root/bindings/apple/include")
    args+=(-library "$stage/simulator/libCTinySQL.a" -headers "$root/bindings/apple/include")
fi
xcodebuild -create-xcframework "${args[@]}" -output "$stage/CTinySQL.xcframework"
rm -rf "$package/CTinySQL.xcframework"
mv "$stage/CTinySQL.xcframework" "$package/CTinySQL.xcframework"
echo "Ready: $package (add this local package in Xcode)"
