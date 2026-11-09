#!/usr/bin/env bash
# Prepares a self-contained SwiftPM release of the TinySQL package.
#
#   bindings/apple/package-release.sh https://example.com/releases/1.2.0/CTinySQL.xcframework.zip
#
# Builds every platform slice, zips the XCFramework, and writes
# bindings/apple/build/release/ with:
#
#   CTinySQL.xcframework.zip   upload this to the URL you passed
#   Package.swift              the package manifest, with that URL and the checksum
#   Sources/TinySQL/           the Swift sources
#   LICENSE
#
# Commit Package.swift and Sources/ to the repository that serves the package
# (SwiftPM needs Package.swift at the repository root), tag a version, and
# upload the zip to the URL. Apps then add the package by URL with
# "Up to Next Major Version" and need neither Go nor a local build.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 https://host/path/CTinySQL.xcframework.zip" >&2
    echo "The URL is where you will upload the zip; SwiftPM requires https." >&2
    exit 2
fi
url="$1"
if [[ "$(uname -s)" != Darwin ]]; then
    echo "Release packaging requires macOS and Xcode." >&2
    exit 1
fi

# Validate before building, and produce an escaped Swift string literal for the
# manifest. Interpolating a raw URL into Swift source mishandles quotes/backslashes.
url_literal="$(swift - "$url" <<'SWIFT'
import Foundation
let input = CommandLine.arguments[1]
guard let url = URL(string: input), url.scheme == "https",
      let host = url.host, !host.isEmpty, url.pathExtension == "zip",
      !input.unicodeScalars.contains(where: { CharacterSet.whitespacesAndNewlines.contains($0) || CharacterSet.controlCharacters.contains($0) }) else {
    fputs("Expected an absolute https URL with a .zip path and no whitespace.\n", stderr)
    exit(2)
}
print(String(reflecting: input))
SWIFT
)"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
package="$root/bindings/swift"
out="$root/bindings/apple/build/release"

"$root/bindings/apple/build-xcframework.sh" all

stage="$(mktemp -d "$root/bindings/apple/build/release.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
# ditto keeps the symlinks and metadata an XCFramework needs; zip would not.
ditto -c -k --sequesterRsrc --keepParent "$package/CTinySQL.xcframework" "$stage/CTinySQL.xcframework.zip"
# Run in the staging directory: SwiftPM creates .build in the working directory.
checksum="$(cd "$stage" && swift package compute-checksum CTinySQL.xcframework.zip)"

cp -R "$package/Sources" "$stage/Sources"
cp "$root/LICENSE" "$stage/LICENSE"
cat > "$stage/Package.swift" <<MANIFEST
// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "TinySQL",
    platforms: [.macOS(.v12), .iOS(.v15)],
    products: [.library(name: "TinySQL", targets: ["TinySQL"])],
    targets: [
        .binaryTarget(
            name: "CTinySQL",
            url: $url_literal,
            checksum: "$checksum"
        ),
        .target(name: "TinySQL", dependencies: ["CTinySQL"], linkerSettings: [
            .linkedFramework("CoreFoundation"),
            .linkedFramework("Security"),
            .linkedLibrary("resolv"),
        ]),
    ]
)
MANIFEST

# The manifest must parse; this catches a malformed URL or checksum early.
swift package --package-path "$stage" dump-package >/dev/null
rm -rf "$stage/.build"

# Keep the last complete release until the new archive and manifest validate.
rm -rf "$out"
mv "$stage" "$out"
trap - EXIT

echo
echo "Release ready in: $out"
echo "  checksum: $checksum"
echo "  1. Upload CTinySQL.xcframework.zip to $url"
echo "  2. Commit Package.swift, Sources/ and LICENSE to the package repository and tag a version"
