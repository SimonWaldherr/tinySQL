// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "TinySQL",
    platforms: [.macOS(.v12), .iOS(.v15)],
    products: [.library(name: "TinySQL", targets: ["TinySQL"])],
    targets: [
        .binaryTarget(name: "CTinySQL", path: "CTinySQL.xcframework"),
        .target(name: "TinySQL", dependencies: ["CTinySQL"], linkerSettings: [
            .linkedFramework("CoreFoundation"),
            .linkedFramework("Security"),
            .linkedLibrary("resolv"),
        ]),
        .testTarget(name: "TinySQLTests", dependencies: ["TinySQL"]),
    ]
)
