# Compatibility contract

## tinyTiles artifacts

tinyTiles artifacts are read-only directory artifacts. They are not SQLite
databases and must not use the `.mbtiles` extension. The recommended directory
extension is `.ttiles`.

At publication time an artifact must contain a manifest, checksums and a
completion marker. Readers reject incomplete artifacts. Full validation is
performed before publication and can be requested explicitly for audits.

## MBTiles input

The first supported import profile is deliberately narrow:

| Capability | Status |
|---|---|
| Flat `tiles(z,x,y,tile_data)` source | supported via tinySQL adapter |
| Normalized `map/images` source | supported via tinySQL adapter |
| Metadata key/value preservation | supported via tinySQL adapter |
| TMS point reads | supported via `tinytiles tile` |
| TMS range reads | supported by the adapter API and revisioned sync protocol |
| OSM PBF → `.ttiles` | supported through the explicit Karte.Bayern `cmd/preprocess` adapter |
| Native offline cache | supported via atomic FileStore records |
| Browser offline cache | supported via a WASM IndexedDB cache and HTTP sync manifest |
| Browser reading of `.ttiles` pager files | intentionally unsupported |
| UTFGrid | explicitly out of scope initially |
| Arbitrary SQLite views | explicitly out of scope initially |
| Writing `.mbtiles` | out of scope |

MBTiles remains the interoperable source format. tinyTiles does not claim to
replace it.

## OSM PBF build boundary

PBF files contain geographic data, not a universal tile rendering policy.
`tinytiles build` therefore delegates PBF interpretation to a separately built
Karte.Bayern-compatible generator instead of copying project-specific layer
rules into this repository. The adapter passes its documented PBF inputs,
zoom range, shard configuration, optional district boundaries and geographic
filters through as structured process arguments, then imports and validates
the generated MBTiles source before atomically publishing the `.ttiles`
directory. The resulting checksummed manifest records the PBF input basenames
and sizes plus the generator adapter/configuration, without storing local
absolute paths.

This makes PBF→`.ttiles` a direct operator workflow while keeping the map
profile versioned with its generator. A future generic OSM renderer can become
another explicit adapter; it must not silently change tinyTiles' artifact or
tile semantics.

## Offline / WASM boundary

The `.ttiles` reader is native and read-only. Browser clients use the separate
`offline` protocol: a revisioned JSON manifest plus individual raw TMS tile
responses are stored in IndexedDB. That avoids a SQLite runtime and avoids
coupling browser storage to tinySQL pages, but it also means `.ttiles` is not
a drop-in MBTiles file or a browser file format.

The protocol preserves raw tile bytes and relevant content metadata when the
server supplies `X-TinyTiles-SHA256` and optional
`X-TinyTiles-Content-Encoding`. It requires CORS exposure of those custom
headers for cross-origin browser synchronization. See
[offline-sync.md](offline-sync.md) for the wire contract.
