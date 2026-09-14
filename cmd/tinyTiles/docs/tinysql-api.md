# tinySQL integration API

tinyTiles depends only on the versioned public package
`github.com/SimonWaldherr/tinySQL/tiles`. It does not import tinySQL internals,
decode pager files, or depend on a concrete reader implementation. Preserve this
boundary when tinyTiles moves to its own repository.

## v1 contract

```go
import tiles "github.com/SimonWaldherr/tinySQL/tiles"

result, err := tiles.ImportMBTiles(ctx, "region.mbtiles", "region.ttiles", &tiles.ImportOptions{
    Schema: tiles.SchemaNormalized, BatchSize: 2_048,
    MaxMemoryBytes: 256 << 20, MinFreeBytes: 8 << 30,
})
if err != nil {
    return err
}

if _, err := tiles.ValidateArtifact(ctx, result.ArtifactPath); err != nil {
    return err
}
reader, err := tiles.OpenArtifact(ctx, result.ArtifactPath, tiles.OpenOptions{
    MaxMemoryBytes: 32 << 20,
})
if err != nil {
    return err
}
defer reader.Close()

tile, found, err := reader.Lookup(ctx, tiles.Key{Z: 12, X: 2184, Y: 1381})
```

| Need | Public API | Contract |
| --- | --- | --- |
| Immutable publication | `ImportMBTiles` | Preflight resource gates, bounded batches, complete validation, atomic publish. |
| Promotion audit | `ValidateArtifact` | Checks marker, checksums, schema, keys, indexes, and logical digests. |
| Serving handle | `OpenArtifact` | Validates before returning one concurrent reader. |
| Tile read | `Lookup` / `LookupFunc` | Exact TMS key and caller-owned byte slice. |
| Spatial read | `Scan` | Ordered, bounded TMS range; callback errors propagate. |
| Metadata | `Metadata` / `MetadataScanner.ScanMetadata` | Exact lookup or ordered callback walk without tile-table materialization. |
| Artifact description | `Info` | Deep-copied semantic information, without pager internals. |

`tiles.Reader` is the serving contract. `ValidateArtifact` and `OpenArtifact`
use only the published artifact on supported native targets; only
`ImportMBTiles` needs the optional `sqliteimport` build tag. The reader CLI and
server are therefore tested SQLite-free, while the builder retains the tag.

`ArtifactInfo` exposes schema, tables, checksums, logical digests, coordinate
system, and semantic index names. It does not make `storage.DB`, paged-index
pages, raw manifests, or a concrete reader type part of the compatibility
promise. `APIVersion` is `1`: additive fields and functions are allowed;
incompatible behavior requires a new public contract. Pin a released tinySQL
version when tinyTiles leaves this worktree.

## Lifecycle and ownership

The operational sequence is documented in [operations](operations.md): import
to a temporary sibling, validate, write `COMPLETE`, fsync, and atomically
publish. `tiles.Reader` values are independent concurrent borrowers. A pooled
dataset closes a reader only after it returns from service; a returned tile byte
slice may outlive the call, but applications must not retain unbounded payloads.

## Why the SQL driver is not the tile hot path

The public tinySQL `driver` remains suitable for general databases, but typed
tile APIs avoid SQL parsing, binding, row scanning, and BLOB conversion for a
natural `(z, x, y)` lookup. They can also state TMS convention, payload
ownership, range order, cache budget, and fail-closed validation directly. A
read-only SQL or audit adapter can be added later without becoming a serving-path
dependency.

Historical root-level `tinysql.OpenMBTilesReader` and `OpenMBTilesArtifact`
remain compatibility APIs during migration. New tinyTiles code uses
`tiles.OpenArtifact` and never needs a tinySQL internal package.

## tinyTiles application surfaces

| Surface | Import path / binary | Intended owner |
| --- | --- | --- |
| Library | `github.com/Karte-Bayern/tinyTiles` | Go application that owns lookup, cache, and lifecycle policy. |
| HTTP adapter | `github.com/Karte-Bayern/tinyTiles/server` | Existing `http.ServeMux` application with XYZ, TileJSON, or sync routes. |
| Binary | `tinytiles-server` | Dedicated artifact process with external edge policy. |

`tinytiles.Open(ctx, path, OpenOptions)` returns a concurrent `Dataset`.
`LookupTMS`/`ScanTMS` are TMS; `LookupXYZ` and `GetTileXYZ` convert the Y value
for XYZ requests. Missing tiles return `tinytiles.ErrTileNotFound`, compatible
with `database/sql.ErrNoRows`. `server.XYZHandler()` is available when a generic
handler is preferable to the application adapter.

## Routing remains separate

Tiles describe rendering output, not the directed topology, access rules, turn
restrictions, edge geometry, or profile-specific weights required for routing.
A future `tinyRoute` project should publish its own versioned artifact and
`RouteReader` contract, sharing only a provenance link to the OSM PBF revision.
It needs independent routing semantics, spatial snapping, deterministic results,
query strategy, checksums, preflight, publication, and rollback. A `.ttiles`
deployment must not depend on a route graph.
