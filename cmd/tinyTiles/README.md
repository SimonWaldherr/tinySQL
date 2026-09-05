# tinyTiles

`tinyTiles` is a production-oriented toolkit for building, validating, serving
and synchronizing read-only tile sets. It turns an MBTiles source into a
portable `.ttiles` directory backed by tinySQL's paged index, and provides a
separate offline-cache protocol for native clients and WebAssembly.

It is deliberately **not** a claim of 100% MBTiles compatibility. MBTiles is
a SQLite container format; `.ttiles` is a distinct, immutable serving artifact.
Keep MBTiles as the interoperable build output and use tinyTiles where a
validated, SQLite-free read path is useful.

## What is included

- bounded MBTiles import for flat `tiles` and normalized `map/images` sources;
- exact TMS point and spatial-range reads through tinySQL's public API;
- atomic `.ttiles` publication and full post-write validation;
- direct OSM PBF → MBTiles → `.ttiles` orchestration through an explicit
  Karte.Bayern-compatible generator adapter;
- an importable concurrent `Dataset`, a mountable HTTP server and a small
  standalone server binary, plus durable native and browser IndexedDB caches;
- reproducible tests, race checks, WASM compilation, benchmarks and demos.

## Quick start

Import, PBF build and SQLite comparison need tinySQL's optional SQLite importer
build tag. A server that only opens an already published `.ttiles` artifact
does not link or open SQLite:

```bash
make build
./dist/tinytiles import --min-free 0 region.mbtiles region.ttiles/
./dist/tinytiles validate region.ttiles/
./dist/tinytiles inspect region.ttiles/
```

For a deployment or recovery host that only reads a published artifact, build
the smaller SQLite-free CLI instead:

```bash
make build-reader-cli
./dist/tinytiles-reader validate region.ttiles/
./dist/tinytiles-reader tile region.ttiles/ 8 137 167 > tile.pbf
```

Its `validate`, `inspect` and `tile` commands work without SQLite. `build`,
`import` and `benchmark` intentionally return a clear build-tag error there.

Build from PBF when a compatible generator is available:

```bash
./dist/tinytiles build \
  --generator /path/to/karte-preprocess \
  --minzoom 8 --maxzoom 14 --building-minzoom 12 \
  --shards 256 --max-memory $((256 * 1024 * 1024)) \
  region.osm.pbf region.ttiles/
```

`build` does not invent a map style. The external generator owns OSM feature
selection, styling and MVT layer semantics; tinyTiles owns the bounded import,
artifact contract and reader/cache path.

## Architecture

```mermaid
flowchart LR
    P["OSM PBF"] --> G["explicit renderer\n(map semantics)"]
    G --> M["MBTiles\n(flat or normalized)"]
    M --> I["tinytiles import\nresource gate + validation"]
    I --> A["immutable .ttiles artifact\nmanifest + checksums + COMPLETE"]

    subgraph TS["tinySQL public API"]
        R["tiles.Reader v1\nTMS Lookup / Scan / Metadata"]
        D["database/sql driver\noptional general SQL only"]
    end

    A --> R
    R --> D2["tinyTiles Dataset\nreader pool + XYZ/TMS boundary"]
    D2 --> H["mountable HTTP server\nor standalone binary"]
    H --> N["native FileStore cache"]
    H --> W["WASM IndexedDB cache"]
    D -. "not the tile request hot path" .-> R
```

The server-side dataset and browser cache have intentionally different storage
formats. A browser never opens SQLite or tinySQL page files; it stores bounded
individual TMS tiles under a versioned cache namespace. That makes the web
path portable and avoids shipping server persistence internals to a client.

The stable dependency boundary is the public
[`tinySQL/tiles` API](docs/tinysql-api.md), not a concrete pager or any
`internal/` package. The same document defines why the generic SQL driver is
not used for tile lookups and how routing should remain a separate future
artifact rather than becoming an accidental tinyTiles feature.

## Commands

```text
tinytiles build      source.osm.pbf[,more.osm.pbf] dataset.ttiles/
tinytiles import     source.mbtiles dataset.ttiles/
tinytiles validate   dataset.ttiles/
tinytiles inspect    dataset.ttiles/
tinytiles tile       dataset.ttiles/ z x y
tinytiles benchmark  --source source.mbtiles --artifact dataset.ttiles/
tinytiles version
tinytiles-server     -artifact dataset.ttiles/ -dataset region
```

Every tile coordinate is **TMS** `(z, x, y)`. The tool never silently flips
rows as XYZ. `tile` writes raw tile bytes to stdout or `-out`; use it only in
a binary-safe pipeline.

### Import and resource gates

```bash
./dist/tinytiles import \
  --schema auto \
  --batch 2048 \
  --max-memory $((256 * 1024 * 1024)) \
  --min-free $((8 * 1024 * 1024 * 1024)) \
  source.mbtiles dataset.ttiles/
```

Before writing the destination, import prints source size, tile count,
estimated working set, estimated disk use and available disk. It fails safely
if the configured memory or disk reserve is unavailable. Batches stream rows
directly into the paged index; tiles, source rows and index trees are never
held as one full in-memory collection. `Ctrl-C` is propagated through the PBF
generator and importer and leaves the previous published artifact untouched.

`--schema auto` preserves the source's flat or normalized shape. The importer
validates all tile keys, index completeness, checksums, metadata and tile
digests before publication. An existing destination requires `--replace` and
is swapped only after the new artifact has passed validation.

### PBF build adapter

`tinytiles build` invokes an external executable, defaulting to
`karte-preprocess`. Build Karte.Bayern's preprocessor once, or point
`--generator` at a compatible executable:

```bash
cd /path/to/Karte.Bayern
go build -o /path/to/bin/karte-preprocess ./cmd/preprocess

cd /path/to/tinyTiles
./dist/tinytiles build \
  --generator /path/to/bin/karte-preprocess \
  --mbtiles-out region.mbtiles \
  --replace-mbtiles --replace \
  region.osm.pbf region.ttiles/
```

Multiple PBF files are comma-separated. `--min-lat`, `--min-lon`, `--max-lat`,
`--max-lon`, `--center-lat`, `--center-lon` and `--radius-km` are deliberately
passed through as explicit Karte.Bayern adapter options. The checksummed
artifact manifest records portable PBF provenance (input basenames/sizes and
generator configuration), never machine-local absolute paths.

## `.ttiles` artifact contract

```text
dataset.ttiles/
  manifest.json
  database/
  indexes/
  checksums.sha256
  COMPLETE
```

`manifest.json` records artifact/tinySQL versions, physical schema, table and
row counts, index configuration, resource estimate, TMS convention, source
provenance and logical data digests. `checksums.sha256` includes the manifest
and all persisted artifact files. `COMPLETE` is written only after a complete
validation pass. Readers reject missing, partial or corrupt artifacts. Serving
code obtains a `tiles.Reader` through the public API, never a tinySQL internal
pager type.

Publication is a sibling temporary directory followed by validation, fsync and
rename. With `--replace`, a previous artifact is kept as a rollback candidate
until the replacement has been published. Details are in
[operations.md](docs/operations.md).

## Import as a Go package

`tinyTiles` has three deliberately equivalent surfaces: the importable
`tinytiles.Dataset`, the mountable `server` package and the
`tinytiles-server` binary. They use the same validated, SQLite-free artifact
reader; choose the surface that fits ownership of the HTTP listener.

```go
import (
    "context"
    "net/http"

    tinytiles "github.com/Karte-Bayern/tinyTiles"
    "github.com/Karte-Bayern/tinyTiles/server"
)

dataset, err := tinytiles.Open(context.Background(), "region.ttiles", tinytiles.OpenOptions{
    Readers: 8,
    MaxMemoryBytes: 16 << 20, // per reader
})
if err != nil { /* fail startup */ }
defer dataset.Close()

// Direct drop-in for Karte.Bayern's existing tileReader interface:
// GetTileXYZ(z, x, y) ([]byte, error), Metadata() (map[string]string, error), Close() error.
var tiles interface {
    GetTileXYZ(int, int, int) ([]byte, error)
    Metadata() (map[string]string, error)
    Close() error
} = dataset

// Or mount a generic XYZ/TileJSON/TMS-sync HTTP surface in an existing mux.
tileServer, err := server.New(server.Config{Dataset: dataset, DatasetID: "region", TileCacheBytes: 32 << 20})
if err != nil { /* fail startup */ }
http.Handle("/tiles/", http.StripPrefix("/tiles/", tileServer.XYZHandler()))
```

`Dataset.LookupTMS` and `Dataset.ScanTMS` are explicitly TMS; `LookupXYZ` and
the compatibility `GetTileXYZ` flip the row exactly once at the application
boundary. `Metadata` is read and copied at open time, so ordinary requests do
not scan a metadata table. A missing `GetTileXYZ` lookup returns
`tinytiles.ErrTileNotFound`, which is compatible with `sql.ErrNoRows`.

Karte.Bayern can therefore replace only the opening path with `tinytiles.Open`
and keep its existing cache, TileJSON and handler policy. No tinySQL internal
package, SQLite driver or production configuration is required at serving
time. The fuller integration contract is in [tinysql-api.md](docs/tinysql-api.md).

## Serve and synchronize offline tiles

Build the reference server and native client:

```bash
make build-server build-native-demo
./dist/tinytiles-server \
  -artifact /path/to/region.ttiles -dataset dach \
  -cors http://localhost:8081

./dist/tinytiles-native-client \
  -manifest http://localhost:8080/sync/manifest.json \
  -cache ./dach-offline -dataset dach \
  -z 8 -xmin 137 -xmax 138 -ymin 167 -ymax 168
```

`tinytiles-server` is built without `sqliteimport`: it opens only the
validated paged artifact through `tinySQL/tiles`. The `tinytiles` build/import
CLI intentionally retains the tag because it reads SQLite MBTiles input.

The standalone server intentionally has no authentication, authorization, rate
limiting or deployment configuration. It is a correct artifact-serving binary,
not a replacement for an application's edge policy. The standalone binary uses
`-tile-cache 33554432` by default: a shared 32 MiB LRU payload/checksum budget,
additionally capped at 4096 entries. Set `-tile-cache 0` to disable it. This
budget is separate from `-max-memory`, which applies to each artifact reader.
Embedding applications opt in with `server.Config.TileCacheBytes`.

XYZ and TMS sync share cached tiles for one immutable dataset revision. Bare
XYZ URLs revalidate; URLs with the current `tinytiles_rev` query parameter
receive immutable cache headers. An obsolete revision returns 404 instead of
serving new bytes under an old versioned URL.

The server exposes XYZ tiles at
`/tiles/{z}/{x}/{y}.mvt`, TileJSON at `/tilejson.json`, metadata at
`/metadata`, and the browser-safe revisioned TMS sync protocol at
`/sync/manifest.json`. See [examples/README.md](examples/README.md) for the
browser demo walkthrough.

### Browser/WASM cache

```bash
make wasm-package
make serve-wasm
```

The browser API is promise-based:

```js
await tinyTiles.open("dach-offline");
await tinyTiles.sync("https://tiles.example/sync/manifest.json", {
  dataset: "dach",
  ranges: [{ z: 8, x_min: 137, x_max: 138, y_min: 167, y_max: 168 }],
  concurrency: 4,
  prune_previous: false
});
const tile = await tinyTiles.get("dach", 8, 137, 167);
```

Synchronization streams a range into at most 32 workers, writes tiles under
an immutable manifest revision, and switches the active local manifest only
after every requested tile is present and checksum-valid. If it is interrupted,
the old revision stays active and a later sync reuses valid tiles already
stored for the new revision. The full protocol, CORS and checksum rules are in
[offline-sync.md](docs/offline-sync.md).

`wasm-package` also creates `tinytiles.wasm.gz` with a deterministic gzip
header. Production static hosting should serve that file as the
`tinytiles.wasm` representation with `Content-Type: application/wasm`,
`Content-Encoding: gzip` and `Vary: Accept-Encoding`. This transport compression
applies only to the WASM module; sync tile responses use the raw-tile rules in
the offline protocol.

## Quality workflow

```bash
make help       # all targets
make fmt-check
make vet
make test
make test-race
make coverage
make bench
make reader-no-sqlite-check
make wasm-check
make ci
```

`make ci` runs formatting, module drift, vet, native tests, race tests and a
SQLite-free server dependency check plus a WASM compile/static-demo check.
`make bench-fixture MBTILES=... ARTIFACT=...`
runs deterministic warm point lookups against SQLite and tinyTiles; it supports
both flat and normalized MBTiles sources and fails the p95 gate above 2× SQLite.
For fixture tiers, metrics and report requirements, see
[benchmarks.md](docs/benchmarks.md). The current Apple M2 Max Bayern/DACH
measurement and gate decision are recorded in
[benchmark-results-2026-08-05.md](docs/benchmark-results-2026-08-05.md).

## Repository layout and move to its own repository

This directory is intentionally self-contained as module
`github.com/Karte-Bayern/tinyTiles`. While it temporarily lives below the
tinySQL worktree, `go.mod` uses a local `replace` directive. Remove that line
and pin a released tinySQL version after moving this directory into its own
repository.

- `cmd/tinytiles/` — native import, validation, reader and benchmark CLI;
- `offline/` — revisioned cache/sync library plus FileStore and IndexedDB;
- `examples/` — HTTP server, native client and static WASM demo;
- `docs/` — compatibility, operations, sync protocol and benchmarks.

## Scope and compatibility

Supported MBTiles input is flat `tiles(zoom_level,tile_column,tile_row,tile_data)`
or normalized `map/images`, plus `metadata(name,value)`. UTFGrid, arbitrary
SQLite views, writing `.mbtiles`, arbitrary SQL and incremental `.ttiles`
updates are explicitly out of scope. See [compatibility.md](docs/compatibility.md).

## License and security

tinyTiles links the AGPL-licensed tinySQL artifact reader and is therefore
offered under **AGPL-3.0-only**. See [LICENSE](LICENSE) and
[SECURITY.md](SECURITY.md). Please do not publish exploit details in an issue
before the standalone repository has enabled private security advisories.
