# tinyTiles

tinyTiles builds, validates, serves, and synchronizes immutable map-tile
artifacts. It imports an MBTiles source into a .ttiles directory backed by
tinySQL's paged index, then offers a SQLite-free read path for servers, native
clients, and browser caches.

MBTiles remains the interoperable build format. A .ttiles directory is a
separate, validated serving artifact rather than an MBTiles compatibility layer.

## Quick start

Run these commands from cmd/tinyTiles:

```bash
make build
./dist/tinytiles import --min-free 0 region.mbtiles region.ttiles/
./dist/tinytiles validate region.ttiles/
./dist/tinytiles inspect region.ttiles/
```

Import, PBF build, and SQLite comparison use the sqliteimport build tag. A
host that only validates or reads an existing artifact can use the smaller
SQLite-free reader instead:

```bash
make build-reader-cli
./dist/tinytiles-reader validate region.ttiles/
./dist/tinytiles-reader tile region.ttiles/ 8 137 167 > tile.pbf
```

All tile coordinates accepted by the CLI are TMS (z, x, y). The reader CLI
supports validate, inspect, and tile; build, import, and benchmark explicitly
require SQLite support.

## Build an artifact

```text
tinytiles build      source.osm.pbf[,more.osm.pbf] dataset.ttiles/
tinytiles import     source.mbtiles dataset.ttiles/
tinytiles validate   dataset.ttiles/
tinytiles inspect    dataset.ttiles/
tinytiles tile       dataset.ttiles/ z x y
tinytiles benchmark  --source source.mbtiles --artifact dataset.ttiles/
```

Import streams bounded batches, checks the configured memory and free-disk
reserve before publication, validates keys, indexes, metadata, and checksums,
then publishes atomically. An existing destination needs --replace and is
swapped only after the new artifact has passed validation. Readers reject
partial or corrupt artifacts.

To build from OSM PBF, supply a compatible external renderer. It owns map
styling and feature selection; tinyTiles owns bounded import and artifact
validation:

```bash
./dist/tinytiles build \
  --generator /path/to/karte-preprocess \
  --minzoom 8 --maxzoom 14 \
  region.osm.pbf region.ttiles/
```

The artifact contains manifest.json, database, indexes, checksums.sha256, and
a COMPLETE marker. Publication and recovery details are in
[operations](docs/operations.md).

## Serve and sync

Build the standalone server and native demo client:

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

The server exposes XYZ tiles at /tiles/{z}/{x}/{y}.mvt, TileJSON at
/tilejson.json, metadata at /metadata, and an immutable TMS synchronization
protocol at /sync/manifest.json. It does not provide authentication,
authorization, TLS, rate limiting, or deployment policy. Put those controls in
the enclosing application or proxy. Set -public-base when the server is behind
a proxy.

Native and browser clients publish a new cache revision only after every
requested tile is present and checksum-valid. Interrupted synchronization keeps
the previous revision active. The protocol, CORS requirements, and cache rules
are in [offline synchronization](docs/offline-sync.md). Runnable server, native,
and WASM examples are in [examples](examples/README.md).

## Embed it in Go

```go
import (
	"context"
	"net/http"

	tinytiles "github.com/Karte-Bayern/tinyTiles"
	"github.com/Karte-Bayern/tinyTiles/server"
)

func mountTiles(ctx context.Context, mux *http.ServeMux) (*tinytiles.Dataset, error) {
	dataset, err := tinytiles.Open(ctx, "region.ttiles", tinytiles.OpenOptions{
		Readers:        8,
		MaxMemoryBytes: 16 << 20, // per reader
	})
	if err != nil {
		return nil, err
	}

	tileServer, err := server.New(server.Config{
		Dataset: dataset,
		DatasetID: "region",
	})
	if err != nil {
		_ = dataset.Close()
		return nil, err
	}
	mux.Handle("/tiles/", http.StripPrefix("/tiles/", tileServer.XYZHandler()))
	return dataset, nil // Close it after the HTTP server shuts down.
}
```

Dataset provides explicit TMS lookup and scan methods, plus LookupXYZ and
GetTileXYZ for the application boundary. The reader pool is concurrency-safe;
MaxMemoryBytes is a per-reader budget. The stable boundary is the public
[tinySQL integration API](docs/tinysql-api.md), never a tinySQL internal pager.

## Development

```bash
make fmt-check
make vet
make test
make test-race
make reader-no-sqlite-check
make wasm-check
make ci
```

Use make bench-fixture with an MBTiles fixture and artifact to compare warm
point lookups in the current environment.

## Scope, license, and security

Supported MBTiles input is flat tiles or normalized map/images tables with
metadata. UTFGrid, arbitrary SQLite views, writing MBTiles, arbitrary SQL, and
incremental .ttiles updates are outside scope. See
[compatibility](docs/compatibility.md).

tinyTiles is AGPL-3.0-only. See [LICENSE](LICENSE) and [SECURITY.md](SECURITY.md)
for license and reporting guidance.
