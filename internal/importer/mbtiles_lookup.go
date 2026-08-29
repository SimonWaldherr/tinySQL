//go:build sqliteimport && !js && !wasm && !baremetal

package importer

// LookupMBTilesTile answers a single "give me this one tile" query directly
// against an .mbtiles file's own SQLite storage, without going through
// OpenMBTiles' "expose a whole zoom range as tinySQL tables" path.
//
// OpenMBTiles is the right tool for a coverage/integrity/bulk query -- it
// reads a zoom range once and gives back ordinary queryable rows -- but it
// has no way to ask for exactly one (z, x, y): the caller would have to open
// that tile's entire zoom level and filter client-side, which is fine for an
// overview zoom (a few thousand tiles) and disqualifying for a base zoom on
// a large tileset (millions). A single point lookup instead runs one
// SQLite query against the source's own (zoom_level, tile_column, tile_row)
// primary key, which every valid .mbtiles file already indexes -- exactly
// what a tile-serving request handler needs on every request.
import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite"
)

// LookupMBTilesTile reads one tile's raw bytes from filePath. z/x/y are in
// the ordinary XYZ/Slippy-map convention this codebase's TILE_* functions
// (tile_functions.go) already use throughout; the flip to the MBTiles
// specification's TMS row convention happens here, the same
// "(1<<z)-1-y" formula cmd/tinysqld/tiles.go already applies at its own
// request boundary.
//
// found is false, with a nil error, when the tileset simply does not cover
// that tile -- a normal, expected outcome for a sparse tileset, not a fault.
func LookupMBTilesTile(ctx context.Context, filePath string, z, x, y int) (data []byte, found bool, err error) {
	if z < 0 {
		return nil, false, fmt.Errorf("lookup mbtiles tile: zoom %d must not be negative", z)
	}
	tmsRow := (1 << uint(z)) - 1 - y

	dsn := "file:" + url.PathEscape(filePath) + "?mode=ro&immutable=1"
	src, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, false, fmt.Errorf("open mbtiles: %w", err)
	}
	defer src.Close()
	if err := src.PingContext(ctx); err != nil {
		return nil, false, fmt.Errorf("open mbtiles %s: %w", filePath, err)
	}

	row := src.QueryRowContext(ctx,
		"SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ? LIMIT 1",
		z, x, tmsRow)
	var tile []byte
	if err := row.Scan(&tile); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("lookup mbtiles tile: %w", err)
	}
	return tile, true, nil
}
