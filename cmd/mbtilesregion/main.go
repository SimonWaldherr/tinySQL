//go:build sqliteimport && !js && !wasm && !baremetal

// Command mbtilesregion imports a real, already-built .mbtiles tileset (a
// vector tileset produced by tippecanoe, or any other spec-compliant
// .mbtiles file) and writes a browser-loadable tinySQL snapshot next to it --
// the same shape cmd/mbtilesdemo produces for its synthetic tileset, but for
// a tileset this tool did not generate itself.
//
// It exists so a CI job can turn "a real regional .mbtiles file" into
// "something tiles-demo-bavaria.html can fetch and importDatabase()"
// without duplicating cmd/mbtilesdemo's rendering pipeline, which is
// specific to its generated art and unrelated to importing existing data.
//
// Run:
//
//	go run -tags=sqliteimport ./cmd/mbtilesregion \
//	    -in dingolfing-landau.mbtiles \
//	    -out artifacts/ \
//	    -table tiles
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mbtilesregion:", err)
		os.Exit(1)
	}
}

func run() error {
	inPath := flag.String("in", "", "path to a spec-compliant .mbtiles file to import (required)")
	outDir := flag.String("out", "artifacts", "directory to write the browser snapshot into")
	table := flag.String("table", "tiles", "tinySQL table name for the imported tiles (also names the {table}_metadata table)")
	snapshotName := flag.String("snapshot-name", "snapshot.b64", "output filename for the base64 tinySQL snapshot")
	flag.Parse()

	if *inPath == "" {
		flag.Usage()
		return fmt.Errorf("-in is required")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", *outDir, err)
	}

	ctx := context.Background()
	src := storage.NewDB()
	importRes, err := importer.ImportMBTiles(ctx, src, "default", *table, *inPath, &importer.ImportOptions{CreateTable: true})
	if err != nil {
		return fmt.Errorf("import %s: %w", *inPath, err)
	}
	fmt.Printf("imported %d tiles from %s into table %q\n", importRes.RowsInserted, *inPath, *table)

	demo, err := buildBrowserDB(src, *table)
	if err != nil {
		return fmt.Errorf("build browser snapshot: %w", err)
	}

	snapshot, err := tinysql.SaveToBytes(demo)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	snapshotPath := filepath.Join(*outDir, *snapshotName)
	encoded := base64.StdEncoding.EncodeToString(snapshot)
	if err := os.WriteFile(snapshotPath, []byte(encoded), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", snapshotPath, err)
	}
	fmt.Printf("wrote %s (%d bytes base64, %d bytes decoded)\n", snapshotPath, len(encoded), len(snapshot))
	return nil
}

// buildBrowserDB re-encodes tile_data as base64 TEXT instead of BLOB. The
// WASM bridge (cmd/query_files_wasm/main.go) round-trips every []byte value
// through Go's string(v) conversion before it reaches JSON, which corrupts
// non-UTF-8 bytes -- true of PNG tiles (see the identical comment in
// cmd/mbtilesdemo/main.go) and just as true of gzip-compressed MVT/pbf
// vector tiles. Encoding to base64 once here, at build time, sidesteps the
// bridge entirely: the browser calls atob() on a TEXT column like any other
// query result, the same way the synthetic demo already does.
func buildBrowserDB(src *storage.DB, table string) (*storage.DB, error) {
	tiles, err := src.Get("default", table)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", table, err)
	}
	zoomIdx, err := tiles.ColIndex("zoom_level")
	if err != nil {
		return nil, err
	}
	colIdx, err := tiles.ColIndex("tile_column")
	if err != nil {
		return nil, err
	}
	rowIdx, err := tiles.ColIndex("tile_row")
	if err != nil {
		return nil, err
	}
	dataIdx, err := tiles.ColIndex("tile_data")
	if err != nil {
		return nil, fmt.Errorf("%s has no tile_data column: %w", table, err)
	}

	demo := storage.NewDB()
	out := storage.NewTable(table, []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.TextType},
	}, false)
	for _, row := range tiles.Rows {
		raw, ok := row[dataIdx].([]byte)
		if !ok || len(raw) == 0 {
			return nil, fmt.Errorf("tile %v/%v/%v has no tile_data blob", row[zoomIdx], row[colIdx], row[rowIdx])
		}
		out.Rows = append(out.Rows, []any{row[zoomIdx], row[colIdx], row[rowIdx], base64.StdEncoding.EncodeToString(raw)})
	}
	out.Version++
	if err := out.CreateSecondaryIndex(table+"_index", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		return nil, fmt.Errorf("index %s: %w", table, err)
	}
	if err := demo.Put("default", out); err != nil {
		return nil, err
	}

	// A missing metadata table (a source without one) is not fatal: the
	// tileset is still servable, just without TileJSON-style attribution.
	if meta, err := src.Get("default", table+"_metadata"); err == nil {
		meta.Version++
		if err := demo.Put("default", meta); err != nil {
			return nil, err
		}
	}
	return demo, nil
}
