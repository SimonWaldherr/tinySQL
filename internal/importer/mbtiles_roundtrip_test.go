//go:build sqliteimport && !js && !wasm && !baremetal

package importer

// Round-trip and streaming tests for MBTiles support.
//
// The export path's whole purpose is producing a file other tools can read, so
// these tests inspect the written SQLite database directly — its schema, its
// indexes, its metadata keys and its tile bytes — rather than only checking that
// tinySQL can read back what it wrote. A self-consistent but non-conforming
// writer would pass the latter and fail every real consumer.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// seedMBTiles writes a small but structurally realistic tileset: several zoom
// levels, distinct tile payloads, and metadata.
func seedMBTiles(t *testing.T, path string, zooms []int) map[string][]byte {
	t.Helper()
	src, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer src.Close()
	ctx := context.Background()
	if _, err := src.ExecContext(ctx, `
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
		INSERT INTO metadata VALUES ('name', 'roundtrip');
		INSERT INTO metadata VALUES ('format', 'pbf');
		INSERT INTO metadata VALUES ('bounds', '-180.0,-85.0511,180.0,85.0511');`); err != nil {
		t.Fatalf("seed schema: %v", err)
	}

	want := map[string][]byte{}
	for _, z := range zooms {
		side := 1 << uint(z)
		for col := 0; col < side && col < 3; col++ {
			for row := 0; row < side && row < 3; row++ {
				// Payload encodes its own address so a mixed-up tile is obvious.
				payload := []byte(fmt.Sprintf("tile-%d-%d-%d", z, col, row))
				if _, err := src.ExecContext(ctx,
					`INSERT INTO tiles VALUES (?, ?, ?, ?)`, z, col, row, payload); err != nil {
					t.Fatalf("seed tile %d/%d/%d: %v", z, col, row, err)
				}
				want[fmt.Sprintf("%d/%d/%d", z, col, row)] = payload
			}
		}
	}
	return want
}

// TestMBTilesRoundTrip imports a tileset, exports it again, and verifies the
// exported file conforms to the specification and preserves every tile exactly.
func TestMBTilesRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.mbtiles")
	want := seedMBTiles(t, srcPath, []int{0, 1, 2})

	db := storage.NewDB()
	imp, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath, &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if imp.RowsInserted != int64(len(want)) {
		t.Fatalf("imported %d tiles, seeded %d", imp.RowsInserted, len(want))
	}

	outPath := filepath.Join(dir, "out.mbtiles")
	exp, err := ExportMBTiles(ctx, db, "default", outPath, &ExportMBTilesOptions{TileRowIsTMS: true})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if exp.TilesWritten != int64(len(want)) {
		t.Errorf("exported %d tiles, want %d", exp.TilesWritten, len(want))
	}
	if exp.MinZoom != 0 || exp.MaxZoom != 2 {
		t.Errorf("zoom range %d..%d, want 0..2", exp.MinZoom, exp.MaxZoom)
	}

	out, err := sql.Open("sqlite", outPath)
	if err != nil {
		t.Fatalf("open export: %v", err)
	}
	defer out.Close()

	// The specification's schema: both tables present, and the unique tile index
	// that makes a z/x/y lookup a seek and forbids duplicates.
	for _, want := range []struct{ typ, name string }{
		{"table", "tiles"},
		{"table", "metadata"},
		{"index", "tile_index"},
		{"index", "name"},
	} {
		var got string
		err := out.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type = ? AND name = ?`, want.typ, want.name).Scan(&got)
		if err != nil {
			t.Errorf("exported file has no %s named %q: %v", want.typ, want.name, err)
		}
	}

	// Every tile, byte for byte, at the same address.
	rows, err := out.QueryContext(ctx, `SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles`)
	if err != nil {
		t.Fatalf("query exported tiles: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var z, col, row int
		var data []byte
		if err := rows.Scan(&z, &col, &row, &data); err != nil {
			t.Fatalf("scan exported tile: %v", err)
		}
		key := fmt.Sprintf("%d/%d/%d", z, col, row)
		expect, ok := want[key]
		if !ok {
			t.Errorf("exported file contains unexpected tile %s", key)
			continue
		}
		if !bytes.Equal(data, expect) {
			t.Errorf("tile %s = %q, want %q", key, data, expect)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != len(want) {
		t.Errorf("exported file has %d tiles, want %d", seen, len(want))
	}

	// Metadata survives, including the source's own keys, and required keys exist.
	meta := map[string]string{}
	mrows, err := out.QueryContext(ctx, `SELECT name, value FROM metadata`)
	if err != nil {
		t.Fatalf("query exported metadata: %v", err)
	}
	defer mrows.Close()
	for mrows.Next() {
		var k, v string
		if err := mrows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		meta[k] = v
	}
	if meta["name"] != "roundtrip" {
		t.Errorf("metadata name = %q, want roundtrip", meta["name"])
	}
	if meta["format"] != "pbf" {
		t.Errorf("metadata format = %q, want pbf (the source's value must not be overwritten)", meta["format"])
	}
	if meta["minzoom"] != "0" || meta["maxzoom"] != "2" {
		t.Errorf("metadata zoom range = %q..%q, want 0..2", meta["minzoom"], meta["maxzoom"])
	}
}

// TestMBTilesExportRefusesExistingFile checks the export does not silently
// destroy a tileset that took hours to build.
func TestMBTilesExportRefusesExistingFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.mbtiles")
	seedMBTiles(t, srcPath, []int{0})

	db := storage.NewDB()
	if _, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath, &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("import: %v", err)
	}
	// Exporting over the source file must fail rather than truncate it.
	if _, err := ExportMBTiles(ctx, db, "default", srcPath, nil); err == nil {
		t.Fatal("export over an existing file succeeded; it should refuse")
	}
}

// TestMBTilesExportFlipsXYZRows checks the TileRowIsTMS=false path: a table
// holding XYZ rows must be written as TMS, because that is what the
// specification stores and what every consumer expects.
func TestMBTilesExportFlipsXYZRows(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	// Build a tileset by hand with XYZ rows: zoom 1, XYZ row 0 is the north.
	table := storage.NewTable("xyz_tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	table.Rows = [][]any{
		{1, 0, 0, []byte("xyz-north")},
		{1, 0, 1, []byte("xyz-south")},
	}
	table.Version++
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	outPath := filepath.Join(t.TempDir(), "flipped.mbtiles")
	if _, err := ExportMBTiles(ctx, db, "default", outPath, &ExportMBTilesOptions{
		TilesTable:   "xyz_tiles",
		TileRowIsTMS: false,
	}); err != nil {
		t.Fatalf("export: %v", err)
	}

	out, err := sql.Open("sqlite", outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	// XYZ row 0 (north) must land in TMS tile_row 1 at zoom 1.
	var data []byte
	if err := out.QueryRowContext(ctx,
		`SELECT tile_data FROM tiles WHERE zoom_level = 1 AND tile_column = 0 AND tile_row = 1`).Scan(&data); err != nil {
		t.Fatalf("north tile not at TMS row 1: %v", err)
	}
	if string(data) != "xyz-north" {
		t.Errorf("TMS row 1 holds %q, want xyz-north", data)
	}
	if err := out.QueryRowContext(ctx,
		`SELECT tile_data FROM tiles WHERE zoom_level = 1 AND tile_column = 0 AND tile_row = 0`).Scan(&data); err != nil {
		t.Fatalf("south tile not at TMS row 0: %v", err)
	}
	if string(data) != "xyz-south" {
		t.Errorf("TMS row 0 holds %q, want xyz-south", data)
	}
}

// TestMBTilesImportStreamsInBatches checks the streaming import inserts every
// tile when the tileset spans many batches, which is the case the batching exists
// for. A batch size of 1 maximises the number of flush boundaries.
func TestMBTilesImportStreamsInBatches(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "many.mbtiles")
	want := seedMBTiles(t, srcPath, []int{2, 3, 4})

	for _, batch := range []int{1, 2, 7, 1000} {
		t.Run(fmt.Sprintf("batch%d", batch), func(t *testing.T) {
			db := storage.NewDB()
			res, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath,
				&ImportOptions{CreateTable: true, BatchSize: batch})
			if err != nil {
				t.Fatalf("import: %v", err)
			}
			if res.RowsInserted != int64(len(want)) {
				t.Errorf("batch %d: inserted %d tiles, want %d", batch, res.RowsInserted, len(want))
			}
			table, err := db.Get("default", "tiles")
			if err != nil {
				t.Fatal(err)
			}
			if len(table.Rows) != len(want) {
				t.Errorf("batch %d: table holds %d rows, want %d", batch, len(table.Rows), len(want))
			}
			// Spot-check that payloads survived batching intact.
			for _, row := range table.Rows {
				key := fmt.Sprintf("%v/%v/%v", row[0], row[1], row[2])
				expect, ok := want[key]
				if !ok {
					t.Errorf("batch %d: unexpected tile %s", batch, key)
					continue
				}
				got, ok := row[3].([]byte)
				if !ok {
					t.Errorf("batch %d: tile %s data is %T, want []byte", batch, key, row[3])
					continue
				}
				if !bytes.Equal(got, expect) {
					t.Errorf("batch %d: tile %s = %q, want %q", batch, key, got, expect)
				}
			}
		})
	}
}

// TestMBTilesImportEmptyTilesetCreatesTable checks an empty tileset still yields
// a queryable table, so callers get zero rows rather than "table not found".
func TestMBTilesImportEmptyTilesetCreatesTable(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "empty.mbtiles")
	seedMBTiles(t, srcPath, nil)

	db := storage.NewDB()
	res, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath, &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.RowsInserted != 0 {
		t.Errorf("RowsInserted = %d, want 0", res.RowsInserted)
	}
	if _, err := db.Get("default", "tiles"); err != nil {
		t.Errorf("empty tileset did not create the tiles table: %v", err)
	}
}

// TestOpenMBTilesInPlace checks querying a tileset without copying it: zoom
// filtering and the index-only mode that makes a large tileset tractable.
func TestOpenMBTilesInPlace(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "big.mbtiles")
	want := seedMBTiles(t, srcPath, []int{0, 1, 2, 3})

	t.Run("all zooms", func(t *testing.T) {
		db := storage.NewDB()
		res, err := OpenMBTiles(ctx, db, "default", srcPath, nil)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if res.TilesExposed != int64(len(want)) {
			t.Errorf("exposed %d tiles, want %d", res.TilesExposed, len(want))
		}
		if res.MetadataExposed != 3 {
			t.Errorf("exposed %d metadata rows, want 3", res.MetadataExposed)
		}
	})

	t.Run("zoom subset", func(t *testing.T) {
		db := storage.NewDB()
		res, err := OpenMBTiles(ctx, db, "default", srcPath, &OpenMBTilesOptions{Zooms: []int{0, 1}})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		// Zoom 0 has 1 tile, zoom 1 has 4 (2x2, under the 3-per-axis cap).
		if res.TilesExposed != 5 {
			t.Errorf("zooms 0-1 exposed %d tiles, want 5", res.TilesExposed)
		}
		if res.MinZoom != 0 || res.MaxZoom != 1 {
			t.Errorf("zoom range %d..%d, want 0..1", res.MinZoom, res.MaxZoom)
		}
		table, err := db.Get("default", "tiles")
		if err != nil {
			t.Fatal(err)
		}
		zoomIdx, err := table.ColIndex("zoom_level")
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range table.Rows {
			z, _ := row[zoomIdx].(int)
			if z > 1 {
				t.Errorf("zoom %d leaked past the Zooms filter", z)
			}
		}
	})

	t.Run("index only", func(t *testing.T) {
		db := storage.NewDB()
		if _, err := OpenMBTiles(ctx, db, "default", srcPath, &OpenMBTilesOptions{WithoutTileData: true}); err != nil {
			t.Fatalf("open: %v", err)
		}
		table, err := db.Get("default", "tiles")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := table.ColIndex("tile_data"); err == nil {
			t.Error("WithoutTileData still produced a tile_data column")
		}
		// Size and hash must still be present: they are the point of an index.
		for _, col := range []string{"tile_size", "tile_sha256"} {
			if _, err := table.ColIndex(col); err != nil {
				t.Errorf("index-only open is missing %s", col)
			}
		}
		sizeIdx, _ := table.ColIndex("tile_size")
		for _, row := range table.Rows {
			if n, _ := row[sizeIdx].(int); n <= 0 {
				t.Errorf("tile_size = %v, want the real byte length", row[sizeIdx])
				break
			}
		}
	})

	t.Run("max tiles truncates", func(t *testing.T) {
		db := storage.NewDB()
		res, err := OpenMBTiles(ctx, db, "default", srcPath, &OpenMBTilesOptions{MaxTiles: 3})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if res.TilesExposed != 3 {
			t.Errorf("exposed %d tiles, want 3", res.TilesExposed)
		}
		if !res.Truncated {
			t.Error("Truncated should report that the view is partial")
		}
	})

	t.Run("source file untouched", func(t *testing.T) {
		// Opening read-only must not create sidecar files next to the tileset.
		db := storage.NewDB()
		if _, err := OpenMBTiles(ctx, db, "default", srcPath, nil); err != nil {
			t.Fatalf("open: %v", err)
		}
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if entries, _ := filepath.Glob(srcPath + suffix); len(entries) > 0 {
				t.Errorf("read-only open created %s next to the source tileset", suffix)
			}
		}
	})
}
