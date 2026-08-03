//go:build sqliteimport && !js && !wasm && !baremetal

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// seedVectorMBTiles writes a small .mbtiles file shaped like tippecanoe's
// output: a pbf format declaration in metadata and a handful of tiles whose
// payload is opaque, gzip-magic-prefixed bytes -- standing in for real
// gzipped MVT data, which is unnecessary to fabricate correctly here since
// mbtilesregion (like tinySQL's tile server) treats tile_data as an opaque
// BLOB regardless of whether it decodes as PNG or protobuf.
func seedVectorMBTiles(t *testing.T, path string) map[string][]byte {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
		INSERT INTO metadata VALUES ('name', 'dingolfing-landau');
		INSERT INTO metadata VALUES ('format', 'pbf');
		INSERT INTO metadata VALUES ('bounds', '12.25,48.53,12.90,48.80');
	`); err != nil {
		t.Fatalf("seed schema: %v", err)
	}

	want := map[string][]byte{}
	for z := 0; z <= 3; z++ {
		side := 1 << uint(z)
		for x := 0; x < side && x < 3; x++ {
			for y := 0; y < side && y < 3; y++ {
				// 0x1f 0x8b is the gzip magic tinySQL's own tile server
				// checks for pbf tiles; the rest is arbitrary opaque payload.
				payload := append([]byte{0x1f, 0x8b}, []byte(byteFill(z, x, y))...)
				if _, err := db.ExecContext(ctx, `INSERT INTO tiles VALUES (?, ?, ?, ?)`, z, x, y, payload); err != nil {
					t.Fatalf("seed tile %d/%d/%d: %v", z, x, y, err)
				}
				want[key(z, x, y)] = payload
			}
		}
	}
	return want
}

func byteFill(z, x, y int) string {
	return "vector-tile-" + itoa(z) + "-" + itoa(x) + "-" + itoa(y)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func key(z, x, y int) string { return byteFill(z, x, y) }

// TestBuildBrowserDBRoundTripsOpaqueVectorTiles checks the full mbtilesregion
// path: import a vector-shaped .mbtiles, base64-encode every tile for the
// WASM bridge, save a snapshot, reload it, and confirm every tile and the
// metadata row survive byte-for-byte.
func TestBuildBrowserDBRoundTripsOpaqueVectorTiles(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.mbtiles")
	want := seedVectorMBTiles(t, srcPath)

	ctx := context.Background()
	src := storage.NewDB()
	importRes, err := importer.ImportMBTiles(ctx, src, "default", "tiles", srcPath, &importer.ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if int(importRes.RowsInserted) != len(want) {
		t.Fatalf("imported %d tiles, seeded %d", importRes.RowsInserted, len(want))
	}

	demo, err := buildBrowserDB(src, "tiles")
	if err != nil {
		t.Fatalf("buildBrowserDB: %v", err)
	}

	snapshot, err := tinysql.SaveToBytes(demo)
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(snapshot)

	// Simulate the browser side: decode the snapshot file's base64, load it
	// into a fresh DB, and read tiles back exactly as tiles-demo-bavaria.js
	// would via SELECT + atob().
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	reloaded, err := tinysql.LoadFromBytes(decoded)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	tiles, err := reloaded.Get("default", "tiles")
	if err != nil {
		t.Fatalf("get tiles: %v", err)
	}
	if len(tiles.Rows) != len(want) {
		t.Fatalf("reloaded %d tiles, want %d", len(tiles.Rows), len(want))
	}
	zoomIdx, _ := tiles.ColIndex("zoom_level")
	colIdx, _ := tiles.ColIndex("tile_column")
	rowIdx, _ := tiles.ColIndex("tile_row")
	dataIdx, _ := tiles.ColIndex("tile_data")
	for _, row := range tiles.Rows {
		z, _ := row[zoomIdx].(int)
		x, _ := row[colIdx].(int)
		y, _ := row[rowIdx].(int)
		b64, ok := row[dataIdx].(string)
		if !ok {
			t.Fatalf("tile %d/%d/%d: tile_data = %T, want string (base64 TEXT)", z, x, y, row[dataIdx])
		}
		got, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			t.Fatalf("tile %d/%d/%d: invalid base64: %v", z, x, y, err)
		}
		expect, ok := want[key(z, x, y)]
		if !ok {
			t.Fatalf("unexpected tile %d/%d/%d", z, x, y)
		}
		if !bytes.Equal(got, expect) {
			t.Fatalf("tile %d/%d/%d: got %d bytes, want %d", z, x, y, len(got), len(expect))
		}
		if got[0] != 0x1f || got[1] != 0x8b {
			t.Fatalf("tile %d/%d/%d: lost its gzip magic bytes through the base64 round trip", z, x, y)
		}
	}

	meta, err := reloaded.Get("default", "tiles_metadata")
	if err != nil {
		t.Fatalf("get tiles_metadata: %v", err)
	}
	found := false
	for _, row := range meta.Rows {
		if row[0] == "format" && row[1] == "pbf" {
			found = true
		}
	}
	if !found {
		t.Fatal("metadata lost the format=pbf row")
	}
}

// TestBuildBrowserDBMissingMetadataIsNotFatal checks a source .mbtiles with
// no metadata table (unusual, but not invalid) still produces a servable
// snapshot instead of erroring.
func TestBuildBrowserDBMissingMetadataIsNotFatal(t *testing.T) {
	src := storage.NewDB()
	tiles := storage.NewTable("tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	tiles.Rows = [][]any{{0, 0, 0, []byte{0x1f, 0x8b, 0x00}}}
	tiles.Version++
	if err := src.Put("default", tiles); err != nil {
		t.Fatal(err)
	}

	demo, err := buildBrowserDB(src, "tiles")
	if err != nil {
		t.Fatalf("buildBrowserDB without metadata: %v", err)
	}
	if _, err := demo.Get("default", "tiles"); err != nil {
		t.Fatalf("tiles table missing: %v", err)
	}
}
