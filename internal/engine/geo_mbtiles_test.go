//go:build sqliteimport && !js && !wasm && !baremetal

package engine

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// newTestMBTilesFile builds a tiny, spec-shaped .mbtiles SQLite file (the
// same construction internal/importer's own mbtiles_test.go uses) with two
// tiles at zoom 1 (TMS rows 0 and 1) and one metadata row.
func newTestMBTilesFile(t *testing.T) string {
	t.Helper()
	fn := filepath.Join(t.TempDir(), "test.mbtiles")
	src, err := sql.Open("sqlite", fn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer src.Close()
	_, err = src.ExecContext(context.Background(), `
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata VALUES ('name', 'demo'), ('format', 'png');
		INSERT INTO tiles VALUES (1, 0, 0, x'aabbcc');
		INSERT INTO tiles VALUES (1, 0, 1, x'ddeeff');
	`)
	if err != nil {
		t.Fatalf("seed mbtiles: %v", err)
	}
	return fn
}

func TestMBTilesTileLookup(t *testing.T) {
	db := storage.NewDB()
	fn := newTestMBTilesFile(t)

	// TMS row 0 at z1 is XYZ y=1 ((1<<1)-1-0 = 1).
	rs := execSQL(t, db, `SELECT MBTILES_TILE('`+filepath.ToSlash(fn)+`', 1, 0, 1) AS v`)
	data, ok := rs.Rows[0]["v"].([]byte)
	if !ok || len(data) != 3 || data[0] != 0xaa {
		t.Errorf("MBTILES_TILE(z1,x0,y1): got %v (%T)", rs.Rows[0]["v"], rs.Rows[0]["v"])
	}

	// A tile the tileset does not cover is NULL, not an error.
	rs2 := execSQL(t, db, `SELECT MBTILES_TILE('`+filepath.ToSlash(fn)+`', 5, 5, 5) AS v`)
	if rs2.Rows[0]["v"] != nil {
		t.Errorf("missing tile should be NULL, got %v", rs2.Rows[0]["v"])
	}
}

func TestMBTilesTilesTableFunc(t *testing.T) {
	db := storage.NewDB()
	fn := newTestMBTilesFile(t)

	rs := execSQL(t, db, `SELECT * FROM MBTILES_TILES('`+filepath.ToSlash(fn)+`')`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 tile rows, got %d: %v", len(rs.Rows), rs.Rows)
	}
	for _, row := range rs.Rows {
		if row["zoom_level"] != 1 {
			t.Errorf("zoom_level: got %v, want 1", row["zoom_level"])
		}
		if _, ok := row["tile_data"].([]byte); !ok {
			t.Errorf("tile_data: got %T, want []byte", row["tile_data"])
		}
	}
}

func TestMBTilesTilesZoomRangeFilter(t *testing.T) {
	db := storage.NewDB()
	fn := newTestMBTilesFile(t)

	rs := execSQL(t, db, `SELECT * FROM MBTILES_TILES('`+filepath.ToSlash(fn)+`', 2, 5)`)
	if len(rs.Rows) != 0 {
		t.Errorf("zoom range 2..5 should exclude the fixture's only z1 tiles, got %d rows", len(rs.Rows))
	}
	rs2 := execSQL(t, db, `SELECT * FROM MBTILES_TILES('`+filepath.ToSlash(fn)+`', 0, 1)`)
	if len(rs2.Rows) != 2 {
		t.Errorf("zoom range 0..1 should include both z1 tiles, got %d rows", len(rs2.Rows))
	}
}

func TestMBTilesMetadataTableFunc(t *testing.T) {
	db := storage.NewDB()
	fn := newTestMBTilesFile(t)

	rs := execSQL(t, db, `SELECT * FROM MBTILES_METADATA('`+filepath.ToSlash(fn)+`') ORDER BY name`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 metadata rows, got %d: %v", len(rs.Rows), rs.Rows)
	}
	if rs.Rows[0]["name"] != "format" || rs.Rows[0]["value"] != "png" {
		t.Errorf("row 0: got %v", rs.Rows[0])
	}
	if rs.Rows[1]["name"] != "name" || rs.Rows[1]["value"] != "demo" {
		t.Errorf("row 1: got %v", rs.Rows[1])
	}
}

func TestMBTilesMetadataNoMetadataTable(t *testing.T) {
	db := storage.NewDB()
	fn := filepath.Join(t.TempDir(), "tilesonly.mbtiles")
	src, err := sql.Open("sqlite", fn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := src.ExecContext(context.Background(), `CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	src.Close()

	rs := execSQL(t, db, `SELECT * FROM MBTILES_METADATA('`+filepath.ToSlash(fn)+`')`)
	if len(rs.Rows) != 0 {
		t.Errorf("a tiles-only file should report empty metadata, not an error: got %d rows", len(rs.Rows))
	}
}
