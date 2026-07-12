//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestImportMBTiles(t *testing.T) {
	ctx := context.Background()
	fn := filepath.Join(t.TempDir(), "test.mbtiles")

	src, err := sql.Open("sqlite", fn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := src.ExecContext(ctx, `CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata VALUES ('name', 'demo');
		INSERT INTO tiles VALUES (1, 2, 3, x'010203');`); err != nil {
		t.Fatalf("seed mbtiles: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	db := storage.NewDB()
	res, err := ImportFile(ctx, db, "default", "tiles", fn, &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportFile MBTiles: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d, want 1", res.RowsInserted)
	}

	tbl, err := db.Get("default", "tiles")
	if err != nil {
		t.Fatalf("get tiles: %v", err)
	}
	if tbl.Rows[0][4] != 3 {
		t.Fatalf("tile_size = %v, want 3", tbl.Rows[0][4])
	}
	hash := sha256.Sum256([]byte{1, 2, 3})
	if tbl.Rows[0][5] != fmt.Sprintf("%x", hash) {
		t.Fatalf("tile_sha256 = %v, want %x", tbl.Rows[0][5], hash)
	}
	if tbl.Rows[0][6] != base64.StdEncoding.EncodeToString([]byte{1, 2, 3}) {
		t.Fatalf("tile_preview_base64 = %v, want AQID", tbl.Rows[0][6])
	}
	meta, err := db.Get("default", "tiles_metadata")
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if len(meta.Rows) != 1 || meta.Rows[0][0] != "name" || meta.Rows[0][1] != "demo" {
		t.Fatalf("unexpected metadata rows: %#v", meta.Rows)
	}

	body, err := os.ReadFile(fn)
	if err != nil {
		t.Fatalf("read mbtiles: %v", err)
	}
	db2 := storage.NewDB()
	res, err = ImportMBTilesReader(ctx, db2, "default", "tiles_reader", bytes.NewReader(body), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportMBTilesReader: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("reader RowsInserted = %d, want 1", res.RowsInserted)
	}
}
