//go:build sqliteimport && !js && !wasm && !baremetal

package importer

// Tests for the ModePagedIndex fast import path (storage.DB.AppendRowsFast,
// wired up in insertTypedRows). The scenario these guard against: importing a
// tileset larger than memory. ImportMBTiles already streamed its *source*
// scan in bounded batches; what it did not do -- before AppendRowsFast --
// was avoid pinning the *destination* table's rows in memory for the whole
// import, regardless of storage mode. These tests exercise many small
// batches against a real PagedIndexBackend so that regressing back to the
// db.Get-once-then-append-forever path would show up as either an error (the
// table isn't in the DB's cache to find) or, if it silently still worked,
// as a correctness failure after a close/reopen forces a real disk read.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// seedMBTilesN writes n synthetic, uniquely-addressed tiles to a fresh
// source .mbtiles in one transaction. Addresses are not a valid tile
// pyramid (row/col just count up); ImportMBTiles does not care, and it
// keeps this fast and simple for row counts seedMBTiles's 3x3-per-zoom cap
// cannot reach.
func seedMBTilesN(t *testing.T, path string, n int) {
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
		INSERT INTO metadata VALUES ('name', 'paged-import-test');`); err != nil {
		t.Fatalf("seed schema: %v", err)
	}
	tx, err := src.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO tiles VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	const zoom = 14
	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf("tile-%06d", i))
		if _, err := stmt.ExecContext(ctx, zoom, i, 0, payload); err != nil {
			t.Fatalf("seed tile %d: %v", i, err)
		}
	}
	if err := stmt.Close(); err != nil {
		t.Fatalf("close stmt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// openPagedIndexDB opens (or reopens) a ModePagedIndex database at dir.
func openPagedIndexDB(t *testing.T, dir string, readOnly bool) *storage.DB {
	t.Helper()
	db, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 8 << 20, // deliberately small: forces many batches to actually exercise persistence, not just fit in one.
		ReadOnly:       readOnly,
	})
	if err != nil {
		t.Fatalf("open paged index db (readOnly=%v): %v", readOnly, err)
	}
	return db
}

// TestImportMBTilesPagedIndexWithUniqueIndex is the primary scenario: a
// tiles table with the README-documented unique composite index, imported in
// many small batches. AppendRowsFast must maintain that index incrementally
// so a lookup after reopening finds every tile through it, not just via a
// full scan.
func TestImportMBTilesPagedIndexWithUniqueIndex(t *testing.T) {
	ctx := context.Background()
	const n = 12_000
	const batchSize = 250 // -> 48 batches, all but the first must use the fast path

	srcPath := filepath.Join(t.TempDir(), "src.mbtiles")
	seedMBTilesN(t, srcPath, n)

	dir := filepath.Join(t.TempDir(), "dst")
	db := openPagedIndexDB(t, dir, false)

	tiles := storage.NewTable("tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	if err := tiles.CreateSecondaryIndex("tile_index", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		t.Fatalf("declare index: %v", err)
	}
	if err := db.Put("default", tiles); err != nil {
		t.Fatalf("create table: %v", err)
	}

	res, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath, &ImportOptions{
		CreateTable: false, // table+index already exist; this is the fast-path-eligible shape
		BatchSize:   batchSize,
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.RowsInserted != n {
		t.Fatalf("RowsInserted = %d, want %d", res.RowsInserted, n)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ro := openPagedIndexDB(t, dir, true)
	defer ro.Close()

	// Sample across the range rather than every row: the point is that the
	// index -- built entirely through incremental appends -- resolves
	// correctly at the start, middle and end of an import that spanned many
	// AppendRows calls.
	for _, i := range []int{0, 1, batchSize - 1, batchSize, n / 2, n - 2, n - 1} {
		rows, ok, err := ro.PagedIndexRows("default", "tiles", "tile_index", []any{14, i, 0})
		if err != nil {
			t.Fatalf("lookup tile %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("lookup tile %d: no tile_index found", i)
		}
		if len(rows) != 1 {
			t.Fatalf("lookup tile %d: got %d rows, want 1", i, len(rows))
		}
		want := fmt.Sprintf("tile-%06d", i)
		got, ok := rows[0][3].([]byte)
		if !ok || string(got) != want {
			t.Fatalf("lookup tile %d: data = %v, want %q", i, rows[0][3], want)
		}
	}

	table, err := ro.Get("default", "tiles")
	if err != nil {
		t.Fatalf("get tiles: %v", err)
	}
	if len(table.Rows) != n {
		t.Fatalf("full scan: %d rows, want %d", len(table.Rows), n)
	}
}

// TestImportMBTilesPagedIndexWithoutIndex covers the common default: nothing
// calls CREATE INDEX, so AppendRowsFast has no unique index to maintain. It
// must still append every row correctly -- an empty index-keys map is a
// valid, ordinary case, not a degraded one.
func TestImportMBTilesPagedIndexWithoutIndex(t *testing.T) {
	ctx := context.Background()
	const n = 5_000

	srcPath := filepath.Join(t.TempDir(), "src.mbtiles")
	seedMBTilesN(t, srcPath, n)

	dir := filepath.Join(t.TempDir(), "dst")
	db := openPagedIndexDB(t, dir, false)

	res, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath, &ImportOptions{
		CreateTable: true,
		BatchSize:   333,
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.RowsInserted != n {
		t.Fatalf("RowsInserted = %d, want %d", res.RowsInserted, n)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ro := openPagedIndexDB(t, dir, true)
	defer ro.Close()
	table, err := ro.Get("default", "tiles")
	if err != nil {
		t.Fatalf("get tiles: %v", err)
	}
	if len(table.Rows) != n {
		t.Fatalf("full scan: %d rows, want %d", len(table.Rows), n)
	}

	meta, err := ro.Get("default", "tiles_metadata")
	if err != nil {
		t.Fatalf("get tiles_metadata: %v", err)
	}
	if len(meta.Rows) == 0 {
		t.Fatalf("tiles_metadata is empty")
	}
}

// TestExportMBTilesStreamsFromPagedIndex is ImportMBTiles's test mirrored for
// the write direction: ExportMBTiles reading a ModePagedIndex source must go
// through db.ScanRowsFast (mbtilesWriteTiles), not db.Get, or this would
// silently pass while regressing the whole point -- exporting a tileset
// larger than memory should not require loading it into memory first. A
// correctness check (every tile survives export byte-for-byte) is what would
// actually catch a broken streaming implementation; boundedness itself isn't
// something a unit test can assert directly.
func TestExportMBTilesStreamsFromPagedIndex(t *testing.T) {
	ctx := context.Background()
	const n = 8_000
	const importBatch = 400

	srcPath := filepath.Join(t.TempDir(), "src.mbtiles")
	seedMBTilesN(t, srcPath, n)

	dir := filepath.Join(t.TempDir(), "mid")
	db := openPagedIndexDB(t, dir, false)
	tiles := storage.NewTable("tiles", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	if err := tiles.CreateSecondaryIndex("tile_index", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		t.Fatalf("declare index: %v", err)
	}
	if err := db.Put("default", tiles); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ImportMBTiles(ctx, db, "default", "tiles", srcPath, &ImportOptions{
		CreateTable: false,
		BatchSize:   importBatch,
	}); err != nil {
		t.Fatalf("import: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "out.mbtiles")
	exportRes, err := ExportMBTiles(ctx, db, "default", outPath, &ExportMBTilesOptions{
		TileRowIsTMS: true,
		BatchSize:    importBatch,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close mid db: %v", err)
	}
	if exportRes.TilesWritten != n {
		t.Fatalf("TilesWritten = %d, want %d", exportRes.TilesWritten, n)
	}

	out, err := sql.Open("sqlite", outPath)
	if err != nil {
		t.Fatalf("open exported mbtiles: %v", err)
	}
	defer out.Close()
	var count int
	if err := out.QueryRowContext(ctx, `SELECT COUNT(*) FROM tiles`).Scan(&count); err != nil {
		t.Fatalf("count exported tiles: %v", err)
	}
	if count != n {
		t.Fatalf("exported file has %d tiles, want %d", count, n)
	}
	for _, i := range []int{0, importBatch - 1, importBatch, n / 2, n - 1} {
		var data []byte
		if err := out.QueryRowContext(ctx,
			`SELECT tile_data FROM tiles WHERE zoom_level = 14 AND tile_column = ? AND tile_row = 0`, i,
		).Scan(&data); err != nil {
			t.Fatalf("read exported tile %d: %v", i, err)
		}
		want := fmt.Sprintf("tile-%06d", i)
		if string(data) != want {
			t.Fatalf("exported tile %d = %q, want %q", i, data, want)
		}
	}
}
