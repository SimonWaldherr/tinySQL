package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestPagedIndexRegionalMBTilesReplaceDeleteInsert is the end-to-end
// regression, at the real SQL/engine layer, for a reported regional-MBTiles
// import failure:
//
//	insert row 10784: split right insert: btree page full: need 1569, have 1536 free
//
// against a tileset shaped exactly like the report: 6,646 map rows, 11,465
// images rows, 32-character tile_id, and 409 BLOBs in the reported
// 1,400-2,500 byte critical band (the rest a long tail into overflow-page
// territory, like real raster tiles). The root cause was a count-balanced
// B+Tree leaf split; see internal/storage/pager/btree.go
// (leafSplitIndex/internalSplitIndex/leafEntryNeedsOverflow) and the
// lower-level regression coverage in
// internal/storage/pager/btree_split_regression_test.go.
//
// This test additionally runs a real UPDATE/DELETE/INSERT sequence against
// overflow-sized BLOBs through Execute (not just a bulk import), then
// verifies every surviving map -> images secondary-index chain, byte for
// byte, after a durable close and read-only reopen.
func TestPagedIndexRegionalMBTilesReplaceDeleteInsert(t *testing.T) {
	const mapRows = 6646
	const imageRows = 11465
	const criticalCount = 409

	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "regional")
	db, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 32 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, sql := range []string{
		`CREATE TABLE map (zoom_level INT, tile_column INT, tile_row INT, tile_id TEXT)`,
		`CREATE TABLE images (tile_id TEXT, tile_data BLOB)`,
		`CREATE TABLE metadata (name TEXT, value TEXT)`,
		`CREATE UNIQUE INDEX idx_map_zxy ON map(zoom_level, tile_column, tile_row)`,
		`CREATE UNIQUE INDEX idx_images_id ON images(tile_id)`,
		`CREATE UNIQUE INDEX idx_metadata_name ON metadata(name)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParsePagedSQL(t, sql)); err != nil {
			t.Fatalf("build %q: %v", sql, err)
		}
	}

	tileID := func(i int) string { return fmt.Sprintf("%032x", uint64(i)*0x9e3779b97f4a7c15+1) }
	payload := func(i int) []byte {
		var size int
		if i < criticalCount {
			size = 1_400 + (i*37)%1_101 // 1,400..2,500: the reported critical band
		} else {
			size = 2_600 + (i*997)%30_000 // long tail, like real raster tiles
		}
		b := make([]byte, size)
		for j := range b {
			b[j] = byte(i*29 + j*13)
		}
		return b
	}

	images, err := db.Get("default", "images")
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string][]byte, imageRows)
	for i := 0; i < imageRows; i++ {
		id := tileID(i)
		data := payload(i)
		images.Rows = append(images.Rows, []any{id, data})
		want[id] = data
	}
	images.Version++
	if err := images.RebuildSecondaryIndexes(); err != nil {
		t.Fatal(err)
	}

	mapTable, err := db.Get("default", "map")
	if err != nil {
		t.Fatal(err)
	}
	type mapRow struct {
		z, x, y int
		id      string
	}
	mapWant := make([]mapRow, 0, mapRows)
	for i := 0; i < mapRows; i++ {
		z, x, y := i%12, (i*37)%4096, (i*73)%4096
		id := tileID(i) // every map row references a distinct existing image
		mapTable.Rows = append(mapTable.Rows, []any{z, x, y, id})
		mapWant = append(mapWant, mapRow{z, x, y, id})
	}
	mapTable.Version++
	if err := mapTable.RebuildSecondaryIndexes(); err != nil {
		t.Fatal(err)
	}

	metadata, err := db.Get("default", "metadata")
	if err != nil {
		t.Fatal(err)
	}
	metadata.Rows = [][]any{{"name", "regional regression"}, {"format", "png"}}
	metadata.Version++
	if err := metadata.RebuildSecondaryIndexes(); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(); err != nil {
		t.Fatalf("initial sync (%d map / %d images rows): %v", mapRows, imageRows, err)
	}

	// A real UPDATE/DELETE/INSERT sequence against overflow BLOBs through the
	// SQL engine must not resurface "btree page full".
	grownID := tileID(0)
	grown := bytes.Repeat([]byte{0x7a}, 120_000)
	if _, err := Execute(ctx, db, "default", mustParsePagedSQL(t,
		fmt.Sprintf(`UPDATE images SET tile_data = X'%s' WHERE tile_id = '%s'`, hex.EncodeToString(grown), grownID))); err != nil {
		t.Fatalf("UPDATE overflow tile_data: %v", err)
	}
	want[grownID] = grown

	deletedID := tileID(1)
	if _, err := Execute(ctx, db, "default", mustParsePagedSQL(t,
		fmt.Sprintf(`DELETE FROM images WHERE tile_id = '%s'`, deletedID))); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	delete(want, deletedID)

	insertedID := "fedcba9876543210fedcba9876543210"[:32]
	inserted := bytes.Repeat([]byte{0x11}, 1_569) // the literal size named in the bug report
	if _, err := Execute(ctx, db, "default", mustParsePagedSQL(t,
		fmt.Sprintf(`INSERT INTO images VALUES ('%s', X'%s')`, insertedID, hex.EncodeToString(inserted)))); err != nil {
		t.Fatalf("INSERT overflow tile_data: %v", err)
	}
	want[insertedID] = inserted

	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := storage.OpenDB(storage.StorageConfig{
		Mode: storage.ModePagedIndex, Path: dir, ReadOnly: true, MaxMemoryBytes: 8 << 20,
	})
	if err != nil {
		t.Fatalf("read-only reopen: %v", err)
	}
	defer reader.Close()

	// Full map -> images secondary-index-seek chain, byte for byte, for every
	// surviving map row (the deleted tile_id's map row is left dangling on
	// purpose, matching what a real DELETE FROM images alone would do; the
	// point here is the images side, not referential cleanup).
	for _, row := range mapWant {
		mapRS, err := Execute(ctx, reader, "default", mustParsePagedSQL(t, fmt.Sprintf(
			`SELECT tile_id FROM map WHERE zoom_level = %d AND tile_column = %d AND tile_row = %d`, row.z, row.x, row.y)))
		if err != nil || len(mapRS.Rows) != 1 {
			t.Fatalf("map %d/%d/%d: rows=%d err=%v", row.z, row.x, row.y, len(mapRS.Rows), err)
		}
		gotID, _ := mapRS.Rows[0]["tile_id"].(string)
		if gotID != row.id {
			t.Fatalf("map %d/%d/%d id=%q want %q", row.z, row.x, row.y, gotID, row.id)
		}
		if gotID == deletedID {
			continue
		}
		imgRS, err := Execute(ctx, reader, "default", mustParsePagedSQL(t,
			fmt.Sprintf(`SELECT tile_data FROM images WHERE tile_id = '%s'`, gotID)))
		if err != nil || len(imgRS.Rows) != 1 {
			t.Fatalf("image %q: rows=%d err=%v", gotID, len(imgRS.Rows), err)
		}
		got, ok := imgRS.Rows[0]["tile_data"].([]byte)
		if !ok || !bytes.Equal(got, want[gotID]) {
			t.Fatalf("image %q differs: got %T/%d bytes want %d", gotID, imgRS.Rows[0]["tile_data"], len(got), len(want[gotID]))
		}
	}

	// The deleted row must be gone; the inserted one must be present and correct.
	deletedRS, err := Execute(ctx, reader, "default", mustParsePagedSQL(t,
		fmt.Sprintf(`SELECT tile_data FROM images WHERE tile_id = '%s'`, deletedID)))
	if err != nil || len(deletedRS.Rows) != 0 {
		t.Fatalf("deleted tile_id %q still present after reopen: rows=%d err=%v", deletedID, len(deletedRS.Rows), err)
	}
	insertedRS, err := Execute(ctx, reader, "default", mustParsePagedSQL(t,
		fmt.Sprintf(`SELECT tile_data FROM images WHERE tile_id = '%s'`, insertedID)))
	if err != nil || len(insertedRS.Rows) != 1 {
		t.Fatalf("inserted row: rows=%d err=%v", len(insertedRS.Rows), err)
	}
	if got, _ := insertedRS.Rows[0]["tile_data"].([]byte); !bytes.Equal(got, inserted) {
		t.Fatalf("inserted tile_data mismatch: %d bytes want %d", len(got), len(inserted))
	}

	// The grown (updated) tile: compare by hash too, cheap insurance against
	// a truncation bug that happens to preserve length.
	grownRS, err := Execute(ctx, reader, "default", mustParsePagedSQL(t,
		fmt.Sprintf(`SELECT tile_data FROM images WHERE tile_id = '%s'`, grownID)))
	if err != nil || len(grownRS.Rows) != 1 {
		t.Fatalf("grown row: rows=%d err=%v", len(grownRS.Rows), err)
	}
	got, _ := grownRS.Rows[0]["tile_data"].([]byte)
	wantSum, gotSum := sha256.Sum256(grown), sha256.Sum256(got)
	if wantSum != gotSum {
		t.Fatalf("grown tile_data hash mismatch after reopen: %d bytes, want %d", len(got), len(grown))
	}

	// A point seek on a unique secondary index must never materialize the
	// whole table.
	if stats := reader.BackendStats(); stats.LoadCount != 0 {
		t.Fatalf("read-only seek path materialized a table: LoadCount=%d", stats.LoadCount)
	}

	// Negative lookup: a well-formed but absent tile_id.
	noHit, err := Execute(ctx, reader, "default", mustParsePagedSQL(t,
		`SELECT tile_data FROM images WHERE tile_id = '00000000000000000000000000000000'`))
	if err != nil || len(noHit.Rows) != 0 {
		t.Fatalf("negative lookup: rows=%d err=%v", len(noHit.Rows), err)
	}
}
