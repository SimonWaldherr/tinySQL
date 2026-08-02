package storage

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"

	_ "modernc.org/sqlite"
)

// TestPagedIndexMBTilesSourceSample is an opt-in integration test for a real
// MBTiles file. It never modifies the source: it copies a deterministic sample
// of map/images rows to a temporary paged-index artifact, then verifies the
// full map(z/x/y) -> tile_id -> tile_data chain after a read-only reopen.
//
// Example:
//
// TINYSQL_MBTILES_PATH=/path/to/bayern.mbtiles \
// TINYSQL_MBTILES_ROWS=10240 \
// go test ./internal/storage -run TestPagedIndexMBTilesSourceSample -count=1
func TestPagedIndexMBTilesSourceSample(t *testing.T) {
	sourcePath := os.Getenv("TINYSQL_MBTILES_PATH")
	if sourcePath == "" {
		t.Skip("set TINYSQL_MBTILES_PATH to run against a real MBTiles source")
	}
	rows := 10_240
	if raw := os.Getenv("TINYSQL_MBTILES_ROWS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid TINYSQL_MBTILES_ROWS %q", raw)
		}
		rows = parsed
	}

	dsn := "file:" + url.PathEscape(sourcePath) + "?mode=ro&immutable=1"
	source, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Ping(); err != nil {
		t.Fatal(err)
	}

	mapTable := NewTable("map", []Column{
		{Name: "zoom_level", Type: IntType},
		{Name: "tile_column", Type: IntType},
		{Name: "tile_row", Type: IntType},
		{Name: "tile_id", Type: TextType},
	}, false)
	imagesTable := NewTable("images", []Column{
		{Name: "tile_id", Type: TextType},
		{Name: "tile_data", Type: BlobType},
	}, false)
	metadataTable := NewTable("metadata", []Column{
		{Name: "name", Type: TextType},
		{Name: "value", Type: TextType},
	}, false)
	type expectedTile struct {
		z, x, y int
		tileID  string
		data    []byte
	}
	expected := make([]expectedTile, 0, rows)

	mapRows, err := source.Query(`SELECT zoom_level, tile_column, tile_row, tile_id FROM map ORDER BY zoom_level, tile_column, tile_row LIMIT ?`, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer mapRows.Close()
	imageByID, err := source.Prepare(`SELECT tile_data FROM images WHERE tile_id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer imageByID.Close()
	seenImages := make(map[string]struct{}, rows)
	for mapRows.Next() {
		var tile expectedTile
		if err := mapRows.Scan(&tile.z, &tile.x, &tile.y, &tile.tileID); err != nil {
			t.Fatal(err)
		}
		if err := imageByID.QueryRow(tile.tileID).Scan(&tile.data); err != nil {
			t.Fatalf("read image %q: %v", tile.tileID, err)
		}
		tile.data = append([]byte(nil), tile.data...)
		mapTable.Rows = append(mapTable.Rows, []any{tile.z, tile.x, tile.y, tile.tileID})
		if _, duplicate := seenImages[tile.tileID]; !duplicate {
			imagesTable.Rows = append(imagesTable.Rows, []any{tile.tileID, tile.data})
			seenImages[tile.tileID] = struct{}{}
		}
		expected = append(expected, tile)
	}
	if err := mapRows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(expected) != rows {
		t.Fatalf("source returned %d map rows, want %d", len(expected), rows)
	}
	metadataRows, err := source.Query(`SELECT name, value FROM metadata ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	expectedMetadata := make(map[string]string)
	for metadataRows.Next() {
		var name, value string
		if err := metadataRows.Scan(&name, &value); err != nil {
			_ = metadataRows.Close()
			t.Fatal(err)
		}
		metadataTable.Rows = append(metadataTable.Rows, []any{name, value})
		expectedMetadata[name] = value
	}
	if err := metadataRows.Err(); err != nil {
		_ = metadataRows.Close()
		t.Fatal(err)
	}
	if err := metadataRows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mapTable.CreateSecondaryIndex("map_zxy", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		t.Fatal(err)
	}
	if err := imagesTable.CreateSecondaryIndex("images_tile_id", []string{"tile_id"}, true); err != nil {
		t.Fatal(err)
	}
	if err := metadataTable.CreateSecondaryIndex("metadata_name", []string{"name"}, true); err != nil {
		t.Fatal(err)
	}

	artifactDir := t.TempDir()
	writer, err := NewPagedIndexBackend(artifactDir, 32<<20, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []*Table{mapTable, imagesTable, metadataTable} {
		table.Version = 1
		if err := writer.SaveTable("default", table); err != nil {
			_ = writer.Close()
			t.Fatalf("persist %s: %v", table.Name, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewPagedIndexBackend(artifactDir, 8<<20, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, tile := range expected {
		mapResult, handled, err := reader.LookupIndexRows("default", "map", "map_zxy", []any{tile.z, tile.x, tile.y})
		if err != nil || !handled || len(mapResult) != 1 {
			t.Fatalf("map %d/%d/%d: rows=%d handled=%v err=%v", tile.z, tile.x, tile.y, len(mapResult), handled, err)
		}
		gotID, _ := mapResult[0][3].(string)
		if gotID != tile.tileID {
			t.Fatalf("map %d/%d/%d id=%q want %q", tile.z, tile.x, tile.y, gotID, tile.tileID)
		}
		imageResult, handled, err := reader.LookupIndexRows("default", "images", "images_tile_id", []any{gotID})
		if err != nil || !handled || len(imageResult) != 1 {
			t.Fatalf("image %q: rows=%d handled=%v err=%v", gotID, len(imageResult), handled, err)
		}
		got, _ := imageResult[0][1].([]byte)
		if !bytes.Equal(got, tile.data) {
			t.Fatalf("image %q differs: got %d bytes want %d", gotID, len(got), len(tile.data))
		}
	}
	for name, want := range expectedMetadata {
		rows, handled, err := reader.LookupIndexRows("default", "metadata", "metadata_name", []any{name})
		if err != nil || !handled || len(rows) != 1 {
			t.Fatalf("metadata %q: rows=%d handled=%v err=%v", name, len(rows), handled, err)
		}
		got, _ := rows[0][1].(string)
		if got != want {
			t.Fatalf("metadata %q=%q want %q", name, got, want)
		}
	}
	if reader.Stats().LoadCount != 0 {
		t.Fatalf("read-only seek path materialized a table: LoadCount=%d", reader.Stats().LoadCount)
	}
	fmt.Fprintf(os.Stderr, "paged-index real-source sample verified: %d map rows, %d images\n", len(expected), len(imagesTable.Rows))
}
