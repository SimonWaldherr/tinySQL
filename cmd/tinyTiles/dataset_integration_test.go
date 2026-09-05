//go:build sqliteimport

package tinytiles

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
	_ "modernc.org/sqlite"
)

func testDataset(t *testing.T) *Dataset {
	t.Helper()
	ctx := t.Context()
	source := filepath.Join(t.TempDir(), "fixture.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata VALUES
			('name', 'fixture'), ('format', 'pbf'), ('kb:content_encoding', 'gzip'),
			('minzoom', '2'), ('maxzoom', '2'), ('bounds', '10,20,30,40');
		INSERT INTO tiles VALUES (2, 1, 2, x'010203');`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "fixture.ttiles")
	if _, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{Schema: tiles.SchemaFlat, BatchSize: 1, MinFreeBytes: 0}); err != nil {
		t.Fatal(err)
	}
	dataset, err := Open(context.Background(), artifact, OpenOptions{Readers: 2, MaxMemoryBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataset.Close() })
	return dataset
}

func TestOpenReadsMetadataAndXYZTile(t *testing.T) {
	dataset := testDataset(t)
	metadata, err := dataset.Metadata()
	if err != nil || metadata["name"] != "fixture" || metadata["format"] != "pbf" {
		t.Fatalf("Metadata = %#v, %v", metadata, err)
	}
	tile, found, err := dataset.LookupXYZ(t.Context(), 2, 1, 1)
	if err != nil || !found || string(tile.Data) != string([]byte{1, 2, 3}) || tile.Key.Y != 2 {
		t.Fatalf("LookupXYZ = %#v, %v, %v", tile, found, err)
	}
}
