package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestPagedIndexLargeMBTilesProjectionRoundTrip exercises the physical layout
// used by MBTiles: a map z/x/y -> tile_id index and a separate image payload
// index. The payload distribution covers the inline/overflow boundary that
// previously made a count-balanced B+Tree leaf split overfill one side.
func TestPagedIndexLargeMBTilesProjectionRoundTrip(t *testing.T) {
	const tiles = 5_120 // 10,240 rows across map and images
	dir := t.TempDir()

	// A read-only open must only accept a published pager artifact and must not
	// create a directory, database file or WAL sidecar for a missing target.
	unpublished := filepath.Join(dir, "unpublished")
	if _, err := NewPagedIndexBackend(unpublished, 2<<20, true); err == nil {
		t.Fatal("read-only open accepted an unpublished artifact")
	}
	if _, err := os.Stat(filepath.Join(unpublished, pagedIndexFilename)); !os.IsNotExist(err) {
		t.Fatalf("read-only open created unpublished artifact: %v", err)
	}

	writer, err := NewPagedIndexBackend(dir, 4<<20, false)
	if err != nil {
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

	for i := 0; i < tiles; i++ {
		z, x, y := i%9, (i*37)%8_192, (i*73)%8_192
		tileID := fmt.Sprintf("tile-%05d-%08x", i, i*0x9e3779b1)
		mapTable.Rows = append(mapTable.Rows, []any{z, x, y, tileID})
		imagesTable.Rows = append(imagesTable.Rows, []any{tileID, pagedIndexTilePayload(i)})
	}
	metadataTable.Rows = [][]any{
		{"name", "bayern regression fixture"},
		{"format", "pbf"},
		{"minzoom", "0"},
		{"maxzoom", "8"},
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

	artifact := filepath.Join(dir, pagedIndexFilename)
	info, err := os.Stat(artifact)
	if err != nil || info.Size() == 0 {
		t.Fatalf("published artifact missing or empty: info=%v err=%v", info, err)
	}

	reader, err := NewPagedIndexBackend(dir, 2<<20, true)
	if err != nil {
		t.Fatalf("read-only reopen: %v", err)
	}
	defer reader.Close()
	for _, name := range []string{"map", "images", "metadata"} {
		metadata, err := reader.IndexMetadata("default", name)
		if err != nil {
			t.Fatalf("index metadata %s: %v", name, err)
		}
		if metadata == nil || len(metadata.Rows) != 0 {
			t.Fatalf("metadata for %s materialized rows", name)
		}
	}
	if reader.Stats().LoadCount != 0 {
		t.Fatalf("metadata lookup materialized a table: LoadCount=%d", reader.Stats().LoadCount)
	}
	imageLocator, found, err := reader.LocateIndex("default", "images", "images_tile_id")
	if err != nil || !found {
		t.Fatalf("locate image index: found=%v err=%v", found, err)
	}
	present, err := imageLocator.ContainsUnique(CanonicalIndexKey([]any{"tile-00000-00000000"}))
	if err != nil || !present {
		t.Fatalf("contains existing image: present=%v err=%v", present, err)
	}
	present, err = imageLocator.ContainsUnique(CanonicalIndexKey([]any{"missing"}))
	if err != nil || present {
		t.Fatalf("contains missing image: present=%v err=%v", present, err)
	}

	// Full map -> tile_id -> tile_data parity through on-disk secondary-index
	// seeks. These lookups never call LoadTable and remain bounded by the pager
	// cache rather than retaining either table's BLOBs.
	for i := 0; i < tiles; i++ {
		z, x, y := i%9, (i*37)%8_192, (i*73)%8_192
		tileID := fmt.Sprintf("tile-%05d-%08x", i, i*0x9e3779b1)
		mapRows, handled, err := reader.LookupIndexRows("default", "map", "map_zxy", []any{z, x, y})
		if err != nil || !handled || len(mapRows) != 1 {
			t.Fatalf("map lookup %d: rows=%d handled=%v err=%v", i, len(mapRows), handled, err)
		}
		if got, ok := mapRows[0][3].(string); !ok || got != tileID {
			t.Fatalf("map lookup %d tile_id=%#v want %q", i, mapRows[0][3], tileID)
		}
		imageRows, handled, err := reader.LookupIndexRows("default", "images", "images_tile_id", []any{tileID})
		if err != nil || !handled || len(imageRows) != 1 {
			t.Fatalf("image lookup %d: rows=%d handled=%v err=%v", i, len(imageRows), handled, err)
		}
		got, ok := imageRows[0][1].([]byte)
		if !ok || !bytes.Equal(got, pagedIndexTilePayload(i)) {
			t.Fatalf("image lookup %d BLOB differs", i)
		}
	}
	metadataRows, handled, err := reader.LookupIndexRows("default", "metadata", "metadata_name", []any{"format"})
	if err != nil || !handled || len(metadataRows) != 1 || metadataRows[0][1] != "pbf" {
		t.Fatalf("metadata lookup: rows=%v handled=%v err=%v", metadataRows, handled, err)
	}
	if reader.Stats().LoadCount != 0 {
		t.Fatalf("on-disk index lookups materialized a table: LoadCount=%d", reader.Stats().LoadCount)
	}
	stats := reader.Stats()
	if stats.CachedPages > stats.MaxCachePages {
		t.Fatalf("pager cache exceeded its bound: cached=%d max=%d", stats.CachedPages, stats.MaxCachePages)
	}

	if err := reader.SaveTable("default", NewTable("writes_are_rejected", nil, false)); !errors.Is(err, ErrReadOnlyStorage) {
		t.Fatalf("read-only SaveTable error=%v, want ErrReadOnlyStorage", err)
	}
	if err := reader.DeleteTable("default", "map"); !errors.Is(err, ErrReadOnlyStorage) {
		t.Fatalf("read-only DeleteTable error=%v, want ErrReadOnlyStorage", err)
	}
}

func pagedIndexTilePayload(i int) []byte {
	var size int
	switch i % 8 {
	case 0:
		size = 48
	case 7:
		size = 9_000 + i%997
	default:
		size = 1_400 + (i*113)%1_101
	}
	payload := make([]byte, size)
	for j := range payload {
		payload[j] = byte(i*29 + j*13)
	}
	return payload
}
