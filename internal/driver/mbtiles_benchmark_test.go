package driver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// BenchmarkReadOnlyMBTilesLikeTwoPointReads is a reproducible database/sql
// benchmark for the canonical MBTiles access shape: z/x/y -> tile_id followed
// by tile_id -> compressed payload. It intentionally builds an ordinary
// ModeIndex artifact first, then measures a fresh read-only Connector so the
// setup is not accidentally measured as an in-memory benchmark.
//
// Run, for example:
//
//	go test ./internal/driver -run '^$' -bench ReadOnlyMBTilesLike -benchmem -count 3
func BenchmarkReadOnlyMBTilesLikeTwoPointReads(b *testing.B) {
	const (
		rows      = 10_000
		blobBytes = 1024
	)
	dir := filepath.Join(b.TempDir(), "mbtiles-like")
	buildMBTilesLikeIndexArtifact(b, dir, rows, blobBytes)

	dsn := "file:" + dir + "?mode=index&read_only=1&max_memory_bytes=32MiB&pool_readers=8"
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	if err := db.Ping(); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	mapStmt, err := db.PrepareContext(ctx, `SELECT tile_id FROM map WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?`)
	if err != nil {
		b.Fatal(err)
	}
	defer mapStmt.Close()
	imageStmt, err := db.PrepareContext(ctx, `SELECT tile_data FROM images WHERE tile_id = ?`)
	if err != nil {
		b.Fatal(err)
	}
	defer imageStmt.Close()

	// Warm both tables once. This reports the steady-state point-read path;
	// cold-open/cold-table measurements belong in a separate benchmark/test.
	var warmID string
	if err := mapStmt.QueryRowContext(ctx, 12, 0, 0).Scan(&warmID); err != nil {
		b.Fatal(err)
	}
	var warmBlob []byte
	if err := imageStmt.QueryRowContext(ctx, warmID).Scan(&warmBlob); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(blobBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := i % rows
		var tileID string
		if err := mapStmt.QueryRowContext(ctx, 12, n, 0).Scan(&tileID); err != nil {
			b.Fatal(err)
		}
		var tile []byte
		if err := imageStmt.QueryRowContext(ctx, tileID).Scan(&tile); err != nil {
			b.Fatal(err)
		}
		if len(tile) != blobBytes {
			b.Fatalf("tile length = %d, want %d", len(tile), blobBytes)
		}
	}
}

func buildMBTilesLikeIndexArtifact(b *testing.B, dir string, rows, blobBytes int) {
	b.Helper()
	db, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModeIndex,
		Path:           dir,
		MaxMemoryBytes: 64 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	mapTable := storage.NewTable("map", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_id", Type: storage.TextType},
	}, false)
	images := storage.NewTable("images", []storage.Column{
		{Name: "tile_id", Type: storage.TextType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("12/%d/0", i)
		payload := make([]byte, blobBytes)
		payload[0] = byte(i)
		payload[len(payload)-1] = byte(i >> 8)
		mapTable.Rows = append(mapTable.Rows, []any{12, i, 0, id})
		images.Rows = append(images.Rows, []any{id, payload})
	}
	if err := mapTable.CreateSecondaryIndex("idx_map_zxy", []string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		b.Fatal(err)
	}
	if err := images.CreateSecondaryIndex("idx_images_tile_id", []string{"tile_id"}, true); err != nil {
		b.Fatal(err)
	}
	if err := db.Put("default", mapTable); err != nil {
		b.Fatal(err)
	}
	if err := db.Put("default", images); err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}
