package engine

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestPagedIndexCompositeSeekReadsOnlyLocatedRows exercises the vertical
// MBTiles-shaped path after a close/reopen. The executor must select the
// persistent index before it considers storage.DB.Get, otherwise Get would
// materialize the whole images table and BackendStats.LoadCount would rise.
func TestPagedIndexCompositeSeekReadsOnlyLocatedRows(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "tiles")
	builder, err := storage.OpenDB(storage.StorageConfig{
		Mode: storage.ModePagedIndex,
		Path: dir,
		// Imports keep dirty pages resident until the final checkpoint. Serving
		// below uses a two-page read cache, which is the bound under test.
		MaxMemoryBytes: 2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE TABLE map (zoom_level INT, tile_column INT, tile_row INT, tile_id TEXT)`,
		`CREATE TABLE images (tile_id TEXT, tile_data BLOB)`,
		`INSERT INTO map VALUES (12, 2174, 1423, '12/2174/1423')`,
		`CREATE UNIQUE INDEX idx_map_zxy ON map(zoom_level, tile_column, tile_row)`,
		`CREATE UNIQUE INDEX idx_images_tile_id ON images(tile_id)`,
	} {
		if _, err := Execute(ctx, builder, "default", mustParsePagedSQL(t, sql)); err != nil {
			t.Fatalf("build %q: %v", sql, err)
		}
	}
	want := bytes.Repeat([]byte{0x5a}, 96*1024) // forces B+Tree overflow pages
	images, err := builder.Get("default", "images")
	if err != nil {
		t.Fatal(err)
	}
	images.Rows = append(images.Rows, []any{"12/2174/1423", want})
	images.Version++
	if err := images.RebuildSecondaryIndexes(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := storage.OpenDB(storage.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 16 * 1024,
		ReadOnly:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	rs, err := Execute(ctx, reader, "default", mustParsePagedSQL(t, `SELECT tile_id FROM map WHERE zoom_level = 12 AND tile_column = 2174 AND tile_row = 1423`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["tile_id"] != "12/2174/1423" {
		t.Fatalf("map point lookup = %#v", rs.Rows)
	}
	rs, err = Execute(ctx, reader, "default", mustParsePagedSQL(t, `SELECT tile_data FROM images WHERE tile_id = '12/2174/1423'`))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := rs.Rows[0]["tile_data"].([]byte)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("BLOB mismatch: got %T/%d bytes", rs.Rows[0]["tile_data"], len(got))
	}
	noHit, err := Execute(ctx, reader, "default", mustParsePagedSQL(t, `SELECT tile_data FROM images WHERE tile_id = 'missing'`))
	if err != nil {
		t.Fatal(err)
	}
	if len(noHit.Rows) != 0 {
		t.Fatalf("negative paged index lookup = %#v", noHit.Rows)
	}
	if stats := reader.BackendStats(); stats.LoadCount != 0 {
		t.Fatalf("paged point reads called LoadTable %d times", stats.LoadCount)
	}

	explain, err := Execute(ctx, reader, "default", mustParsePagedSQL(t, `EXPLAIN SELECT tile_id FROM map WHERE zoom_level = 12 AND tile_column = 2174 AND tile_row = 1423`))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range explain.Rows {
		if row["operation"] == "PAGED INDEX POINT SEEK" && strings.Contains(row["detail"].(string), "idx_map_zxy") {
			found = true
		}
	}
	if !found {
		t.Fatalf("EXPLAIN = %#v", explain.Rows)
	}

	// Each query receives a clone of cached metadata. This catches the former
	// race where the planner attached candidates to a shared schema Table.
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 16 {
				result, err := Execute(ctx, reader, "default", mustParsePagedSQL(t, `SELECT tile_data FROM images WHERE tile_id = '12/2174/1423'`))
				if err != nil {
					errs <- err
					return
				}
				if len(result.Rows) != 1 || !bytes.Equal(result.Rows[0]["tile_data"].([]byte), want) {
					errs <- fmt.Errorf("concurrent paged BLOB lookup = %#v", result.Rows)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	stats := reader.BackendStats()
	if stats.CachedPages > stats.MaxCachePages {
		t.Fatalf("read-only pager cache exceeded limit: %#v", stats)
	}
	if stats.TransientFrames != 0 {
		t.Fatalf("query-local pages leaked after concurrent lookups: %#v", stats)
	}
}

func TestPagedIndexMixedNumericEqualityFallsBackToLoadedTable(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "numeric")
	builder, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE TABLE nums (v FLOAT64)`,
		`INSERT INTO nums VALUES (1)`,
		`CREATE INDEX idx_nums_v ON nums(v)`,
	} {
		if _, err := Execute(ctx, builder, "default", mustParsePagedSQL(t, sql)); err != nil {
			t.Fatalf("build %q: %v", sql, err)
		}
	}
	if err := builder.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	rs, err := Execute(ctx, reader, "default", mustParsePagedSQL(t, `SELECT v FROM nums WHERE v = 1.0`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["v"]) != 1 {
		t.Fatalf("numeric equality after paged reopen = %#v", rs.Rows)
	}
}

func mustParsePagedSQL(t *testing.T, sql string) Statement {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return stmt
}
