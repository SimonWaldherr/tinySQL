package main

// End-to-end: the tile endpoint serving from a disk-backed immutable page store.
//
// The in-memory tile tests prove the addressing and the HTTP contract. This one
// proves the deployment that actually matters for a tileset larger than RAM: an
// artifact built once, reopened read-only as ModePagedIndex, and served per
// record. A tile lookup is a complete composite equality predicate, which is the
// shape that resolves the B+Tree and materializes only the located row instead of
// decoding the whole table.

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestTileEndpointOverPagedIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tiles-artifact")

	// Build the artifact writable, exactly as a tile pipeline would.
	build, err := tinysql.OpenEnterprise(tinysql.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 32 << 20,
	}, "default")
	if err != nil {
		t.Fatal(err)
	}
	tiles := storage.NewTable("world", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	// TMS rows, as the MBTiles specification stores them.
	tiles.Rows = [][]any{
		{1, 0, 1, []byte("north-west")},
		{1, 1, 1, []byte("north-east")},
		{1, 0, 0, []byte("south-west")},
		{1, 1, 0, []byte("south-east")},
	}
	if err := tiles.CreateSecondaryIndex("tile_index",
		[]string{"zoom_level", "tile_column", "tile_row"}, true); err != nil {
		t.Fatal(err)
	}
	meta := storage.NewTable("world_metadata", []storage.Column{
		{Name: "name", Type: storage.TextType},
		{Name: "value", Type: storage.TextType},
	}, false)
	meta.Rows = [][]any{{"name", "paged"}, {"format", "png"}, {"minzoom", "1"}, {"maxzoom", "1"}}
	for _, tbl := range []*storage.Table{tiles, meta} {
		if err := build.DB.Put("default", tbl); err != nil {
			t.Fatal(err)
		}
	}
	if err := build.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen read-only, the serving configuration.
	serve, err := tinysql.OpenEnterprise(tinysql.StorageConfig{
		Mode:           storage.ModePagedIndex,
		Path:           dir,
		MaxMemoryBytes: 32 << 20,
		ReadOnly:       true,
	}, "default")
	if err != nil {
		t.Fatalf("reopen read-only: %v", err)
	}
	defer serve.Close()

	h := newDaemon(serve, daemonConfig{DefaultTenant: "default", Tiles: true}).routes()

	// The XYZ-to-TMS flip must still hold over the paged path.
	for _, tc := range []struct{ path, want string }{
		{"/tiles/world/1/0/0.png", "north-west"},
		{"/tiles/world/1/1/0.png", "north-east"},
		{"/tiles/world/1/0/1.png", "south-west"},
		{"/tiles/world/1/1/1.png", "south-east"},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200 (%s)", tc.path, rec.Code, rec.Body.String())
			continue
		}
		if got := rec.Body.String(); got != tc.want {
			t.Errorf("%s served %q, want %q", tc.path, got, tc.want)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
			t.Errorf("%s Content-Type = %q, want image/png from the metadata format", tc.path, ct)
		}
	}

	// A tile outside the tileset is still a clean 404 rather than an error.
	req := httptest.NewRequest(http.MethodGet, "/tiles/world/5/9/9.png", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("absent tile over paged index: status %d, want 404", rec.Code)
	}
}
