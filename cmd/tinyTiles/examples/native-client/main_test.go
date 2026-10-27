package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Karte-Bayern/tinyTiles/offline"
)

func TestReadCachedTileUsesActiveRevision(t *testing.T) {
	ctx := context.Background()
	cache := t.TempDir()
	store, err := offline.NewFileStore(cache)
	if err != nil {
		t.Fatal(err)
	}
	manifest := offline.Manifest{FormatVersion: offline.ProtocolVersion, Dataset: "demo", Revision: "new", CoordinateSystem: "TMS", TileURLTemplate: "https://example.invalid/{z}/{x}/{y}"}
	key := offline.TileKey{Z: 2, X: 1, Y: 2}
	for revision, data := range map[string]string{"old": "stale", "new": "active tile"} {
		if err := store.PutTile(ctx, manifest.Dataset, revision, key, offline.Tile{Data: []byte(data), ContentEncoding: "gzip"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PutManifest(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "tile.bin")
	if err := readCachedTile(ctx, cache, "demo", "2/1/2", out); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil || string(got) != "active tile" {
		t.Fatalf("tile = %q, error = %v", got, err)
	}
	for _, tc := range []struct{ dataset, tile, want string }{
		{"missing", "2/1/2", "no active manifest"},
		{"demo", "2/1/3", "not cached"},
		{"demo", "2/1", "z/x/y"},
		{"demo", "2/1/a", "invalid tile coordinate"},
		{"demo", "2/4/0", "invalid TMS tile coordinate"},
	} {
		err := readCachedTile(ctx, cache, tc.dataset, tc.tile, "")
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("read %s %s: got %v, want %q", tc.dataset, tc.tile, err, tc.want)
		}
	}
}
