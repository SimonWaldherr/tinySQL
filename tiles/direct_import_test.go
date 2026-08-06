//go:build !js && !wasm && !baremetal

package tiles_test

import (
	"context"
	"path/filepath"
	"testing"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

func TestImportTilesWithoutSQLiteBuildTag(t *testing.T) {
	ctx := context.Background()
	artifact := filepath.Join(t.TempDir(), "direct.ttiles")
	source := directTestSource{tiles: []tiles.Tile{{Key: tiles.Key{Z: 0, X: 0, Y: 0}, Data: []byte("tile")}}}

	result, err := tiles.ImportTiles(ctx, source, artifact, &tiles.ImportOptions{BatchSize: 1, MaxMemoryBytes: 8 << 20})
	if err != nil {
		t.Fatalf("direct import: %v", err)
	}
	if result.Info.Schema != tiles.SchemaFlat || result.Info.Source != "direct.fixture" {
		t.Fatalf("unexpected artifact info: %#v", result.Info)
	}

	reader, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{})
	if err != nil {
		t.Fatalf("open direct artifact: %v", err)
	}
	defer reader.Close()
	got, found, err := reader.Lookup(ctx, source.tiles[0].Key)
	if err != nil || !found || string(got.Data) != "tile" {
		t.Fatalf("lookup: data=%q found=%t err=%v", got.Data, found, err)
	}
}

type directTestSource struct{ tiles []tiles.Tile }

func (s directTestSource) Info(context.Context) (tiles.SourceInfo, error) {
	var bytes, max int64
	for _, tile := range s.tiles {
		size := int64(len(tile.Data))
		bytes += size
		if size > max {
			max = size
		}
	}
	// Keep metadata nil to cover the empty-table cache path.
	return tiles.SourceInfo{Name: "direct.fixture", SourceBytes: bytes, TileCount: int64(len(s.tiles)), TileBytes: bytes, MaxTileBytes: max}, nil
}

func (s directTestSource) ScanTiles(ctx context.Context, visit func(tiles.Tile) error) error {
	for _, tile := range s.tiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(tile); err != nil {
			return err
		}
	}
	return nil
}
