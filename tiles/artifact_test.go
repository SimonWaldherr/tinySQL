//go:build sqliteimport && !js && !wasm && !baremetal

package tiles_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/tiles"
	_ "modernc.org/sqlite"
)

func TestPublicArtifactReaderContract(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	if err := createFixture(source); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "region.ttiles")
	result, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{
		Schema:         tiles.SchemaFlat,
		BatchSize:      2,
		MaxMemoryBytes: 16 << 20,
		Provenance: map[string]any{
			"kind":   "fixture",
			"nested": map[string]any{"owner": "tinyTiles"},
		},
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if result.Info.APIVersion != tiles.APIVersion || result.Info.Schema != tiles.SchemaFlat || result.Info.CoordinateSystem != tiles.CoordinateTMS {
		t.Fatalf("unexpected result info: %#v", result.Info)
	}
	if result.Info.TileDigestSHA256 == "" || len(result.Info.Tables) != 2 {
		t.Fatalf("missing semantic manifest details: %#v", result.Info)
	}

	validated, err := tiles.ValidateArtifact(ctx, artifact)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if validated.TileDigestSHA256 != result.Info.TileDigestSHA256 {
		t.Fatalf("tile digest changed: import=%s validate=%s", result.Info.TileDigestSHA256, validated.TileDigestSHA256)
	}

	reader, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{MaxMemoryBytes: 4 << 20})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer reader.Close()

	firstInfo := reader.Info()
	firstInfo.Provenance["nested"].(map[string]any)["owner"] = "mutated"
	firstInfo.Tables[0].Columns[0] = "mutated"
	secondInfo := reader.Info()
	if got := secondInfo.Provenance["nested"].(map[string]any)["owner"]; got != "tinyTiles" {
		t.Fatalf("reader info provenance aliased: %v", got)
	}
	if secondInfo.Tables[0].Columns[0] != "name" {
		t.Fatalf("reader info table aliased: %#v", secondInfo.Tables[0])
	}

	value, found, err := reader.Metadata(ctx, "format")
	if err != nil || !found || value != "pbf" {
		t.Fatalf("metadata value=%q found=%t err=%v", value, found, err)
	}
	metadata := make(map[string]string)
	if err := reader.ScanMetadata(ctx, func(name, value string) error {
		metadata[name] = value
		return nil
	}); err != nil {
		t.Fatalf("scan metadata: %v", err)
	}
	if metadata["format"] != "pbf" || metadata["name"] == "" {
		t.Fatalf("scanned metadata=%#v", metadata)
	}
	metadataStop := errors.New("stop metadata scan")
	if err := reader.ScanMetadata(ctx, func(string, string) error { return metadataStop }); !errors.Is(err, metadataStop) {
		t.Fatalf("metadata callback error=%v, want sentinel", err)
	}
	tile, found, err := reader.Lookup(ctx, tiles.Key{Z: 2, X: 1, Y: 0})
	if err != nil || !found || !bytes.Equal(tile.Data, []byte{1, 1, 0}) {
		t.Fatalf("lookup tile=%#v found=%t err=%v", tile, found, err)
	}
	tile.Data[0] = 99
	again, found, err := reader.Lookup(ctx, tile.Key)
	if err != nil || !found || !bytes.Equal(again.Data, []byte{1, 1, 0}) {
		t.Fatalf("lookup payload aliased: tile=%#v found=%t err=%v", again, found, err)
	}

	var visited []tiles.Key
	if err := reader.Scan(ctx, tiles.Range{Z: 2, XMin: 0, XMax: 1, YMin: 0, YMax: 1}, func(tile tiles.Tile) error {
		visited = append(visited, tile.Key)
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := []tiles.Key{{Z: 2, X: 0, Y: 0}, {Z: 2, X: 0, Y: 1}, {Z: 2, X: 1, Y: 0}, {Z: 2, X: 1, Y: 1}}
	if len(visited) != len(want) {
		t.Fatalf("scan keys=%v, want %v", visited, want)
	}
	for i := range want {
		if visited[i] != want[i] {
			t.Fatalf("scan key %d=%v, want %v", i, visited[i], want[i])
		}
	}
	sentinel := errors.New("stop scan")
	if err := reader.Scan(ctx, tiles.Range{Z: 2, XMin: 0, XMax: 1, YMin: 0, YMax: 1}, func(tiles.Tile) error {
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("scan callback error=%v, want sentinel", err)
	}
}

func TestPublicReaderParallelAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source.mbtiles")
	if err := createFixture(source); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "region.ttiles")
	if _, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{Schema: tiles.SchemaFlat, BatchSize: 2, MaxMemoryBytes: 16 << 20}); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 3; restart++ {
		for _, readerCount := range []int{1, 4, 8} {
			var wg sync.WaitGroup
			errs := make(chan error, readerCount)
			for worker := 0; worker < readerCount; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					reader, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{MaxMemoryBytes: 2 << 20})
					if err != nil {
						errs <- err
						return
					}
					defer reader.Close()
					for n := 0; n < 16; n++ {
						key := tiles.Key{Z: 2, X: (worker + n) % 2, Y: (worker + n/2) % 2}
						tile, found, lookupErr := reader.Lookup(ctx, key)
						if lookupErr != nil || !found || len(tile.Data) != 3 || tile.Data[1] != byte(key.X) || tile.Data[2] != byte(key.Y) {
							errs <- errors.New("parallel public lookup did not preserve tile parity")
							return
						}
					}
					count := 0
					if scanErr := reader.Scan(ctx, tiles.Range{Z: 2, XMin: 0, XMax: 1, YMin: 0, YMax: 1}, func(tiles.Tile) error {
						count++
						return nil
					}); scanErr != nil || count != 4 {
						errs <- errors.New("parallel public range scan did not preserve tile parity")
					}
				}(worker)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("restart=%d readers=%d: %v", restart, readerCount, err)
			}
		}
	}
}

func BenchmarkPublicReaderLookup(b *testing.B) {
	ctx := context.Background()
	source := filepath.Join(b.TempDir(), "source.mbtiles")
	if err := createFixture(source); err != nil {
		b.Fatal(err)
	}
	artifact := filepath.Join(b.TempDir(), "region.ttiles")
	if _, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{Schema: tiles.SchemaFlat, BatchSize: 2, MaxMemoryBytes: 16 << 20}); err != nil {
		b.Fatal(err)
	}
	reader, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{MaxMemoryBytes: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	keys := []tiles.Key{{Z: 2, X: 0, Y: 0}, {Z: 2, X: 0, Y: 1}, {Z: 2, X: 1, Y: 0}, {Z: 2, X: 1, Y: 1}}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		tile, found, err := reader.Lookup(ctx, keys[n%len(keys)])
		if err != nil || !found || len(tile.Data) != 3 {
			b.Fatalf("lookup found=%t tile=%#v err=%v", found, tile, err)
		}
	}
}

func BenchmarkPublicReaderLookupFunc(b *testing.B) {
	ctx := context.Background()
	source := filepath.Join(b.TempDir(), "source.mbtiles")
	if err := createFixture(source); err != nil {
		b.Fatal(err)
	}
	artifact := filepath.Join(b.TempDir(), "region.ttiles")
	if _, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{Schema: tiles.SchemaFlat, BatchSize: 2, MaxMemoryBytes: 16 << 20}); err != nil {
		b.Fatal(err)
	}
	reader, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{MaxMemoryBytes: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = reader.Close() })
	keys := []tiles.Key{{Z: 2, X: 0, Y: 0}, {Z: 2, X: 0, Y: 1}, {Z: 2, X: 1, Y: 0}, {Z: 2, X: 1, Y: 1}}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		found, err := reader.LookupFunc(ctx, keys[n%len(keys)], func(tile tiles.Tile) error {
			if len(tile.Data) != 3 {
				return errors.New("unexpected tile payload")
			}
			return nil
		})
		if err != nil || !found {
			b.Fatalf("lookup func found=%t err=%v", found, err)
		}
	}
}

func createFixture(path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata VALUES ('format', 'pbf'), ('name', 'public API fixture');
	`); err != nil {
		return err
	}
	for x := 0; x < 2; x++ {
		for y := 0; y < 2; y++ {
			if _, err := db.Exec(`INSERT INTO tiles VALUES (?,?,?,?)`, 2, x, y, []byte{1, byte(x), byte(y)}); err != nil {
				return err
			}
		}
	}
	return nil
}
