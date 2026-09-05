package tinytiles

import (
	"context"
	"errors"
	"testing"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

type memoryReader struct {
	tiles map[tiles.Key][]byte
}

func (r *memoryReader) Close() error { return nil }

func (r *memoryReader) Info() tiles.ArtifactInfo { return tiles.ArtifactInfo{} }

func (r *memoryReader) Metadata(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (r *memoryReader) Lookup(_ context.Context, key tiles.Key) (tiles.Tile, bool, error) {
	value, found := r.tiles[key]
	if !found {
		return tiles.Tile{}, false, nil
	}
	return tiles.Tile{Key: key, Data: append([]byte(nil), value...)}, true, nil
}

func (r *memoryReader) LookupFunc(ctx context.Context, key tiles.Key, fn func(tiles.Tile) error) (bool, error) {
	tile, found, err := r.Lookup(ctx, key)
	if err != nil || !found {
		return found, err
	}
	return true, fn(tile)
}

func (r *memoryReader) Scan(_ context.Context, tileRange tiles.Range, fn func(tiles.Tile) error) error {
	for key, value := range r.tiles {
		if key.Z == tileRange.Z && key.X >= tileRange.XMin && key.X <= tileRange.XMax && key.Y >= tileRange.YMin && key.Y <= tileRange.YMax {
			if err := fn(tiles.Tile{Key: key, Data: append([]byte(nil), value...)}); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestDatasetXYZAdapterAndMetadataCopies(t *testing.T) {
	reader := &memoryReader{tiles: map[tiles.Key][]byte{{Z: 2, X: 1, Y: 2}: {1, 2, 3}}}
	dataset := &Dataset{metadata: map[string]string{"name": "fixture"}, readers: make(chan tiles.Reader, 1), done: make(chan struct{})}
	dataset.readers <- reader

	tile, found, err := dataset.LookupXYZ(t.Context(), 2, 1, 1)
	if err != nil || !found || tile.Key != (tiles.Key{Z: 2, X: 1, Y: 2}) {
		t.Fatalf("LookupXYZ = %#v, %v, %v", tile, found, err)
	}
	data, err := dataset.GetTileXYZ(2, 1, 1)
	if err != nil || string(data) != string([]byte{1, 2, 3}) {
		t.Fatalf("GetTileXYZ = %v, %v", data, err)
	}
	if _, err := dataset.GetTileXYZ(2, 1, 0); !errors.Is(err, ErrTileNotFound) {
		t.Fatalf("missing GetTileXYZ error = %v, want ErrTileNotFound", err)
	}
	metadata, err := dataset.Metadata()
	if err != nil || metadata["name"] != "fixture" {
		t.Fatalf("Metadata = %#v, %v", metadata, err)
	}
	metadata["name"] = "mutated"
	metadata, err = dataset.Metadata()
	if err != nil || metadata["name"] != "fixture" {
		t.Fatalf("Metadata mutation leaked: %#v, %v", metadata, err)
	}
	if got, err := XYZToTMSY(2, 1); err != nil || got != 2 {
		t.Fatalf("XYZToTMSY = %d, %v", got, err)
	}
	if got, err := TMSToXYZY(2, 2); err != nil || got != 1 {
		t.Fatalf("TMSToXYZY = %d, %v", got, err)
	}
	if err := dataset.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dataset.LookupTMS(t.Context(), tiles.Key{Z: 2, X: 1, Y: 2}); !errors.Is(err, ErrClosed) {
		t.Fatalf("lookup after Close = %v, want ErrClosed", err)
	}
}

func TestDatasetCloseUnblocksWaitingLookup(t *testing.T) {
	reader := &memoryReader{tiles: map[tiles.Key][]byte{}}
	dataset := &Dataset{readers: make(chan tiles.Reader, 1), done: make(chan struct{})}
	dataset.readers <- reader
	borrowed, err := dataset.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := dataset.LookupTMS(context.Background(), tiles.Key{Z: 0, X: 0, Y: 0})
		done <- err
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- dataset.Close() }()
	dataset.release(borrowed)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("waiting lookup error = %v, want ErrClosed", err)
	}
}
