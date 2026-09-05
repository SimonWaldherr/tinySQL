//go:build !js || !wasm

package offline

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileStorePersistsTilesAndManifests(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cache")
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("persisted")
	key := TileKey{Z: 2, X: 1, Y: 2}
	tile := checkedTile([]byte{1, 2, 3, 4})
	if err := store.PutTile(context.Background(), manifest.Dataset, manifest.Revision, key, tile); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	gotManifest, found, err := reopened.GetManifest(context.Background(), manifest.Dataset)
	if err != nil || !found || gotManifest.Revision != manifest.Revision {
		t.Fatalf("manifest=%#v found=%t err=%v", gotManifest, found, err)
	}
	gotTile, found, err := reopened.GetTile(context.Background(), manifest.Dataset, manifest.Revision, key)
	if err != nil || !found || string(gotTile.Data) != string(tile.Data) || gotTile.Checksum != tile.Checksum {
		t.Fatalf("tile=%#v found=%t err=%v", gotTile, found, err)
	}
	if err := reopened.DeleteRevision(context.Background(), manifest.Dataset, manifest.Revision); err != nil {
		t.Fatal(err)
	}
	if _, found, err := reopened.GetTile(context.Background(), manifest.Dataset, manifest.Revision, key); err != nil || found {
		t.Fatalf("deleted revision returned found=%t err=%v", found, err)
	}
}

func TestFileStoreRejectsCorruptRecord(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("corrupt")
	key := TileKey{Z: 1, X: 0, Y: 0}
	if err := store.PutTile(context.Background(), manifest.Dataset, manifest.Revision, key, checkedTile([]byte("valid"))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.tilePath(manifest.Dataset, manifest.Revision, key), []byte("broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetTile(context.Background(), manifest.Dataset, manifest.Revision, key); err == nil {
		t.Fatal("corrupt record accepted")
	}
}

func TestFileStoreRejectsUnsafeRootAndImpossibleLimit(t *testing.T) {
	if _, err := NewFileStore("."); err == nil {
		t.Fatal("current directory accepted as a cache root")
	}
	root := filepath.VolumeName(filepath.Clean(t.TempDir())) + string(os.PathSeparator)
	if _, err := NewFileStore(root); err == nil {
		t.Fatal("filesystem root accepted as a cache root")
	}
	store, err := NewFileStore(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetMaxTileSize(int64(^uint64(0) >> 1)); err == nil {
		t.Fatal("overflowing maximum tile size accepted")
	}
}

func TestFileStoreConcurrentReadersAndWriters(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := TileKey{Z: 4, X: 3, Y: 2}
	if err := store.PutTile(ctx, "demo", "r1", key, checkedTile([]byte("initial"))); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		writer := writer
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 20; i++ {
				payload := []byte{byte(writer), byte(i)}
				if err := store.PutTile(ctx, "demo", "r1", key, checkedTile(payload)); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}()
	}
	for reader := 0; reader < 4; reader++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 40; i++ {
				tile, found, err := store.GetTile(ctx, "demo", "r1", key)
				if err != nil || !found || len(tile.Data) == 0 {
					t.Errorf("read found=%t bytes=%d err=%v", found, len(tile.Data), err)
					return
				}
			}
		}()
	}
	group.Wait()
}
