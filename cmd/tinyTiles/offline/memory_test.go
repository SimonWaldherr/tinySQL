package offline

import (
	"context"
	"sync"
	"testing"
)

func TestMemoryStoreCopiesTilePayloadsAndDeletesRevision(t *testing.T) {
	store := NewMemoryStore()
	manifest := testManifest("r1")
	key := TileKey{Z: 2, X: 1, Y: 2}
	payload := []byte("source")
	tile := checkedTile(payload)
	if err := store.PutTile(context.Background(), manifest.Dataset, manifest.Revision, key, tile); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'S'
	tile.Data[1] = 'X'
	got, found, err := store.GetTile(context.Background(), manifest.Dataset, manifest.Revision, key)
	if err != nil || !found || string(got.Data) != "source" {
		t.Fatalf("stored tile=%q found=%t err=%v", got.Data, found, err)
	}
	got.Data[0] = 'X'
	got, found, err = store.GetTile(context.Background(), manifest.Dataset, manifest.Revision, key)
	if err != nil || !found || string(got.Data) != "source" {
		t.Fatalf("retrieved tile leaked mutation=%q found=%t err=%v", got.Data, found, err)
	}
	if err := store.PutManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteRevision(context.Background(), manifest.Dataset, manifest.Revision); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetTile(context.Background(), manifest.Dataset, manifest.Revision, key); err != nil || found {
		t.Fatalf("deleted tile found=%t err=%v", found, err)
	}
}

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	const workers = 8
	const perWorker = 32
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < perWorker; i++ {
				key := TileKey{Z: 6, X: worker, Y: i}
				tile := checkedTile([]byte{byte(worker), byte(i)})
				if err := store.PutTile(ctx, "concurrent", "r1", key, tile); err != nil {
					t.Errorf("put %s: %v", key, err)
					return
				}
				got, found, err := store.GetTile(ctx, "concurrent", "r1", key)
				if err != nil || !found || string(got.Data) != string(tile.Data) {
					t.Errorf("get %s found=%t data=%x err=%v", key, found, got.Data, err)
					return
				}
			}
		}()
	}
	group.Wait()
}
