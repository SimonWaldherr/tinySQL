//go:build !js || !wasm

package offline

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkMemoryStoreGetTileParallel(b *testing.B) {
	store := NewMemoryStore()
	key := TileKey{Z: 8, X: 137, Y: 167}
	tile := checkedTile(makeBenchmarkTileBytes(8 << 10))
	if err := store.PutTile(context.Background(), "bench", "r1", key, tile); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			got, found, err := store.GetTile(context.Background(), "bench", "r1", key)
			if err != nil || !found || len(got.Data) != len(tile.Data) {
				b.Fatalf("get found=%t bytes=%d err=%v", found, len(got.Data), err)
			}
		}
	})
}

func BenchmarkFileStoreGetTileParallel(b *testing.B) {
	store, err := NewFileStore(filepath.Join(b.TempDir(), "cache"))
	if err != nil {
		b.Fatal(err)
	}
	key := TileKey{Z: 8, X: 137, Y: 167}
	tile := checkedTile(makeBenchmarkTileBytes(8 << 10))
	if err := store.PutTile(context.Background(), "bench", "r1", key, tile); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			got, found, err := store.GetTile(context.Background(), "bench", "r1", key)
			if err != nil || !found || len(got.Data) != len(tile.Data) {
				b.Fatalf("get found=%t bytes=%d err=%v", found, len(got.Data), err)
			}
		}
	})
}

func BenchmarkSynchronizerStreamsRange(b *testing.B) {
	keys := []TileRange{{Z: 6, XMin: 0, XMax: 7, YMin: 0, YMax: 7}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store := NewMemoryStore()
		fetcher := &fakeFetcher{manifest: testManifest("benchmark")}
		synchronizer := &Synchronizer{Store: store, Fetcher: fetcher}
		result, err := synchronizer.Sync(context.Background(), SyncRequest{Ranges: keys, Concurrency: 4})
		if err != nil || result.Downloaded != 64 {
			b.Fatalf("sync result=%#v err=%v", result, err)
		}
	}
}

func makeBenchmarkTileBytes(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 31)
	}
	return data
}
