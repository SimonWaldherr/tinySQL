package main

// Benchmarks for the tile-serving SQL path (see executeTileSQL in http.go).
//
// Run, for example:
//
//	go test ./cmd/tinysqld -run '^$' -bench TileLookup -benchmem -count 3

import (
	"context"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func newBenchTileDaemon(b *testing.B) *daemon {
	b.Helper()
	inst := &tinysql.Instance{DB: storage.NewDB()}
	tiles := storage.NewTable("world", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	tiles.Rows = [][]any{{1, 0, 1, []byte("north-west")}}
	tiles.Version++
	if err := inst.DB.Put("default", tiles); err != nil {
		b.Fatal(err)
	}
	return newDaemon(inst, daemonConfig{DefaultTenant: "default", Tiles: true})
}

const benchTileLookupSQL = "SELECT tile_data FROM world WHERE zoom_level = 1 AND tile_column = 0 AND tile_row = 1 LIMIT 1"

// BenchmarkTileLookupUncached measures the tile lookup the way the
// tile-serving path used to run it: parsed fresh with tinysql.ParseSQL on
// every call, which is what every non-tile SQL endpoint on the daemon still
// does today.
func BenchmarkTileLookupUncached(b *testing.B) {
	d := newBenchTileDaemon(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.executeSQL(ctx, "default", benchTileLookupSQL); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTileLookupCached measures the same lookup through executeTileSQL,
// which is what tiles.go now calls: a repeat request for the same tile (the
// common case for a map viewport) reuses the parsed statement and its cached
// plan shape instead of paying ParseSQL again.
func BenchmarkTileLookupCached(b *testing.B) {
	d := newBenchTileDaemon(b)
	ctx := context.Background()
	// Warm the cache once, like the first request for a tile does.
	if _, err := d.executeTileSQL(ctx, "default", benchTileLookupSQL); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.executeTileSQL(ctx, "default", benchTileLookupSQL); err != nil {
			b.Fatal(err)
		}
	}
}
