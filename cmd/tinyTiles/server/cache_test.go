package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	tinytiles "github.com/Karte-Bayern/tinyTiles"
	"github.com/Karte-Bayern/tinyTiles/offline"
	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

type cacheTileSource struct{ data []byte }

func (s cacheTileSource) Info(context.Context) (tiles.SourceInfo, error) {
	return tiles.SourceInfo{Name: "cache.fixture", TileCount: 1, TileBytes: int64(len(s.data)), MaxTileBytes: int64(len(s.data)), Metadata: map[string]string{"format": "pbf"}}, nil
}
func (s cacheTileSource) ScanTiles(_ context.Context, visit func(tiles.Tile) error) error {
	return visit(tiles.Tile{Key: tiles.Key{Z: 2, X: 1, Y: 2}, Data: s.data})
}
func cacheDataset(tb testing.TB) (*tinytiles.Dataset, []byte) {
	tb.Helper()
	data := bytes.Repeat([]byte("tile-payload"), 3000)
	artifact := filepath.Join(tb.TempDir(), "cache.ttiles")
	if _, err := tiles.ImportTiles(tb.Context(), cacheTileSource{data}, artifact, &tiles.ImportOptions{BatchSize: 1, MaxMemoryBytes: 8 << 20}); err != nil {
		tb.Fatal(err)
	}
	ds, err := tinytiles.Open(tb.Context(), artifact, tinytiles.OpenOptions{Readers: 2, MaxMemoryBytes: 1 << 20})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = ds.Close() })
	return ds, data
}

func TestTileCacheBoundsAndLRU(t *testing.T) {
	c := newTileCache(8)
	payload := tilePayload{data: []byte("abc"), checksum: "x"}
	a, b, d := tiles.Key{Z: 2, X: 0}, tiles.Key{Z: 2, X: 1}, tiles.Key{Z: 2, X: 2}
	c.put(a, payload)
	c.put(b, payload)
	c.get(a)
	c.put(d, payload)
	if _, ok := c.get(b); ok {
		t.Fatal("least recently used tile was not evicted")
	}
	if _, ok := c.get(a); !ok {
		t.Fatal("recent tile was evicted")
	}
	c.put(b, tilePayload{data: make([]byte, 9)})
	if c.used != 8 || len(c.entries) != 2 {
		t.Fatalf("budget exceeded: bytes=%d entries=%d", c.used, len(c.entries))
	}
	many := newTileCache(1 << 20)
	for i := 0; i < tileCacheMaxEntries+10; i++ {
		many.put(tiles.Key{Z: 20, X: i}, tilePayload{})
	}
	if len(many.entries) != tileCacheMaxEntries {
		t.Fatal("entry bound not enforced")
	}
	var disabled *tileCache
	disabled.put(a, payload)
	if _, ok := disabled.get(a); ok {
		t.Fatal("disabled cache returned a tile")
	}
}

func TestCachedTilesHTTPAndConcurrentRequests(t *testing.T) {
	ds, data := cacheDataset(t)
	s, err := New(Config{Dataset: ds, DatasetID: "cache", TileCacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	paths := []string{"/tiles/2/1/1.mvt", "/sync/tiles/" + s.revision + "/2/1/2"}
	for _, path := range paths {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), data) || w.Header().Get(offline.HeaderTileChecksum) != offline.Checksum(data) {
			t.Fatalf("bad tile response: %d", w.Code)
		}
		head := httptest.NewRecorder()
		h.ServeHTTP(head, httptest.NewRequest("HEAD", path, nil))
		if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != w.Header().Get("Content-Length") {
			t.Fatal("HEAD mismatch")
		}
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("If-None-Match", w.Header().Get("ETag"))
		conditional := httptest.NewRecorder()
		h.ServeHTTP(conditional, req)
		if conditional.Code != 304 || conditional.Body.Len() != 0 {
			t.Fatal("conditional response mismatch")
		}
	}
	if len(s.tileCache.entries) != 1 {
		t.Fatal("XYZ and TMS did not share a cached tile")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest("GET", paths[j%2], nil))
				if !bytes.Equal(w.Body.Bytes(), data) {
					t.Error("concurrent payload mismatch")
				}
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.lookupTile(ctx, tiles.Key{Z: 2, X: 1, Y: 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hit: %v", err)
	}
	if _, found, err := s.lookupTile(t.Context(), tiles.Key{Z: 2, X: 0, Y: 0}); err != nil || found {
		t.Fatal("missing tile became a hit")
	}
	other, err := New(Config{Dataset: ds, DatasetID: "cache", TileCacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(other.tileCache.entries) != 0 {
		t.Fatal("new server inherited an old cache")
	}
	if _, err := New(Config{Dataset: ds, DatasetID: "cache", TileCacheBytes: -1}); err == nil {
		t.Fatal("negative budget accepted")
	}
}

type benchmarkTileWriter struct {
	header        http.Header
	bytes, status int
}

func (w *benchmarkTileWriter) Header() http.Header    { return w.header }
func (w *benchmarkTileWriter) WriteHeader(status int) { w.status = status }
func (w *benchmarkTileWriter) Write(data []byte) (int, error) {
	w.bytes += len(data)
	return len(data), nil
}
func BenchmarkTileServerCache(b *testing.B) {
	ds, data := cacheDataset(b)
	for _, budget := range []int64{0, 1 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", budget), func(b *testing.B) {
			s, err := New(Config{Dataset: ds, DatasetID: "cache", TileCacheBytes: budget})
			if err != nil {
				b.Fatal(err)
			}
			h := s.Handler()
			req := httptest.NewRequest("GET", "/tiles/2/1/1.mvt", nil)
			w := &benchmarkTileWriter{header: make(http.Header)}
			h.ServeHTTP(w, req)
			b.ReportAllocs()
			for b.Loop() {
				clear(w.header)
				w.bytes = 0
				w.status = 200
				h.ServeHTTP(w, req)
				if w.status != 200 || w.bytes != len(data) {
					b.Fatal("bad response")
				}
			}
		})
	}
}

func TestXYZRevisionCachePolicy(t *testing.T) {
	ds, _ := cacheDataset(t)
	s, err := New(Config{Dataset: ds, DatasetID: "cache", TileCacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	for _, tc := range []struct {
		query  string
		status int
		policy string
	}{
		{"", 200, "public, max-age=0, must-revalidate"},
		{"?tinytiles_rev=" + s.revision, 200, "public, max-age=31536000, immutable"},
		{"?tinytiles_rev=old-revision", 404, ""},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/tiles/2/1/1.mvt"+tc.query, nil))
		if w.Code != tc.status || w.Header().Get("Cache-Control") != tc.policy {
			t.Fatalf("query=%q status=%d policy=%q", tc.query, w.Code, w.Header().Get("Cache-Control"))
		}
	}
}
