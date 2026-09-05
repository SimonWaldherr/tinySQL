package offline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPFetcherResolvesRelativeTemplateAndVerifiesChecksum(t *testing.T) {
	tileData := []byte("tile bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/sync/manifest.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"format_version":1,"dataset":"demo","revision":"r1","coordinate_system":"TMS","tile_url_template":"/tiles/{revision}/{z}/{x}/{y}"}`))
		case "/tiles/r1/1/0/0":
			w.Header().Set("X-TinyTiles-SHA256", Checksum(tileData))
			_, _ = w.Write(tileData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	fetcher := &HTTPFetcher{ManifestURL: server.URL + "/sync/manifest.json"}
	manifest, err := fetcher.FetchManifest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.TileURLTemplate != server.URL+"/tiles/{revision}/{z}/{x}/{y}" {
		t.Fatalf("resolved template=%q", manifest.TileURLTemplate)
	}
	tile, err := fetcher.FetchTile(context.Background(), manifest, TileKey{Z: 1, X: 0, Y: 0})
	if err != nil || string(tile.Data) != string(tileData) {
		t.Fatalf("tile=%q err=%v", tile.Data, err)
	}
}

func TestHTTPFetcherRejectsOversizedAndBadChecksumTiles(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set(HeaderTileChecksum, Checksum([]byte("different")))
		_, _ = w.Write([]byte("too-large"))
	}))
	defer server.Close()
	// Substitute the server URL in a custom manifest to exercise both limits
	// without a second handler-specific manifest implementation.
	manifest := Manifest{FormatVersion: ProtocolVersion, Dataset: "demo", Revision: "r1", CoordinateSystem: "TMS", TileURLTemplate: server.URL + "/tiles/{z}/{x}/{y}"}
	fetcher := &HTTPFetcher{MaxTileSize: 3}
	if _, err := fetcher.FetchTile(context.Background(), manifest, TileKey{Z: 1, X: 0, Y: 0}); err == nil {
		t.Fatal("oversized tile accepted")
	}
	fetcher.MaxTileSize = 1024
	if _, err := fetcher.FetchTile(context.Background(), manifest, TileKey{Z: 1, X: 0, Y: 0}); err == nil {
		t.Fatal("bad checksum accepted")
	}
}

func TestHTTPFetcherPreservesRawTileEncodingHeader(t *testing.T) {
	payload := []byte("compressed-tile-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(HeaderTileChecksum, Checksum(payload))
		w.Header().Set(HeaderTileContentEncoding, "gzip")
		w.Header().Set("Content-Type", "application/vnd.mapbox-vector-tile")
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	manifest := Manifest{FormatVersion: ProtocolVersion, Dataset: "demo", Revision: "r1", CoordinateSystem: "TMS", TileURLTemplate: server.URL + "/tiles/{z}/{x}/{y}"}
	tile, err := (&HTTPFetcher{}).FetchTile(context.Background(), manifest, TileKey{Z: 1, X: 0, Y: 0})
	if err != nil {
		t.Fatal(err)
	}
	if tile.ContentEncoding != "gzip" || string(tile.Data) != string(payload) {
		t.Fatalf("tile=%#v", tile)
	}
}

func TestHTTPFetcherRejectsHostlessURLs(t *testing.T) {
	if _, err := (&HTTPFetcher{ManifestURL: "http:/missing-host"}).FetchManifest(context.Background()); err == nil {
		t.Fatal("hostless manifest URL accepted")
	}
	manifest := Manifest{FormatVersion: ProtocolVersion, Dataset: "demo", Revision: "r1", CoordinateSystem: "TMS", TileURLTemplate: "http:/missing-host/{z}/{x}/{y}"}
	if _, err := (&HTTPFetcher{}).FetchTile(context.Background(), manifest, TileKey{Z: 1, X: 0, Y: 0}); err == nil {
		t.Fatal("hostless tile URL accepted")
	}
}
