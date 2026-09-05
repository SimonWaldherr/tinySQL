//go:build sqliteimport

package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	tinytiles "github.com/Karte-Bayern/tinyTiles"
	"github.com/Karte-Bayern/tinyTiles/offline"
	tiles "github.com/SimonWaldherr/tinySQL/tiles"
	_ "modernc.org/sqlite"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	dataset := testDataset(t)
	server, err := New(Config{Dataset: dataset, DatasetID: "fixture", PublicBase: "https://tiles.example", CORSOrigin: "http://localhost:8081"})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func testDataset(t *testing.T) *tinytiles.Dataset {
	t.Helper()
	ctx := t.Context()
	source := filepath.Join(t.TempDir(), "fixture.mbtiles")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
		INSERT INTO metadata VALUES
			('name', 'fixture'), ('format', 'pbf'), ('kb:content_encoding', 'gzip'),
			('minzoom', '2'), ('maxzoom', '2'), ('bounds', '10,20,30,40');
		INSERT INTO tiles VALUES (2, 1, 2, x'010203');`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "fixture.ttiles")
	if _, err := tiles.ImportMBTiles(ctx, source, artifact, &tiles.ImportOptions{Schema: tiles.SchemaFlat, BatchSize: 1, MinFreeBytes: 0}); err != nil {
		t.Fatal(err)
	}
	dataset, err := tinytiles.Open(context.Background(), artifact, tinytiles.OpenOptions{Readers: 2, MaxMemoryBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataset.Close() })
	return dataset
}

func TestHandlerServesXYZAndRevisionedTMS(t *testing.T) {
	server := testServer(t)
	xyzRequest := httptest.NewRequest(http.MethodGet, "https://tiles.example/tiles/2/1/1.mvt", nil)
	xyzResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(xyzResponse, xyzRequest)
	if xyzResponse.Code != http.StatusOK || string(xyzResponse.Body.Bytes()) != string([]byte{1, 2, 3}) {
		t.Fatalf("XYZ response = %d %q", xyzResponse.Code, xyzResponse.Body.Bytes())
	}
	if got := xyzResponse.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("XYZ Content-Encoding = %q", got)
	}
	if got := xyzResponse.Header().Get(offline.HeaderTileChecksum); got != offline.Checksum([]byte{1, 2, 3}) {
		t.Fatalf("XYZ checksum = %q", got)
	}
	if got := xyzResponse.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:8081" {
		t.Fatalf("CORS origin = %q", got)
	}

	manifestRequest := httptest.NewRequest(http.MethodGet, "https://tiles.example/sync/manifest.json", nil)
	manifestResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(manifestResponse, manifestRequest)
	if manifestResponse.Code != http.StatusOK {
		t.Fatalf("manifest status = %d: %s", manifestResponse.Code, manifestResponse.Body.String())
	}
	var manifest offline.Manifest
	if err := json.Unmarshal(manifestResponse.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.CoordinateSystem != "TMS" || !strings.Contains(manifest.TileURLTemplate, "/sync/tiles/{revision}/{z}/{x}/{y}") {
		t.Fatalf("manifest = %#v", manifest)
	}
	tmsRequest := httptest.NewRequest(http.MethodGet, "https://tiles.example/sync/tiles/"+manifest.Revision+"/2/1/2", nil)
	tmsResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(tmsResponse, tmsRequest)
	if tmsResponse.Code != http.StatusOK || string(tmsResponse.Body.Bytes()) != string([]byte{1, 2, 3}) {
		t.Fatalf("TMS response = %d %q", tmsResponse.Code, tmsResponse.Body.Bytes())
	}
	if got := tmsResponse.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("TMS must not use HTTP Content-Encoding, got %q", got)
	}
	if got := tmsResponse.Header().Get(offline.HeaderTileContentEncoding); got != "gzip" {
		t.Fatalf("TMS raw encoding = %q", got)
	}
}

func TestHandlerTileJSONAndConditionalResponses(t *testing.T) {
	server := testServer(t)
	request := httptest.NewRequest(http.MethodGet, "https://tiles.example/tilejson.json", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("TileJSON status = %d: %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["scheme"] != "xyz" || payload["tinytiles:revision"] == "" {
		t.Fatalf("TileJSON = %#v", payload)
	}
	conditional := httptest.NewRequest(http.MethodGet, "https://tiles.example/tilejson.json", nil)
	conditional.Header.Set("If-None-Match", response.Header().Get("ETag"))
	notModified := httptest.NewRecorder()
	server.Handler().ServeHTTP(notModified, conditional)
	if notModified.Code != http.StatusNotModified {
		t.Fatalf("conditional TileJSON status = %d", notModified.Code)
	}
	missing := httptest.NewRecorder()
	server.Handler().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "https://tiles.example/tiles/2/1/0.mvt", nil))
	if missing.Code != http.StatusNoContent {
		t.Fatalf("missing XYZ status = %d", missing.Code)
	}
}

func TestConfigurationAndPathValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("nil dataset accepted")
	}
	for _, value := range []string{"ftp://tiles.example", "https://user@tiles.example", "https://tiles.example/?q=1"} {
		if _, err := normalizePublicBase(value); err == nil {
			t.Fatalf("public base accepted %q", value)
		}
	}
	for _, path := range []string{"2/1", "31/0/0", "2/4/1", "2/1/not-int"} {
		if _, _, _, err := parseXYZPath(path); err == nil {
			t.Fatalf("XYZ path accepted %q", path)
		}
	}
}
