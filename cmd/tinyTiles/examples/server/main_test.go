//go:build !js && !wasm && !baremetal

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Karte-Bayern/tinyTiles/offline"
)

func TestParseKeyAndDatasetValidation(t *testing.T) {
	key, err := parseKey([]string{"8", "137", "167"})
	if err != nil || key.Z != 8 || key.X != 137 || key.Y != 167 {
		t.Fatalf("key=%#v err=%v", key, err)
	}
	for _, parts := range [][]string{{"8", "x", "167"}, {"31", "0", "0"}, {"8", "137"}} {
		if _, err := parseKey(parts); err == nil {
			t.Fatalf("invalid key accepted: %#v", parts)
		}
	}
	if err := validateDataset("dach/offline"); err != nil {
		t.Fatalf("valid dataset rejected: %v", err)
	}
	if err := validateDataset("bad\x00dataset"); err == nil {
		t.Fatal("invalid dataset accepted")
	}
	if got := quoteETag("revision"); got != `"revision"` {
		t.Fatalf("ETag=%q", got)
	}
}

func TestManifestAndConditionalTileRoutes(t *testing.T) {
	revision := strings.Repeat("a", 64)
	server := &tileServer{dataset: "demo", revision: revision, corsOrigin: "http://localhost:8081"}
	request := httptest.NewRequest(http.MethodGet, "http://tiles.example/sync/manifest.json", nil)
	request.Host = "tiles.example"
	response := httptest.NewRecorder()
	server.routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("manifest status=%d body=%q", response.Code, response.Body.String())
	}
	if expose := response.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(expose, offline.HeaderTileChecksum) {
		t.Fatalf("missing CORS expose header: %q", expose)
	}
	var manifest offline.Manifest
	if err := json.Unmarshal(response.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Dataset != "demo" || manifest.Revision != revision || manifest.TileURLTemplate != "http://tiles.example/tiles/{revision}/{z}/{x}/{y}" {
		t.Fatalf("manifest=%#v", manifest)
	}

	tileRequest := httptest.NewRequest(http.MethodGet, "http://tiles.example/tiles/"+revision+"/8/137/167", nil)
	tileRequest.Header.Set("If-None-Match", quoteETag(revision+":8/137/167"))
	tileResponse := httptest.NewRecorder()
	server.routes().ServeHTTP(tileResponse, tileRequest)
	if tileResponse.Code != http.StatusNotModified || tileResponse.Header().Get("ETag") != quoteETag(revision+":8/137/167") {
		t.Fatalf("conditional tile status=%d etag=%q", tileResponse.Code, tileResponse.Header().Get("ETag"))
	}
}

func TestNewTileServerRejectsInvalidPoolSettings(t *testing.T) {
	if _, err := newTileServer(t.Context(), "unused", "demo", "", "", 0, 1); err == nil {
		t.Fatal("zero readers accepted")
	}
	if _, err := newTileServer(t.Context(), "unused", "demo", "", "", 1, 0); err == nil {
		t.Fatal("zero memory accepted")
	}
}

func TestServerURLValidation(t *testing.T) {
	if got, err := normalizePublicBase("https://tiles.example/base/"); err != nil || got != "https://tiles.example/base" {
		t.Fatalf("base=%q err=%v", got, err)
	}
	for _, value := range []string{"tiles.example", "ftp://tiles.example", "https://user@tiles.example", "https://tiles.example/?x=1"} {
		if _, err := normalizePublicBase(value); err == nil {
			t.Fatalf("invalid public base accepted: %q", value)
		}
	}
	if got, err := normalizeCORSOrigin("http://localhost:8081/"); err != nil || got != "http://localhost:8081" {
		t.Fatalf("cors=%q err=%v", got, err)
	}
	for _, value := range []string{"https://tiles.example/path", "https://user@tiles.example", "null"} {
		if _, err := normalizeCORSOrigin(value); err == nil {
			t.Fatalf("invalid CORS origin accepted: %q", value)
		}
	}
	server := &tileServer{}
	if got, err := server.baseURL(&http.Request{Host: "tiles.example:8080"}); err != nil || got != "http://tiles.example:8080" {
		t.Fatalf("request base=%q err=%v", got, err)
	}
	if _, err := server.baseURL(&http.Request{Host: "tiles.example/untrusted-path"}); err == nil {
		t.Fatal("invalid request host accepted")
	}
}
