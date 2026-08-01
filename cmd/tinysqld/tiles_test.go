package main

// Tests for the /tiles/ XYZ endpoint.
//
// The assertion that matters most is the row flip: the URL is XYZ but the stored
// tile_row is TMS, so a tile requested at y=0 must come back from the *last* TMS
// row. Getting that wrong yields a vertically mirrored map that looks almost
// correct, so it is checked explicitly rather than inferred from a round trip.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// newTileDaemon builds a daemon serving a zoom-1 tileset whose four tiles are
// labelled by compass position, stored with TMS rows as the specification says.
func newTileDaemon(t *testing.T, withMetadata bool) http.Handler {
	t.Helper()
	inst := &tinysql.Instance{DB: storage.NewDB()}

	tiles := storage.NewTable("world", []storage.Column{
		{Name: "zoom_level", Type: storage.IntType},
		{Name: "tile_column", Type: storage.IntType},
		{Name: "tile_row", Type: storage.IntType},
		{Name: "tile_data", Type: storage.BlobType},
	}, false)
	// TMS rows: row 1 is the northern half at zoom 1, row 0 the southern.
	tiles.Rows = [][]any{
		{1, 0, 1, []byte("north-west")},
		{1, 1, 1, []byte("north-east")},
		{1, 0, 0, []byte("south-west")},
		{1, 1, 0, []byte("south-east")},
		{0, 0, 0, []byte("world")},
	}
	tiles.Version++
	if err := inst.DB.Put("default", tiles); err != nil {
		t.Fatal(err)
	}

	if withMetadata {
		meta := storage.NewTable("world_metadata", []storage.Column{
			{Name: "name", Type: storage.TextType},
			{Name: "value", Type: storage.TextType},
		}, false)
		meta.Rows = [][]any{
			{"name", "Test World"},
			{"format", "pbf"},
			{"minzoom", "0"},
			{"maxzoom", "1"},
			{"bounds", "-180.0,-85.0511,180.0,85.0511"},
			{"center", "0,0,1"},
			{"attribution", "test"},
		}
		meta.Version++
		if err := inst.DB.Put("default", meta); err != nil {
			t.Fatal(err)
		}
	}

	return newDaemon(inst, daemonConfig{DefaultTenant: "default", Tiles: true}).routes()
}

func getTile(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestTileEndpointFlipsXYZToTMS is the core test: an XYZ y must map to the
// correct TMS row.
func TestTileEndpointFlipsXYZToTMS(t *testing.T) {
	h := newTileDaemon(t, true)

	cases := []struct {
		path string
		want string
	}{
		// Zoom 1, XYZ y=0 is the north; it lives at TMS row 1.
		{"/tiles/world/1/0/0.pbf", "north-west"},
		{"/tiles/world/1/1/0.pbf", "north-east"},
		// XYZ y=1 is the south; TMS row 0.
		{"/tiles/world/1/0/1.pbf", "south-west"},
		{"/tiles/world/1/1/1.pbf", "south-east"},
		// Zoom 0 has one tile and the flip is a no-op.
		{"/tiles/world/0/0/0.pbf", "world"},
	}
	for _, tc := range cases {
		rec := getTile(t, h, tc.path)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200 (%s)", tc.path, rec.Code, strings.TrimSpace(rec.Body.String()))
			continue
		}
		if got := rec.Body.String(); got != tc.want {
			t.Errorf("%s returned %q, want %q — the XYZ/TMS row flip is wrong",
				tc.path, got, tc.want)
		}
	}
}

// TestTileEndpointContentHeaders checks the format-derived headers. pbf is the
// case that matters: MBTiles stores vector tiles gzipped, so the response must
// declare Content-Encoding or clients cannot decode it.
func TestTileEndpointContentHeaders(t *testing.T) {
	h := newTileDaemon(t, true)
	rec := getTile(t, h, "/tiles/world/1/0/0.pbf")
	if got := rec.Header().Get("Content-Type"); got != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf for format=pbf", got)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip: MBTiles stores pbf tiles compressed", got)
	}
	if got := rec.Header().Get("Content-Length"); got != fmt.Sprint(len("north-west")) {
		t.Errorf("Content-Length = %q, want %d", got, len("north-west"))
	}

	// Without a metadata table there is no declared format, so the payload is
	// sent as opaque bytes rather than mislabelled.
	plain := newTileDaemon(t, false)
	rec = getTile(t, plain, "/tiles/world/1/0/0")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d without metadata, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream when the format is unknown", got)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want empty for an unknown format", got)
	}
}

// TestTileEndpointMissingTile checks a sparse tileset returns 404 rather than an
// error or an empty 200, which a map client treats very differently.
func TestTileEndpointMissingTile(t *testing.T) {
	h := newTileDaemon(t, true)
	// Valid coordinates, no such tile stored.
	rec := getTile(t, h, "/tiles/world/1/0/0.pbf")
	if rec.Code != http.StatusOK {
		t.Fatalf("sanity: expected the seeded tile to exist, got %d", rec.Code)
	}
	rec = getTile(t, h, "/tiles/world/5/9/9.pbf")
	if rec.Code != http.StatusNotFound {
		t.Errorf("absent tile: status %d, want 404", rec.Code)
	}
}

// TestTileEndpointRejectsBadRequests covers coordinate validation and the
// tileset-name restriction that keeps the name out of SQL as anything but an
// identifier.
func TestTileEndpointRejectsBadRequests(t *testing.T) {
	h := newTileDaemon(t, true)
	cases := []struct {
		path string
		want int
	}{
		{"/tiles/world/1/0", http.StatusBadRequest},             // too few parts
		{"/tiles/world/1/0/0/0.pbf", http.StatusBadRequest},     // too many
		{"/tiles/world/abc/0/0.pbf", http.StatusBadRequest},     // zoom not a number
		{"/tiles/world/1/9/0.pbf", http.StatusBadRequest},       // x outside the zoom grid
		{"/tiles/world/1/0/9.pbf", http.StatusBadRequest},       // y outside the zoom grid
		{"/tiles/world/99/0/0.pbf", http.StatusBadRequest},      // zoom out of range
		{"/tiles/world/-1/0/0.pbf", http.StatusBadRequest},      // negative zoom
		{"/tiles/wo%20rld/1/0/0.pbf", http.StatusBadRequest},    // space in name
		{"/tiles/world;DROP/1/0/0.pbf", http.StatusBadRequest},  // punctuation in name
		{"/tiles/nosuchtileset/1/0/0.pbf", http.StatusNotFound}, // unknown tileset
	}
	for _, tc := range cases {
		rec := getTile(t, h, tc.path)
		if rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", tc.path, rec.Code, tc.want,
				strings.TrimSpace(rec.Body.String()))
		}
	}
}

// TestTileJSON checks the discovery document a map client fetches first.
func TestTileJSON(t *testing.T) {
	h := newTileDaemon(t, true)
	rec := getTile(t, h, "/tiles/world.json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("TileJSON is not valid JSON: %v", err)
	}
	if got["tilejson"] != "3.0.0" {
		t.Errorf("tilejson = %v, want 3.0.0", got["tilejson"])
	}
	if got["scheme"] != "xyz" {
		t.Errorf("scheme = %v, want xyz — the endpoint serves XYZ, not TMS", got["scheme"])
	}
	if got["name"] != "Test World" {
		t.Errorf("name = %v, want Test World from metadata", got["name"])
	}
	if got["format"] != "pbf" {
		t.Errorf("format = %v, want pbf", got["format"])
	}
	tiles, ok := got["tiles"].([]any)
	if !ok || len(tiles) != 1 {
		t.Fatalf("tiles = %v, want a single URL template", got["tiles"])
	}
	tmpl, _ := tiles[0].(string)
	for _, want := range []string{"{z}", "{x}", "{y}", "/tiles/world/", ".pbf"} {
		if !strings.Contains(tmpl, want) {
			t.Errorf("tile URL %q is missing %q", tmpl, want)
		}
	}
	if got["minzoom"] != float64(0) || got["maxzoom"] != float64(1) {
		t.Errorf("zoom range = %v..%v, want 0..1", got["minzoom"], got["maxzoom"])
	}
	bounds, ok := got["bounds"].([]any)
	if !ok || len(bounds) != 4 {
		t.Errorf("bounds = %v, want 4 numbers", got["bounds"])
	}
	center, ok := got["center"].([]any)
	if !ok || len(center) != 3 {
		t.Errorf("center = %v, want 3 numbers", got["center"])
	}
}

// TestTileJSONDerivesZoomRangeWithoutMetadata checks the fallback: with no
// metadata table the zoom range still comes from the tiles themselves, so a
// client gets something usable.
func TestTileJSONDerivesZoomRangeWithoutMetadata(t *testing.T) {
	h := newTileDaemon(t, false)
	rec := getTile(t, h, "/tiles/world.json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["minzoom"] != float64(0) || got["maxzoom"] != float64(1) {
		t.Errorf("derived zoom range = %v..%v, want 0..1 from the tiles table",
			got["minzoom"], got["maxzoom"])
	}
	// Falls back to png so the URL template is still well-formed.
	if got["format"] != "png" {
		t.Errorf("format = %v, want the png default when metadata declares none", got["format"])
	}
}

func TestTileMetadataEndpoint(t *testing.T) {
	h := newTileDaemon(t, true)
	rec := getTile(t, h, "/tiles/world/metadata")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["format"] != "pbf" || got["name"] != "Test World" {
		t.Errorf("metadata = %v, want the seeded rows", got)
	}

	// A tileset with no metadata table reports 404 rather than an empty object.
	plain := newTileDaemon(t, false)
	if rec := getTile(t, plain, "/tiles/world/metadata"); rec.Code != http.StatusNotFound {
		t.Errorf("missing metadata: status %d, want 404", rec.Code)
	}
}

// TestTilesDisabledByDefault checks the endpoint is not exposed unless asked for.
func TestTilesDisabledByDefault(t *testing.T) {
	inst := &tinysql.Instance{DB: storage.NewDB()}
	h := newDaemon(inst, daemonConfig{DefaultTenant: "default"}).routes()
	rec := getTile(t, h, "/tiles/world/1/0/0.pbf")
	if rec.Code != http.StatusNotFound {
		t.Errorf("with -tiles off the endpoint should not be routed; got status %d", rec.Code)
	}
}

// TestTileHeadRequest checks HEAD returns the headers without a body, which map
// clients and CDNs use for cache validation.
func TestTileHeadRequest(t *testing.T) {
	h := newTileDaemon(t, true)
	req := httptest.NewRequest(http.MethodHead, "/tiles/world/1/0/0.pbf", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD status %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD returned a %d-byte body, want none", rec.Body.Len())
	}
	if got := rec.Header().Get("Content-Length"); got != fmt.Sprint(len("north-west")) {
		t.Errorf("HEAD Content-Length = %q, want %d", got, len("north-west"))
	}
}

// TestTileTenantIsolation checks the tenant query parameter selects the right
// database, so one daemon can serve several tenants' tilesets.
func TestTileTenantIsolation(t *testing.T) {
	inst := &tinysql.Instance{DB: storage.NewDB()}
	for _, tenant := range []string{"default", "other"} {
		tiles := storage.NewTable("world", []storage.Column{
			{Name: "zoom_level", Type: storage.IntType},
			{Name: "tile_column", Type: storage.IntType},
			{Name: "tile_row", Type: storage.IntType},
			{Name: "tile_data", Type: storage.BlobType},
		}, false)
		tiles.Rows = [][]any{{0, 0, 0, []byte(tenant + "-tile")}}
		tiles.Version++
		if err := inst.DB.Put(tenant, tiles); err != nil {
			t.Fatal(err)
		}
	}
	h := newDaemon(inst, daemonConfig{DefaultTenant: "default", Tiles: true}).routes()

	if got := getTile(t, h, "/tiles/world/0/0/0").Body.String(); got != "default-tile" {
		t.Errorf("default tenant served %q", got)
	}
	if got := getTile(t, h, "/tiles/world/0/0/0?tenant=other").Body.String(); got != "other-tile" {
		t.Errorf("tenant=other served %q, want other-tile", got)
	}
}
