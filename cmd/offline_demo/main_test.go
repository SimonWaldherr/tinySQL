package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCreatesAndReusesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "poi.snapshot")
	var first bytes.Buffer
	if err := run(context.Background(), config{snapshot: path, query: "München", json: true, readOnly: true}, &first); err != nil {
		t.Fatalf("first run: %v", err)
	}
	var payload struct {
		Source   string `json:"source"`
		ReadOnly bool   `json:"read_only"`
		Results  []poi  `json:"results"`
	}
	if err := json.Unmarshal(first.Bytes(), &payload); err != nil {
		t.Fatalf("decode first output: %v", err)
	}
	if payload.Source != "new snapshot" || !payload.ReadOnly || len(payload.Results) != 4 {
		t.Fatalf("unexpected first output: %#v", payload)
	}

	var second bytes.Buffer
	if err := run(context.Background(), config{snapshot: path, query: "museum", json: true, readOnly: true}, &second); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if err := json.Unmarshal(second.Bytes(), &payload); err != nil {
		t.Fatalf("decode second output: %v", err)
	}
	if payload.Source != "snapshot" || len(payload.Results) != 2 {
		t.Fatalf("unexpected reused snapshot output: %#v", payload)
	}
}

func TestWebSearchReturnsSnapshotPOIs(t *testing.T) {
	db, _, err := openPOIDatabase(context.Background(), config{})
	if err != nil {
		t.Fatal(err)
	}
	a := &offlineApp{db: db, source: "in-memory dataset", readOnly: true}
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=museum", nil)
	res := httptest.NewRecorder()
	a.search(res, req)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte("Deutsches Museum")) {
		t.Fatalf("search response: %d %s", res.Code, res.Body.String())
	}
}

func TestSearchRejectsBlankQuery(t *testing.T) {
	if err := run(context.Background(), config{query: "   "}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected blank query to fail")
	}
}

func TestNearbySearchAndSnapshotReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nearby.snapshot")
	var output bytes.Buffer
	cfg := config{snapshot: path, near: "11.5756,48.1372", radius: "1500", json: true, readOnly: true}
	if err := run(context.Background(), cfg, &output); err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Source  string `json:"source"`
		Results []poi  `json:"results"`
	}
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Source != "new snapshot" || len(payload.Results) != 3 {
		t.Fatalf("nearby results = %#v", payload)
	}
	for i, id := range []int{1, 2, 5} {
		if p := payload.Results[i]; p.ID != id || p.Distance == nil {
			t.Fatalf("result %d = %#v, want ID %d and a distance", i, p, id)
		}
	}
	if *payload.Results[0].Distance != 0 || *payload.Results[1].Distance < 900 || *payload.Results[1].Distance > 1100 || *payload.Results[2].Distance <= *payload.Results[1].Distance {
		t.Fatalf("unexpected distances: %#v", payload.Results)
	}

	output.Reset()
	cfg.query = "museum"
	if err := run(context.Background(), cfg, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Source != "snapshot" || len(payload.Results) != 2 || payload.Results[0].ID != 2 || payload.Results[1].ID != 5 {
		t.Fatalf("filtered snapshot results = %#v", payload)
	}

	// longitude comes first: this selects Berlin, not Munich or the ocean.
	output.Reset()
	cfg.query, cfg.near, cfg.radius = "", "13.3777,52.5163", "100"
	if err := run(context.Background(), cfg, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Results) != 1 || payload.Results[0].ID != 4 {
		t.Fatalf("Berlin results = %#v", payload.Results)
	}
}

func TestNearbySearchInclusiveRadiusAndValidation(t *testing.T) {
	db, _, err := openPOIDatabase(context.Background(), config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	center := &proximity{lon: 11.5756, lat: 48.1372, radius: 1500}
	all, err := queryPOIs(context.Background(), db, "", center)
	if err != nil || len(all) != 3 || all[1].Distance == nil {
		t.Fatalf("nearby results = %#v, err=%v", all, err)
	}
	center.radius = *all[1].Distance
	boundary, err := queryPOIs(context.Background(), db, "", center)
	if err != nil || len(boundary) != 2 || boundary[1].ID != 2 {
		t.Fatalf("inclusive radius results = %#v, err=%v", boundary, err)
	}
	center.radius = math.Nextafter(center.radius, 0)
	inside, err := queryPOIs(context.Background(), db, "", center)
	if err != nil || len(inside) != 1 || inside[0].ID != 1 {
		t.Fatalf("just-inside radius results = %#v, err=%v", inside, err)
	}

	for _, tc := range []struct{ coords, radius string }{
		{"11,48", ""}, {"", "10"}, {"11", "10"}, {"181,48", "10"},
		{"11,91", "10"}, {"NaN,48", "10"}, {"11,Inf", "10"},
		{"11,48", "NaN"}, {"11,48", "Inf"}, {"11,48", "0"}, {"11,48", "-1"},
	} {
		if _, err := parseProximity(tc.coords, tc.radius); err == nil {
			t.Errorf("parseProximity(%q, %q) accepted invalid input", tc.coords, tc.radius)
		}
	}
}

func TestNearbySearchAPI(t *testing.T) {
	db, _, err := openPOIDatabase(context.Background(), config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := &offlineApp{db: db}
	for _, tc := range []struct {
		url  string
		want []int
	}{
		{"/api/search?lon=11.5756&lat=48.1372&radius_m=1500", []int{1, 2, 5}},
		{"/api/search?q=museum&lon=11.5756&lat=48.1372&radius_m=1500", []int{2, 5}},
		{"/api/search?lon=13.3777&lat=52.5163&radius_m=100", []int{4}},
		{"/api/search?lon=0&lat=0&radius_m=10", []int{}},
	} {
		t.Run(tc.url, func(t *testing.T) {
			res := httptest.NewRecorder()
			a.handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, tc.url, nil))
			var got struct {
				Results []poi `json:"results"`
			}
			if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &got) != nil || len(got.Results) != len(tc.want) {
				t.Fatalf("response %d: %s", res.Code, res.Body.String())
			}
			for i, id := range tc.want {
				if got.Results[i].ID != id || got.Results[i].Distance == nil {
					t.Fatalf("result %d = %#v, want ID %d and distance", i, got.Results[i], id)
				}
			}
		})
	}
	for _, query := range []string{"lon=11&lat=48", "lon=181&lat=48&radius_m=5", "lon=NaN&lat=48&radius_m=5", "lon=11&lat=48&radius_m=-5"} {
		res := httptest.NewRecorder()
		a.handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/search?"+query, nil))
		if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), `"error"`) {
			t.Errorf("%s: response %d %s", query, res.Code, res.Body.String())
		}
	}
}

func TestWebRejectsCLINearbyFlags(t *testing.T) {
	for _, cfg := range []config{{near: "11,48"}, {radius: "100"}, {near: "11,48", radius: "100"}} {
		if err := serveWeb(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "browser form") {
			t.Errorf("%s: expected clear error, got %v", fmt.Sprint(cfg), err)
		}
	}
}
