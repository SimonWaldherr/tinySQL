package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
