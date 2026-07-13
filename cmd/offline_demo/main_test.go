package main

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestSearchRejectsBlankQuery(t *testing.T) {
	if err := run(context.Background(), config{query: "   "}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected blank query to fail")
	}
}
