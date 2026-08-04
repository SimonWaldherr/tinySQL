package exporter

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func geoSample() *engine.ResultSet {
	return &engine.ResultSet{
		Cols: []string{"id", "name", "geom"},
		Rows: []engine.Row{
			{"id": 1, "name": "alice", "geom": `{"type":"Point","coordinates":[13.4,52.5]}`},
			{"id": 2, "name": "bob", "geom": `{"type":"Point","coordinates":[2.35,48.85]}`},
		},
	}
}

func TestExportGeoJSONExplicitColumn(t *testing.T) {
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, geoSample(), "geom", Options{}); err != nil {
		t.Fatalf("ExportGeoJSON: %v", err)
	}
	var fc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fc); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if fc["type"] != "FeatureCollection" {
		t.Fatalf("type = %v, want FeatureCollection", fc["type"])
	}
	features := fc["features"].([]any)
	if len(features) != 2 {
		t.Fatalf("got %d features, want 2", len(features))
	}
	f0 := features[0].(map[string]any)
	geom := f0["geometry"].(map[string]any)
	if geom["type"] != "Point" {
		t.Errorf("feature 0 geometry type = %v, want Point", geom["type"])
	}
	props := f0["properties"].(map[string]any)
	if props["name"] != "alice" {
		t.Errorf("feature 0 properties.name = %v, want alice", props["name"])
	}
	if _, hasGeom := props["geom"]; hasGeom {
		t.Errorf("properties unexpectedly includes the geometry column: %v", props)
	}
}

func TestExportGeoJSONAutoDetect(t *testing.T) {
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, geoSample(), "", Options{}); err != nil {
		t.Fatalf("ExportGeoJSON with auto-detect: %v", err)
	}
	var fc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fc); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	features := fc["features"].([]any)
	f0 := features[0].(map[string]any)
	if f0["geometry"].(map[string]any)["type"] != "Point" {
		t.Errorf("auto-detected geometry column mismatch: %v", f0["geometry"])
	}
}

func TestExportGeoJSONAutoDetectAmbiguous(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"a", "b"},
		Rows: []engine.Row{
			{"a": `{"type":"Point","coordinates":[0,0]}`, "b": `{"type":"Point","coordinates":[1,1]}`},
		},
	}
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, rs, "", Options{}); err == nil {
		t.Errorf("ExportGeoJSON with two candidate geometry columns succeeded, want an error")
	}
}

func TestExportGeoJSONAutoDetectNoCandidate(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"a"},
		Rows: []engine.Row{{"a": "not geometry"}},
	}
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, rs, "", Options{}); err == nil {
		t.Errorf("ExportGeoJSON with no geometry-looking column succeeded, want an error")
	}
}

func TestExportGeoJSONNullGeometry(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "geom"},
		Rows: []engine.Row{{"id": 1, "geom": nil}},
	}
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, rs, "geom", Options{}); err != nil {
		t.Fatalf("ExportGeoJSON: %v", err)
	}
	var fc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fc); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	f0 := fc["features"].([]any)[0].(map[string]any)
	if f0["geometry"] != nil {
		t.Errorf("null geometry cell produced %v, want a null geometry field", f0["geometry"])
	}
}

// TestExportGeoJSONLegacyRawMessageCell covers the pre-GEOMETRY-type shape
// importers like ImportGeoJSON/ImportOSM still produce: a json.RawMessage
// cell, not a canonical string.
func TestExportGeoJSONLegacyRawMessageCell(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "geom"},
		Rows: []engine.Row{{"id": 1, "geom": json.RawMessage(`{"type":"Point","coordinates":[5,6]}`)}},
	}
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, rs, "geom", Options{}); err != nil {
		t.Fatalf("ExportGeoJSON: %v", err)
	}
	var fc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fc); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	f0 := fc["features"].([]any)[0].(map[string]any)
	geom, ok := f0["geometry"].(map[string]any)
	if !ok || geom["type"] != "Point" {
		t.Errorf("legacy json.RawMessage cell did not round-trip: %v", f0["geometry"])
	}
}

func TestExportGeoJSONMalformedCellDoesNotAbort(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "geom"},
		Rows: []engine.Row{
			{"id": 1, "geom": "not json at all"},
			{"id": 2, "geom": `{"type":"Point","coordinates":[0,0]}`},
		},
	}
	var buf bytes.Buffer
	if err := ExportGeoJSON(&buf, rs, "geom", Options{}); err != nil {
		t.Fatalf("ExportGeoJSON: %v", err)
	}
	var fc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fc); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	features := fc["features"].([]any)
	if len(features) != 2 {
		t.Fatalf("got %d features, want 2 (malformed cell should degrade to null, not abort)", len(features))
	}
	if features[0].(map[string]any)["geometry"] != nil {
		t.Errorf("malformed geometry cell should become geometry:null, got %v", features[0].(map[string]any)["geometry"])
	}
}
