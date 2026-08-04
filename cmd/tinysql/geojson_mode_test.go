package main

import (
	"bytes"
	"encoding/json"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func geoModeSample() *tsql.ResultSet {
	return &tsql.ResultSet{
		Cols: []string{"id", "name", "geom"},
		Rows: []tsql.Row{
			{"id": 1, "name": "berlin", "geom": `{"type":"Point","coordinates":[13.4,52.5]}`},
			{"id": 2, "name": "paris", "geom": `{"type":"Point","coordinates":[2.35,48.85]}`},
		},
	}
}

func TestGetPrinterGeoJSONAndTopoJSON(t *testing.T) {
	if _, ok := getPrinter(ModeGeoJSON).(*GeoJSONPrinter); !ok {
		t.Errorf("getPrinter(%q) did not return a *GeoJSONPrinter", ModeGeoJSON)
	}
	if _, ok := getPrinter(ModeTopoJSON).(*TopoJSONPrinter); !ok {
		t.Errorf("getPrinter(%q) did not return a *TopoJSONPrinter", ModeTopoJSON)
	}
}

func TestGeoJSONPrinterOutput(t *testing.T) {
	cfg := &Config{Mode: ModeGeoJSON, GeomCol: "geom"}
	var buf bytes.Buffer
	if err := (&GeoJSONPrinter{}).Print(&buf, geoModeSample(), cfg); err != nil {
		t.Fatalf("GeoJSONPrinter.Print: %v", err)
	}
	var fc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &fc); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if fc["type"] != "FeatureCollection" {
		t.Errorf("type = %v, want FeatureCollection", fc["type"])
	}
	if got := len(fc["features"].([]any)); got != 2 {
		t.Errorf("got %d features, want 2", got)
	}
}

func TestGeoJSONPrinterAutoDetectsColumn(t *testing.T) {
	cfg := &Config{Mode: ModeGeoJSON} // GeomCol left empty -> auto-detect
	var buf bytes.Buffer
	if err := (&GeoJSONPrinter{}).Print(&buf, geoModeSample(), cfg); err != nil {
		t.Fatalf("GeoJSONPrinter.Print with auto-detect: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"Point"`)) {
		t.Errorf("auto-detected output missing geometry: %s", buf.String())
	}
}

func TestTopoJSONPrinterOutput(t *testing.T) {
	cfg := &Config{Mode: ModeTopoJSON, GeomCol: "geom"}
	var buf bytes.Buffer
	if err := (&TopoJSONPrinter{}).Print(&buf, geoModeSample(), cfg); err != nil {
		t.Fatalf("TopoJSONPrinter.Print: %v", err)
	}
	var topo map[string]any
	if err := json.Unmarshal(buf.Bytes(), &topo); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if topo["type"] != "Topology" {
		t.Errorf("type = %v, want Topology", topo["type"])
	}
	objects, ok := topo["objects"].(map[string]any)
	if !ok {
		t.Fatalf("objects missing: %v", topo)
	}
	collection, ok := objects["collection"].(map[string]any)
	if !ok {
		t.Fatalf("objects.collection missing: %v", objects)
	}
	geometries, ok := collection["geometries"].([]any)
	if !ok || len(geometries) != 2 {
		t.Fatalf("objects.collection.geometries = %v, want 2 entries", collection["geometries"])
	}
}
