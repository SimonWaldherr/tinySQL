package exporter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/exporter"
	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestExportTopoJSONStructure(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "name", "geom"},
		Rows: []engine.Row{
			{"id": 1, "name": "berlin", "geom": `{"type":"Point","coordinates":[13.4,52.5]}`},
			{"id": 2, "name": "paris", "geom": `{"type":"Point","coordinates":[2.35,48.85]}`},
		},
	}
	var buf bytes.Buffer
	if err := exporter.ExportTopoJSON(&buf, rs, "geom", "", exporter.Options{}); err != nil {
		t.Fatalf("ExportTopoJSON: %v", err)
	}
	var topo map[string]any
	if err := json.Unmarshal(buf.Bytes(), &topo); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if topo["type"] != "Topology" {
		t.Fatalf("type = %v, want Topology", topo["type"])
	}
	objects := topo["objects"].(map[string]any)
	collection, ok := objects["collection"].(map[string]any)
	if !ok {
		t.Fatalf("objects.collection missing; objects = %v", objects)
	}
	if collection["type"] != "GeometryCollection" {
		t.Fatalf("objects.collection.type = %v, want GeometryCollection", collection["type"])
	}
	geometries := collection["geometries"].([]any)
	if len(geometries) != 2 {
		t.Fatalf("got %d child geometries, want 2", len(geometries))
	}
	g0 := geometries[0].(map[string]any)
	if g0["type"] != "Point" {
		t.Errorf("child 0 type = %v, want Point", g0["type"])
	}
	props := g0["properties"].(map[string]any)
	if props["name"] != "berlin" {
		t.Errorf("child 0 properties.name = %v, want berlin", props["name"])
	}
	if _, hasTransform := topo["transform"]; !hasTransform {
		t.Errorf("topology has no transform; quantization was expected to run")
	}
}

// TestExportTopoJSONSharedArcDedup checks that two polygons tracing the
// identical boundary (one forward, one reversed) collapse to a single
// shared arc, referenced forward by one geometry and reversed (~i) by the
// other -- the whole-ring dedup this exporter's v1 scope implements.
func TestExportTopoJSONSharedArcDedup(t *testing.T) {
	forward := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`
	reversed := `{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}`
	rs := &engine.ResultSet{
		Cols: []string{"id", "geom"},
		Rows: []engine.Row{
			{"id": 1, "geom": forward},
			{"id": 2, "geom": reversed},
		},
	}
	var buf bytes.Buffer
	if err := exporter.ExportTopoJSON(&buf, rs, "geom", "", exporter.Options{}); err != nil {
		t.Fatalf("ExportTopoJSON: %v", err)
	}
	var topo map[string]any
	if err := json.Unmarshal(buf.Bytes(), &topo); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	arcs := topo["arcs"].([]any)
	if len(arcs) != 1 {
		t.Fatalf("got %d arcs, want 1 (shared-arc dedup should collapse the reversed duplicate)", len(arcs))
	}

	geometries := topo["objects"].(map[string]any)["collection"].(map[string]any)["geometries"].([]any)
	arcsOf := func(g any) []any {
		return g.(map[string]any)["arcs"].([]any)[0].([]any)
	}
	ref0 := arcsOf(geometries[0])[0].(float64)
	ref1 := arcsOf(geometries[1])[0].(float64)
	if (ref0 >= 0) == (ref1 >= 0) {
		t.Errorf("expected one forward and one reversed arc reference, got %v and %v", ref0, ref1)
	}
}

// TestTopoJSONRoundTrip exports a small table (points with properties) as
// TopoJSON, re-imports it via ImportTopoJSON into a fresh DB, and checks
// that geometry and properties survive the round trip within the
// resolution 16-bit quantization guarantees.
func TestTopoJSONRoundTrip(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "city", "geom"},
		Rows: []engine.Row{
			{"id": 1, "city": "berlin", "geom": `{"type":"Point","coordinates":[13.405,52.52]}`},
			{"id": 2, "city": "paris", "geom": `{"type":"Point","coordinates":[2.3522,48.8566]}`},
		},
	}
	var buf bytes.Buffer
	if err := exporter.ExportTopoJSON(&buf, rs, "geom", "", exporter.Options{}); err != nil {
		t.Fatalf("ExportTopoJSON: %v", err)
	}

	db := storage.NewDB()
	result, err := importer.ImportTopoJSON(context.Background(), db, "default", "cities", &buf, &importer.ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportTopoJSON: %v", err)
	}
	if result.RowsInserted != 2 {
		t.Fatalf("RowsInserted = %d, want 2", result.RowsInserted)
	}

	tbl, err := db.Get("default", "cities")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 2 {
		t.Fatalf("imported %d rows, want 2", len(tbl.Rows))
	}

	cityIdx, err := tbl.ColIndex("city")
	if err != nil {
		t.Fatalf("city column: %v", err)
	}
	geomIdx, err := tbl.ColIndex("geometry")
	if err != nil {
		t.Fatalf("geometry column: %v", err)
	}

	want := map[string][2]float64{"berlin": {13.405, 52.52}, "paris": {2.3522, 48.8566}}
	for _, row := range tbl.Rows {
		city, _ := row[cityIdx].(string)
		wantCoord, ok := want[city]
		if !ok {
			t.Fatalf("unexpected city %q in imported rows", city)
		}
		var geomBytes []byte
		switch g := row[geomIdx].(type) {
		case string:
			geomBytes = []byte(g)
		case json.RawMessage:
			geomBytes = g
		default:
			t.Fatalf("geometry cell for %q has unexpected type %T", city, g)
		}
		var geom struct {
			Coordinates [2]float64 `json:"coordinates"`
		}
		if err := json.Unmarshal(geomBytes, &geom); err != nil {
			t.Fatalf("%q geometry is not valid JSON: %v", city, err)
		}
		if math.Abs(geom.Coordinates[0]-wantCoord[0]) > 1e-3 || math.Abs(geom.Coordinates[1]-wantCoord[1]) > 1e-3 {
			t.Errorf("%q round-tripped to %v, want approximately %v", city, geom.Coordinates, wantCoord)
		}
	}
}
