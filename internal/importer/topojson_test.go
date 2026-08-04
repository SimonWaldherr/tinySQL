package importer

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A unit square arc, delta-encoded with a trivial (identity) transform so
// the expected absolute coordinates can be verified by hand: (0,0) -> +
// (1,0) -> (1,0); + (0,1) -> (1,1); + (-1,0) -> (0,1); + (0,-1) -> (0,0).
const topoUnitSquareDoc = `{
	"type": "Topology",
	"transform": {"scale": [1, 1], "translate": [0, 0]},
	"arcs": [[[0,0],[1,0],[0,1],[-1,0],[0,-1]]],
	"objects": {
		"regions": {
			"type": "GeometryCollection",
			"geometries": [
				{"type": "Polygon", "arcs": [[0]], "properties": {"name": "square1"}}
			]
		}
	}
}`

func TestImportTopoJSONResolvesArcs(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	res, err := ImportTopoJSON(ctx, db, "default", "regions", strings.NewReader(topoUnitSquareDoc), &ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		t.Fatalf("ImportTopoJSON: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d, want 1", res.RowsInserted)
	}

	tbl, err := db.Get("default", "regions")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(tbl.Rows))
	}
	nameIdx, err := tbl.ColIndex("name")
	if err != nil {
		t.Fatalf("name column: %v", err)
	}
	if tbl.Rows[0][nameIdx] != "square1" {
		t.Errorf("name = %v, want square1", tbl.Rows[0][nameIdx])
	}
	geomIdx, err := tbl.ColIndex("geometry")
	if err != nil {
		t.Fatalf("geometry column: %v", err)
	}
	geomBytes, ok := tbl.Rows[0][geomIdx].(json.RawMessage)
	if !ok {
		t.Fatalf("geometry = %T, want json.RawMessage", tbl.Rows[0][geomIdx])
	}
	var geom struct {
		Type        string        `json:"type"`
		Coordinates [][][]float64 `json:"coordinates"`
	}
	if err := json.Unmarshal(geomBytes, &geom); err != nil {
		t.Fatalf("geometry JSON: %v", err)
	}
	if geom.Type != "Polygon" {
		t.Fatalf("geometry type = %v, want Polygon", geom.Type)
	}
	want := [][]float64{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}
	got := geom.Coordinates[0]
	if len(got) != len(want) {
		t.Fatalf("ring has %d positions, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Errorf("position %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestImportTopoJSONReversedArcReference(t *testing.T) {
	doc := `{
		"type": "Topology",
		"transform": {"scale": [1, 1], "translate": [0, 0]},
		"arcs": [[[0,0],[1,0],[0,1],[-1,0],[0,-1]]],
		"objects": {
			"regions": {
				"type": "GeometryCollection",
				"geometries": [
					{"type": "Polygon", "arcs": [[-1]], "properties": {}}
				]
			}
		}
	}`
	ctx := context.Background()
	db := storage.NewDB()
	if _, err := ImportTopoJSON(ctx, db, "default", "regions", strings.NewReader(doc), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportTopoJSON: %v", err)
	}
	tbl, err := db.Get("default", "regions")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	geomIdx, err := tbl.ColIndex("geometry")
	if err != nil {
		t.Fatalf("geometry column: %v", err)
	}
	var geom struct {
		Coordinates [][][]float64 `json:"coordinates"`
	}
	if err := json.Unmarshal(tbl.Rows[0][geomIdx].(json.RawMessage), &geom); err != nil {
		t.Fatalf("geometry JSON: %v", err)
	}
	want := [][]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}
	got := geom.Coordinates[0]
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("reversed ring position %d = %v, want %v (full ring: %v)", i, got[i], want[i], got)
		}
	}
}

func TestImportTopoJSONMultiObjectTagging(t *testing.T) {
	doc := `{
		"type": "Topology",
		"arcs": [],
		"objects": {
			"cities": {"type": "Point", "coordinates": [1, 2], "properties": {"name": "a"}},
			"borders": {"type": "Point", "coordinates": [3, 4], "properties": {"name": "b"}}
		}
	}`
	ctx := context.Background()
	db := storage.NewDB()
	if _, err := ImportTopoJSON(ctx, db, "default", "mixed", strings.NewReader(doc), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportTopoJSON: %v", err)
	}
	tbl, err := db.Get("default", "mixed")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 2 {
		t.Fatalf("rows = %d, want 2 (one per top-level object)", len(tbl.Rows))
	}
	topoObjIdx, err := tbl.ColIndex("topo_object")
	if err != nil {
		t.Fatalf("importing 2 top-level objects without a filter should add a topo_object column: %v", err)
	}
	seen := map[string]bool{}
	for _, row := range tbl.Rows {
		v, _ := row[topoObjIdx].(string)
		seen[v] = true
	}
	if !seen["cities"] || !seen["borders"] {
		t.Errorf("topo_object values = %v, want both cities and borders tagged", seen)
	}
}

func TestImportTopoJSONObjectFilter(t *testing.T) {
	doc := `{
		"type": "Topology",
		"arcs": [],
		"objects": {
			"cities": {"type": "Point", "coordinates": [1, 2], "properties": {"name": "a"}},
			"borders": {"type": "Point", "coordinates": [3, 4], "properties": {"name": "b"}}
		}
	}`
	ctx := context.Background()
	db := storage.NewDB()
	if _, err := ImportTopoJSON(ctx, db, "default", "cities_only", strings.NewReader(doc),
		&ImportOptions{CreateTable: true, TopoJSONObject: "cities"}); err != nil {
		t.Fatalf("ImportTopoJSON: %v", err)
	}
	tbl, err := db.Get("default", "cities_only")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (filtered to a single object)", len(tbl.Rows))
	}
	if _, err := tbl.ColIndex("topo_object"); err == nil {
		t.Errorf("a single explicitly-filtered object should not get a topo_object column")
	}
}
