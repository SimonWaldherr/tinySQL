//go:build sqliteimport && !js && !wasm && !baremetal

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildMapshaperShapes(t *testing.T) {
	shapes := buildMapshaperShapes()
	if len(shapes) != 3 {
		t.Fatalf("buildMapshaperShapes() returned %d shapes, want 3", len(shapes))
	}

	wantKinds := map[string]string{
		"coastline":   "LineString",
		"lagoon":      "Polygon with a hole",
		"archipelago": "MultiPolygon",
	}
	seen := make(map[string]bool)
	for _, shape := range shapes {
		if seen[shape.Name] {
			t.Fatalf("duplicate shape name %q", shape.Name)
		}
		seen[shape.Name] = true
		if shape.Kind != wantKinds[shape.Name] {
			t.Errorf("shape %q kind = %q, want %q", shape.Name, shape.Kind, wantKinds[shape.Name])
		}

		var geometry struct {
			Type        string          `json:"type"`
			Coordinates json.RawMessage `json:"coordinates"`
		}
		if err := json.Unmarshal([]byte(shape.Geometry), &geometry); err != nil {
			t.Errorf("shape %q has invalid GeoJSON: %v", shape.Name, err)
			continue
		}
		if geometry.Type == "" || len(geometry.Coordinates) == 0 {
			t.Errorf("shape %q has incomplete GeoJSON", shape.Name)
		}
	}
}

func TestMapshaperShapeDemoQueries(t *testing.T) {
	db := tinysql.NewDB()
	if err := addMapshaperShapesTable(db); err != nil {
		t.Fatalf("addMapshaperShapesTable: %v", err)
	}

	operations := []string{
		"geometry",
		"ST_SIMPLIFY(geometry, 2, 'visvalingam-weighted')",
		"ST_SMOOTH(geometry, 1)",
		"ST_AFFINE(geometry, 8, 0, 0.8, 18)",
		"ST_REMOVE_HOLES(geometry)",
	}
	for _, operation := range operations {
		sql := fmt.Sprintf(
			"WITH selected AS (SELECT geometry AS source_geometry, %s AS geometry FROM mapshaper_shapes WHERE name = 'lagoon') "+
				"SELECT source_geometry, geometry, ST_BBOX(geometry) AS bbox, ST_CENTROID(geometry) AS centroid FROM selected",
			operation,
		)
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", operation, err)
		}
		result, err := tinysql.Execute(context.Background(), db, "default", stmt)
		if err != nil {
			t.Fatalf("execute %q: %v", operation, err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("%q returned %d rows, want 1", operation, len(result.Rows))
		}
		for _, column := range []string{"source_geometry", "geometry", "bbox", "centroid"} {
			if value, ok := result.Rows[0][column].(string); !ok || value == "" {
				t.Errorf("%q returned no %s", operation, column)
			}
		}
	}
}
