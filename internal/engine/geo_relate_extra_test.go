package engine

import (
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func execScalarBool(t *testing.T, db *storage.DB, sql string) bool {
	t.Helper()
	v := execScalar(t, db, sql)
	b, ok := v.(bool)
	if !ok {
		t.Fatalf("%s: result is %T (%v), want bool", sql, v, v)
	}
	return b
}

func TestGeoTouchesPolygons(t *testing.T) {
	db := storage.NewDB()
	// squareA and squareB share the full edge x=1, y:0..1 -- touching, not overlapping.
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, relateSquareA, relateSquareB)); !got {
		t.Errorf("edge-sharing squares: got %v, want true", got)
	}
	// squareOver genuinely overlaps squareA in area -- not a touch.
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, relateSquareA, relateSquareOver)); got {
		t.Errorf("overlapping squares: got %v, want false", got)
	}
	// Far-apart squares don't even intersect -- not a touch.
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, relateSquareA, relateSquareFar)); got {
		t.Errorf("disjoint squares: got %v, want false", got)
	}
	// Nested (fully-contained) squares: interiors overlap, no boundary contact.
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, relateBigSquare, relateSmallInner)); got {
		t.Errorf("nested squares: got %v, want false", got)
	}
	// Corner-touching squares (share exactly one point).
	cornerA := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`
	cornerB := `{"type":"Polygon","coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]]}`
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, cornerA, cornerB)); !got {
		t.Errorf("corner-touching squares: got %v, want true", got)
	}
}

func TestGeoTouchesPointsAndLines(t *testing.T) {
	db := storage.NewDB()
	line := `{"type":"LineString","coordinates":[[0,0],[10,0]]}`
	endpoint := `{"type":"Point","coordinates":[0,0]}`
	midpoint := `{"type":"Point","coordinates":[5,0]}`
	off := `{"type":"Point","coordinates":[5,5]}`

	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, endpoint, line)); !got {
		t.Errorf("point at line endpoint: got %v, want true", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, midpoint, line)); got {
		t.Errorf("point in line interior: got %v, want false (interior contact, not a touch)", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, off, line)); got {
		t.Errorf("disjoint point: got %v, want false", got)
	}

	poly := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`
	onBoundary := `{"type":"Point","coordinates":[0.5,0]}`
	inside := `{"type":"Point","coordinates":[0.5,0.5]}`
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, onBoundary, poly)); !got {
		t.Errorf("point on polygon boundary: got %v, want true", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, inside, poly)); got {
		t.Errorf("point inside polygon: got %v, want false", got)
	}
}

func TestGeoTouchesLinesLines(t *testing.T) {
	db := storage.NewDB()
	a := `{"type":"LineString","coordinates":[[0,0],[1,0]]}`
	bEndpoint := `{"type":"LineString","coordinates":[[1,0],[1,1]]}`
	bCross := `{"type":"LineString","coordinates":[[0.5,-1],[0.5,1]]}`
	bCollinearOverlap := `{"type":"LineString","coordinates":[[0.5,0],[1.5,0]]}`

	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, a, bEndpoint)); !got {
		t.Errorf("endpoint-touching lines: got %v, want true", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, a, bCross)); got {
		t.Errorf("crossing lines: got %v, want false", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_TOUCHES('%s', '%s') AS v`, a, bCollinearOverlap)); got {
		t.Errorf("collinear overlapping lines: got %v, want false (interior overlap, not a touch)", got)
	}
}

func TestGeoCovers(t *testing.T) {
	db := storage.NewDB()
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_COVERS('%s', '%s') AS v`, relateBigSquare, relateSmallInner)); !got {
		t.Errorf("big square covers small inner square: got %v, want true", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_COVEREDBY('%s', '%s') AS v`, relateSmallInner, relateBigSquare)); !got {
		t.Errorf("small square covered by big square: got %v, want true", got)
	}
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_COVERS('%s', '%s') AS v`, relateSmallInner, relateBigSquare)); got {
		t.Errorf("small square does not cover big square: got %v, want false", got)
	}
	// A polygon covers its own boundary point.
	poly := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`
	corner := `{"type":"Point","coordinates":[0,0]}`
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_COVERS('%s', '%s') AS v`, poly, corner)); !got {
		t.Errorf("polygon covers its own corner point: got %v, want true", got)
	}
	outside := `{"type":"Point","coordinates":[5,5]}`
	if got := execScalarBool(t, db, fmt.Sprintf(`SELECT ST_COVERS('%s', '%s') AS v`, poly, outside)); got {
		t.Errorf("polygon does not cover an outside point: got %v, want false", got)
	}
}

func TestGeoPerimeter(t *testing.T) {
	db := storage.NewDB()
	// A ~1-degree-square polygon near the equator: perimeter should be
	// roughly 4 * 111.2km, loosely bounded to tolerate the haversine formula's
	// exact value without hard-coding it.
	square := `{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}`
	perim := execScalar(t, db, fmt.Sprintf(`SELECT ST_PERIMETER('%s') AS v`, square))
	f, ok := perim.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", perim)
	}
	want := 4 * 111200.0
	if math.Abs(f-want) > want*0.05 {
		t.Errorf("perimeter: got %v, want approximately %v", f, want)
	}
}
