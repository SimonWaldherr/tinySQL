package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func execRelateBool(t *testing.T, db *storage.DB, sql string) bool {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	v := rs.Rows[0]["v"]
	b, ok := v.(bool)
	if !ok {
		t.Fatalf("%s: result is %T (%v), want bool", sql, v, v)
	}
	return b
}

func execRelateErrors(t *testing.T, db *storage.DB, sql string) {
	t.Helper()
	if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err == nil {
		t.Errorf("%s: succeeded, want an error", sql)
	}
}

const (
	relateSquareA    = `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`
	relateSquareB    = `{"type":"Polygon","coordinates":[[[1,0],[2,0],[2,1],[1,1],[1,0]]]}` // shares the x=1 edge with squareA
	relateSquareFar  = `{"type":"Polygon","coordinates":[[[10,10],[11,10],[11,11],[10,11],[10,10]]]}`
	relateSquareOver = `{"type":"Polygon","coordinates":[[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}`
	relateBigSquare  = `{"type":"Polygon","coordinates":[[[-4,-4],[4,-4],[4,4],[-4,4],[-4,-4]]]}`
	relateSmallInner = `{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}`
)

func TestGeoIntersectsPolygons(t *testing.T) {
	db := storage.NewDB()

	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, relateSquareA, relateSquareOver)); !got {
		t.Errorf("overlapping squares: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, relateSquareA, relateSquareFar)); got {
		t.Errorf("far-apart squares: got %v, want false", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, relateSquareA, relateSquareB)); !got {
		t.Errorf("edge-sharing squares: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, relateBigSquare, relateSmallInner)); !got {
		t.Errorf("nested squares (no shared boundary): got %v, want true", got)
	}
}

// TestGeoIntersectsHoleNesting is the single most important regression test
// in this file: a polygon that sits entirely inside another polygon's hole
// must be reported as NOT intersecting (and DISJOINT). A naive
// "any vertex of A or B inside the other" check gets this backwards.
func TestGeoIntersectsHoleNesting(t *testing.T) {
	db := storage.NewDB()
	innerOfHole := `{"type":"Polygon","coordinates":[[[-0.5,-0.5],[0.5,-0.5],[0.5,0.5],[-0.5,0.5],[-0.5,-0.5]]]}`

	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, geoTestPolygonWithHole, innerOfHole)); got {
		t.Errorf("polygon inside a hole: ST_INTERSECTS = %v, want false", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_DISJOINT('%s', '%s') AS v`, geoTestPolygonWithHole, innerOfHole)); !got {
		t.Errorf("polygon inside a hole: ST_DISJOINT = %v, want true", got)
	}
}

func TestGeoIntersectsLines(t *testing.T) {
	db := storage.NewDB()
	crossA := `{"type":"LineString","coordinates":[[0,0],[2,2]]}`
	crossB := `{"type":"LineString","coordinates":[[0,2],[2,0]]}`
	touchA := `{"type":"LineString","coordinates":[[0,0],[1,1]]}`
	touchB := `{"type":"LineString","coordinates":[[1,1],[2,0]]}`
	parallelA := `{"type":"LineString","coordinates":[[0,0],[2,0]]}`
	parallelB := `{"type":"LineString","coordinates":[[0,1],[2,1]]}`
	collinearA := `{"type":"LineString","coordinates":[[0,0],[2,0]]}`
	collinearB := `{"type":"LineString","coordinates":[[1,0],[3,0]]}`

	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, crossA, crossB)); !got {
		t.Errorf("proper crossing: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, touchA, touchB)); !got {
		t.Errorf("endpoint touch: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, parallelA, parallelB)); got {
		t.Errorf("parallel non-touching lines: got %v, want false", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, collinearA, collinearB)); !got {
		t.Errorf("collinear overlapping lines: got %v, want true", got)
	}
}

func TestGeoIntersectsLinesAndPolygons(t *testing.T) {
	db := storage.NewDB()
	crossingLine := `{"type":"LineString","coordinates":[[0.5,-1],[0.5,2]]}`
	insideLine := `{"type":"LineString","coordinates":[[0.2,0.2],[0.8,0.8]]}`
	outsideLine := `{"type":"LineString","coordinates":[[10,10],[11,11]]}`

	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, crossingLine, relateSquareA)); !got {
		t.Errorf("line crossing polygon boundary: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, insideLine, relateSquareA)); !got {
		t.Errorf("line fully inside polygon: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, outsideLine, relateSquareA)); got {
		t.Errorf("line fully outside polygon: got %v, want false", got)
	}
}

func TestGeoIntersectsPointOnBoundary(t *testing.T) {
	db := storage.NewDB()
	edgeMidpoint := `{"type":"Point","coordinates":[0.5,0]}`
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, edgeMidpoint, relateSquareA)); !got {
		t.Errorf("point exactly on polygon edge: got %v, want true", got)
	}
}

func TestGeoIntersectsMultiPolygons(t *testing.T) {
	db := storage.NewDB()
	multiA := fmt.Sprintf(`{"type":"MultiPolygon","coordinates":[%s,%s]}`,
		`[[[0,0],[1,0],[1,1],[0,1],[0,0]]]`, `[[[100,100],[101,100],[101,101],[100,101],[100,100]]]`)
	multiB := fmt.Sprintf(`{"type":"MultiPolygon","coordinates":[%s,%s]}`,
		`[[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]`, `[[[200,200],[201,200],[201,201],[200,201],[200,200]]]`)
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, multiA, multiB)); !got {
		t.Errorf("multipolygons with one overlapping part-pair: got %v, want true", got)
	}
}

func TestGeoIntersectsRejectsGeometryCollection(t *testing.T) {
	db := storage.NewDB()
	gc := `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0,0]}]}`
	execRelateErrors(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, gc, relateSquareA))
	execRelateErrors(t, db, fmt.Sprintf(`SELECT ST_INTERSECTS('%s', '%s') AS v`, relateSquareA, gc))
}

func TestGeoDisjointBasic(t *testing.T) {
	db := storage.NewDB()
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_DISJOINT('%s', '%s') AS v`, relateSquareA, relateSquareOver)); got {
		t.Errorf("overlapping squares: ST_DISJOINT = %v, want false", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_DISJOINT('%s', '%s') AS v`, relateSquareA, relateSquareFar)); !got {
		t.Errorf("far-apart squares: ST_DISJOINT = %v, want true", got)
	}
}

func TestGeoEqualsPolygons(t *testing.T) {
	db := storage.NewDB()
	identical := relateSquareA
	rotatedStart := `{"type":"Polygon","coordinates":[[[1,0],[1,1],[0,1],[0,0],[1,0]]]}`
	reversedWinding := `{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}`
	asMultiPolygon := `{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,1],[0,0]]]]}`
	withHoleAdded := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]],[[0.1,0.1],[0.2,0.1],[0.2,0.2],[0.1,0.2],[0.1,0.1]]]}`
	extraCollinearVertex := `{"type":"Polygon","coordinates":[[[0,0],[0.5,0],[1,0],[1,1],[0,1],[0,0]]]}`

	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", identical, identical, true},
		{"rotated start vertex", identical, rotatedStart, true},
		{"reversed winding", identical, reversedWinding, true},
		{"polygon vs equivalent multipolygon", identical, asMultiPolygon, true},
		{"hole added on one side", identical, withHoleAdded, false},
		{"extra collinear vertex (documented v1 limitation)", identical, extraCollinearVertex, false},
	}
	for _, c := range cases {
		if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_EQUALS('%s', '%s') AS v`, c.a, c.b)); got != c.want {
			t.Errorf("%s: ST_EQUALS = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestGeoEqualsLinesAndPoints(t *testing.T) {
	db := storage.NewDB()
	lineForward := `{"type":"LineString","coordinates":[[0,0],[1,1],[2,0]]}`
	lineReversed := `{"type":"LineString","coordinates":[[2,0],[1,1],[0,0]]}`
	multiPointA := `{"type":"MultiPoint","coordinates":[[0,0],[1,1],[2,2]]}`
	multiPointB := `{"type":"MultiPoint","coordinates":[[2,2],[0,0],[1,1]]}`

	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_EQUALS('%s', '%s') AS v`, lineForward, lineReversed)); !got {
		t.Errorf("line vs its exact reverse: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_EQUALS('%s', '%s') AS v`, multiPointA, multiPointB)); !got {
		t.Errorf("multipoint with points in different order: got %v, want true", got)
	}
	if got := execRelateBool(t, db, fmt.Sprintf(`SELECT ST_EQUALS('%s', '%s') AS v`, relateSquareA, lineForward)); got {
		t.Errorf("polygon vs linestring: got %v, want false (different kind)", got)
	}
}
