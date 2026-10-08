package engine

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func newGeoSQLTestDB(t *testing.T) *storage.DB {
	t.Helper()
	return storage.NewDB()
}

func executeGeoSQL(db *storage.DB, q string) (*ResultSet, error) {
	return Execute(context.Background(), db, "default", mustParse(q))
}

// gridGeometry returns random GeoJSON text on a coarse integer grid, so that
// touching boundaries, shared vertices, points on edges and nested rings are
// frequent rather than vanishingly rare.
func gridGeometry(rng *rand.Rand) string {
	pos := func() string { return fmt.Sprintf("[%d,%d]", rng.Intn(9), rng.Intn(9)) }
	rect := func() string {
		x0, y0 := rng.Intn(7), rng.Intn(7)
		x1, y1 := x0+1+rng.Intn(4), y0+1+rng.Intn(4)
		return fmt.Sprintf("[[%d,%d],[%d,%d],[%d,%d],[%d,%d],[%d,%d]]", x0, y0, x1, y0, x1, y1, x0, y1, x0, y0)
	}
	hole := func(x0, y0 int) string {
		return fmt.Sprintf("[[%d,%d],[%d,%d],[%d,%d],[%d,%d]]", x0, y0, x0+1, y0, x0+1, y0+1, x0, y0)
	}
	switch rng.Intn(7) {
	case 0:
		return fmt.Sprintf(`{"type":"Point","coordinates":%s}`, pos())
	case 1:
		var pts []string
		for i := 2 + rng.Intn(4); i > 0; i-- {
			pts = append(pts, pos())
		}
		return `{"type":"LineString","coordinates":[` + strings.Join(pts, ",") + `]}`
	case 2, 3:
		return `{"type":"Polygon","coordinates":[` + rect() + `]}`
	case 4:
		x0, y0 := rng.Intn(4), rng.Intn(4)
		outer := fmt.Sprintf("[[%d,%d],[%d,%d],[%d,%d],[%d,%d],[%d,%d]]", x0, y0, x0+5, y0, x0+5, y0+5, x0, y0+5, x0, y0)
		return `{"type":"Polygon","coordinates":[` + outer + `,` + hole(x0+1+rng.Intn(2), y0+1+rng.Intn(2)) + `]}`
	case 5:
		return `{"type":"MultiPolygon","coordinates":[[` + rect() + `],[` + rect() + `]]}`
	default:
		// Padded so that the text is long enough to enter the polygon cache.
		return `{ "type" : "Polygon" , "coordinates" : [ ` + rect() + ` ] }` + strings.Repeat(" ", geoPolyCacheMinText)
	}
}

func TestTypedIntersectsMatchesMapPath(t *testing.T) {
	rng := rand.New(rand.NewSource(14))
	typed, total := 0, 0
	for i := 0; i < 20000; i++ {
		a, b := gridGeometry(rng), gridGeometry(rng)
		aObj, err := geoObjectFromValue(a)
		if err != nil {
			t.Fatal(err)
		}
		bObj, err := geoObjectFromValue(b)
		if err != nil {
			t.Fatal(err)
		}
		aKind, err1 := classifyGeoRelateKind(aObj)
		bKind, err2 := classifyGeoRelateKind(bObj)
		if err1 != nil || err2 != nil {
			t.Fatalf("fixture produced an unsupported geometry: %s / %s", a, b)
		}
		want, err := geoIntersectsDispatch(aObj, aKind, bObj, bKind)
		if err != nil {
			t.Fatalf("map path failed on %s / %s: %v", a, b, err)
		}
		total++
		sa, okA := geoRelateShapeFromText(a)
		sb, okB := geoRelateShapeFromText(b)
		if !okA || !okB {
			continue // handled by the map path; nothing to compare
		}
		typed++
		if got := geoIntersectsShapes(sa, sb); got != want {
			t.Fatalf("typed path says %v, map path %v\n a = %s\n b = %s", got, want, a, b)
		}
	}
	if typed < total*9/10 {
		t.Fatalf("only %d of %d pairs took the typed path", typed, total)
	}
}

// Through SQL: results, NULL handling and errors are the same as before for
// valid, NULL and malformed arguments.
func TestSQLIntersectsTypedPathEdgeCases(t *testing.T) {
	cases := []struct {
		a, b string
		want any
		err  bool
	}{
		{`'{"type":"Point","coordinates":[1,1]}'`, `'{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}'`, true, false},
		{`'{"type":"Point","coordinates":[5,5]}'`, `'{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}'`, false, false},
		{`'{"type":"Point","coordinates":[2,1]}'`, `'{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}'`, true, false}, // on the boundary
		{`'{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}'`, `'{"type":"Point","coordinates":[1,1]}'`, true, false},
		{`NULL`, `'{"type":"Point","coordinates":[1,1]}'`, nil, false},
		{`'{"type":"Point","coordinates":[1,1]}'`, `NULL`, nil, false},
		{`'{"type":"Point","coordinates":[1,1]}'`, `'not geojson'`, nil, true},
		{`'{"type":"Polygon","coordinates":[[[0,0],[1,0],[0,0]]]}'`, `'{"type":"Point","coordinates":[1,1]}'`, nil, true},
		{`'{"type":"GeometryCollection","geometries":[]}'`, `'{"type":"Point","coordinates":[1,1]}'`, nil, true},
	}
	db := newGeoSQLTestDB(t)
	for _, c := range cases {
		for _, fn := range []string{"ST_INTERSECTS", "ST_DISJOINT"} {
			rs, err := executeGeoSQL(db, fmt.Sprintf(`SELECT %s(%s, %s) AS v`, fn, c.a, c.b))
			if c.err {
				if err == nil {
					t.Fatalf("%s(%s, %s): expected an error", fn, c.a, c.b)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s(%s, %s): %v", fn, c.a, c.b, err)
			}
			want := c.want
			if fn == "ST_DISJOINT" {
				if b, ok := want.(bool); ok {
					want = !b
				}
			}
			if rs.Rows[0]["v"] != want {
				t.Fatalf("%s(%s, %s) = %v, want %v", fn, c.a, c.b, rs.Rows[0]["v"], want)
			}
		}
	}
}
