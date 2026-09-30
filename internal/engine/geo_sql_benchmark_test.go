package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// SQL-level GIS benchmark: one full scan of geoBenchRows rows per function, so
// ns/op divided by geoBenchRows is the per-row cost an application sees,
// including argument evaluation and GeoJSON decoding -- which is where most of
// the time goes for the small geometries typical of feature data.
const geoBenchRows = 2000

const (
	geoBenchRefPoint = `'{"type":"Point","coordinates":[10.5,51.5]}'`
	geoBenchRefPoly  = `'{"type":"Polygon","coordinates":[[[8,49],[13,49],[13,54],[8,54],[8,49]]]}'`
)

func geoBenchRing(cx, cy, r float64, n int) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i <= n; i++ {
		a := 2 * math.Pi * float64(i%n) / float64(n)
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "[%.5f,%.5f]", cx+r*math.Cos(a), cy+r*math.Sin(a))
	}
	sb.WriteString("]")
	return sb.String()
}

func geoBenchLine(cx, cy float64, n int) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"LineString","coordinates":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "[%.5f,%.5f]", cx+0.01*float64(i), cy+0.004*math.Sin(float64(i)/3))
	}
	sb.WriteString("]}")
	return sb.String()
}

func newGeoBenchDB(b *testing.B) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	run := func(q string) {
		if _, err := Execute(ctx, db, "default", mustParse(q)); err != nil {
			b.Fatalf("%s: %v", q, err)
		}
	}
	run(`CREATE TABLE g (id INT PRIMARY KEY, pt GEOMETRY, poly GEOMETRY, line GEOMETRY, wkt TEXT, ptxt TEXT)`)
	for i := 0; i < geoBenchRows; i++ {
		lon := 6 + float64(i%97)*0.08
		lat := 47 + float64(i%53)*0.14
		pt := fmt.Sprintf(`{"type":"Point","coordinates":[%.5f,%.5f]}`, lon, lat)
		poly := fmt.Sprintf(`{"type":"Polygon","coordinates":[%s]}`, geoBenchRing(lon, lat, 0.03, 16))
		line := geoBenchLine(lon, lat, 32)
		wkt := fmt.Sprintf("POINT(%.5f %.5f)", lon, lat)
		run(fmt.Sprintf(`INSERT INTO g VALUES (%d, '%s', '%s', '%s', '%s', '%s')`, i, pt, poly, line, wkt, pt))
	}
	return db
}

func BenchmarkGeoSQL(b *testing.B) {
	db := newGeoBenchDB(b)
	ctx := context.Background()
	cases := []struct{ name, expr string }{
		{"GEO_LON", `GEO_LON(pt)`},
		{"GEO_LON_text", `GEO_LON(ptxt)`},
		{"GEO_DISTANCE", `GEO_DISTANCE(pt, ` + geoBenchRefPoint + `)`},
		{"GEO_DWITHIN", `GEO_DWITHIN(pt, ` + geoBenchRefPoint + `, 50000)`},
		{"GEO_WITHIN_BBOX", `GEO_WITHIN_BBOX(pt, 8, 49, 13, 54)`},
		{"GEO_WITHIN_POLYGON", `GEO_WITHIN_POLYGON(pt, ` + geoBenchRefPoly + `)`},
		{"ST_CONTAINS_poly_pt", `ST_CONTAINS(poly, pt)`},
		{"GEO_POLYGON_AREA", `GEO_POLYGON_AREA(poly)`},
		{"GEO_LENGTH", `GEO_LENGTH(line)`},
		{"GEO_INTERSECTS_poly_poly", `GEO_INTERSECTS(poly, ` + geoBenchRefPoly + `)`},
		{"GEO_INTERSECTS_line_poly", `GEO_INTERSECTS(line, ` + geoBenchRefPoly + `)`},
		{"GEO_INTERSECTS_pt_poly", `GEO_INTERSECTS(pt, ` + geoBenchRefPoly + `)`},
		{"GEO_DISJOINT", `GEO_DISJOINT(pt, ` + geoBenchRefPoly + `)`},
		{"GEO_BUFFER", `GEO_BUFFER(pt, 500, 16)`},
		{"GEO_CENTROID", `GEO_CENTROID(poly)`},
		{"GEO_ENVELOPE", `GEO_ENVELOPE(poly)`},
		{"GEO_BBOX", `GEO_BBOX(poly)`},
		{"GEO_CONVEX_HULL", `GEO_CONVEX_HULL(poly)`},
		{"GEO_SIMPLIFY", `GEO_SIMPLIFY(line, 0.001, 'dp')`},
		{"GEO_AS_WKT", `GEO_AS_WKT(poly)`},
		{"GEO_FROM_WKT", `GEO_FROM_WKT(wkt)`},
		{"GEO_AS_GEOJSON", `GEO_AS_GEOJSON(poly)`},
		{"GEO_CLIP", `GEO_CLIP(poly, ` + geoBenchRefPoly + `)`},
		{"GEO_TRANSFORM", `GEO_TRANSFORM(pt, 3857)`},
		{"GEO_IS_VALID", `GEO_IS_VALID(poly)`},
		{"GEO_EQUALS", `GEO_EQUALS(poly, poly)`},
		{"GEO_BEARING", `GEO_BEARING(pt, ` + geoBenchRefPoint + `)`},
		{"GEO_MIDPOINT", `GEO_MIDPOINT(pt, ` + geoBenchRefPoint + `)`},
		{"GEO_DESTINATION", `GEO_DESTINATION(pt, 90, 10000)`},
		{"GEO_LINE_INTERPOLATE", `GEO_LINE_INTERPOLATE(line, 0.5)`},
		{"GEO_SNAP", `GEO_SNAP(pt, 0.001)`},
		{"GEO_TOUCHES", `GEO_TOUCHES(poly, ` + geoBenchRefPoly + `)`},
		{"GEO_COVERS", `GEO_COVERS(` + geoBenchRefPoly + `, pt)`},
		{"GEO_PERIMETER", `GEO_PERIMETER(poly)`},
		{"GEO_SMOOTH", `GEO_SMOOTH(line, 1)`},
		{"GEO_AS_WKB", `GEO_AS_WKB(poly)`},
	}
	for _, c := range cases {
		stmt, err := parseGeoBenchSQL(`SELECT ` + c.expr + ` AS v FROM g`)
		if err != nil {
			b.Logf("skip %s: %v", c.name, err)
			continue
		}
		if rs, err := Execute(ctx, db, "default", stmt); err != nil || len(rs.Rows) != geoBenchRows {
			b.Logf("skip %s: err=%v", c.name, err)
			continue
		}
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rs, err := Execute(ctx, db, "default", stmt)
				if err != nil {
					b.Fatal(err)
				}
				geoBenchSink = len(rs.Rows)
			}
		})
	}
}

var geoBenchSink int

func parseGeoBenchSQL(q string) (Statement, error) {
	p := NewParser(q)
	return p.ParseStatement()
}
