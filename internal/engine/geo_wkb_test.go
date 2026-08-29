package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoWKBRoundTrip(t *testing.T) {
	db := storage.NewDB()
	geoms := []string{
		`{"type":"Point","coordinates":[1,2]}`,
		`{"type":"Point","coordinates":[1,2,3]}`,
		`{"type":"LineString","coordinates":[[0,0],[1,1],[2,0]]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`,
		`{"type":"MultiPoint","coordinates":[[10,40],[40,30]]}`,
		`{"type":"MultiLineString","coordinates":[[[10,10],[20,20]],[[40,40],[30,30]]]}`,
		`{"type":"MultiPolygon","coordinates":[[[[30,20],[45,40],[10,40],[30,20]]]]}`,
		`{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,1]},{"type":"LineString","coordinates":[[0,0],[1,1]]}]}`,
	}
	for _, g := range geoms {
		wkt1 := execScalarString(t, db, `SELECT ST_ASTEXT('`+g+`') AS v`)
		wkt2 := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMWKB(ST_ASBINARY('`+g+`'))) AS v`)
		if wkt1 != wkt2 {
			t.Errorf("WKB round-trip for %s: got %q, want %q", g, wkt2, wkt1)
		}
	}
}

func TestGeoWKBEWKBSRID(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT ST_ASBINARY(ST_MAKEPOINT(1,2)) AS plain, ST_ASEWKB(ST_MAKEPOINT(1,2)) AS ewkb`)
	plain, ok := rs.Rows[0]["plain"].([]byte)
	if !ok {
		t.Fatalf("ST_ASBINARY: expected []byte, got %T", rs.Rows[0]["plain"])
	}
	ewkb, ok := rs.Rows[0]["ewkb"].([]byte)
	if !ok {
		t.Fatalf("ST_ASEWKB: expected []byte, got %T", rs.Rows[0]["ewkb"])
	}
	if len(ewkb) != len(plain)+4 {
		t.Errorf("EWKB should be exactly 4 bytes (SRID) longer than plain WKB: plain=%d ewkb=%d", len(plain), len(ewkb))
	}

	// Round-trip through EWKB should reproduce the same geometry.
	got := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMEWKB(ST_ASEWKB(ST_MAKEPOINT(3,4)))) AS v`)
	if got != "POINT(3 4)" {
		t.Errorf("EWKB round-trip: got %q", got)
	}
}

func TestGeoWKBHexInput(t *testing.T) {
	db := storage.NewDB()
	hex := execScalarString(t, db, `SELECT HEX(ST_ASBINARY(ST_MAKEPOINT(5,6))) AS v`)
	got := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMWKB('`+hex+`')) AS v`)
	if got != "POINT(5 6)" {
		t.Errorf("hex WKB input: got %q", got)
	}
}

func TestGeoWKBTruncated(t *testing.T) {
	db := storage.NewDB()
	execExpectError(t, db, `SELECT ST_GEOMFROMWKB(UNHEX('0101000000')) AS v`) // point header but no coordinates
	execExpectError(t, db, `SELECT ST_GEOMFROMWKB(UNHEX('ff')) AS v`)         // invalid byte order marker
}
