package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func execScalar(t *testing.T, db *storage.DB, sql string) any {
	t.Helper()
	rs := execSQL(t, db, sql)
	if len(rs.Rows) != 1 {
		t.Fatalf("%s: expected 1 row, got %d", sql, len(rs.Rows))
	}
	return rs.Rows[0]["v"]
}

func execScalarString(t *testing.T, db *storage.DB, sql string) string {
	t.Helper()
	v := execScalar(t, db, sql)
	s, ok := v.(string)
	if !ok {
		t.Fatalf("%s: expected string, got %T (%v)", sql, v, v)
	}
	return s
}

func execExpectError(t *testing.T, db *storage.DB, sql string) {
	t.Helper()
	if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err == nil {
		t.Errorf("%s: succeeded, want an error", sql)
	}
}

func TestGeoWKTRoundTrip(t *testing.T) {
	db := storage.NewDB()
	cases := []struct {
		wkt  string
		want string // expected ST_ASTEXT output; "" means "same as wkt"
	}{
		{"POINT(30 10)", ""},
		{"POINT (30 10)", "POINT(30 10)"},
		{"LINESTRING(30 10, 10 30, 40 40)", "LINESTRING(30 10,10 30,40 40)"},
		{"POLYGON((30 10,40 40,20 40,10 20,30 10))", ""},
		{"POLYGON((35 10,45 45,15 40,10 20,35 10),(20 30,35 35,30 20,20 30))", ""},
		{"MULTIPOINT(10 40,40 30,20 20,30 10)", ""},
		{"MULTILINESTRING((10 10,20 20,10 40),(40 40,30 30,40 20,30 10))", ""},
		{"MULTIPOLYGON(((30 20,45 40,10 40,30 20)),((15 5,40 10,10 20,5 10,15 5)))", ""},
		{"GEOMETRYCOLLECTION(POINT(40 10),LINESTRING(10 10,20 20,10 40))", ""},
		{"POINT Z (1 2 3)", "POINT Z(1 2 3)"},
		{"POINT EMPTY", ""},
		{"LINESTRING EMPTY", ""},
	}
	for _, c := range cases {
		want := c.want
		if want == "" {
			want = c.wkt
		}
		got := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMTEXT('`+c.wkt+`')) AS v`)
		if got != want {
			t.Errorf("round-trip %q: got %q, want %q", c.wkt, got, want)
		}
	}
}

func TestGeoWKTEWKTSRID(t *testing.T) {
	db := storage.NewDB()
	got := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMTEXT('SRID=4326;POINT(1 2)')) AS v`)
	if got != "POINT(1 2)" {
		t.Errorf("got %q", got)
	}
	execExpectError(t, db, `SELECT ST_GEOMFROMTEXT('SRID=3857;POINT(1 2)') AS v`)

	ewkt := execScalarString(t, db, `SELECT ST_ASEWKT(ST_MAKEPOINT(1,2)) AS v`)
	if ewkt != "SRID=4326;POINT(1 2)" {
		t.Errorf("ST_ASEWKT: got %q", ewkt)
	}
}

func TestGeoWKTMultiPointBothForms(t *testing.T) {
	db := storage.NewDB()
	a := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMTEXT('MULTIPOINT(10 40, 40 30)')) AS v`)
	b := execScalarString(t, db, `SELECT ST_ASTEXT(ST_GEOMFROMTEXT('MULTIPOINT((10 40), (40 30))')) AS v`)
	if a != b {
		t.Errorf("bare vs parenthesized MULTIPOINT diverged: %q vs %q", a, b)
	}
}

func TestGeoWKTTypedConstructors(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `SELECT ST_POINTFROMTEXT('POINT(1 2)') AS v`)
	execExpectError(t, db, `SELECT ST_POINTFROMTEXT('LINESTRING(0 0,1 1)') AS v`)
	execExpectError(t, db, `SELECT ST_LINEFROMTEXT('POINT(1 2)') AS v`)
}

func TestGeoWKTMalformed(t *testing.T) {
	db := storage.NewDB()
	bad := []string{
		`SELECT ST_GEOMFROMTEXT('NOTAGEOM(1 2)') AS v`,
		`SELECT ST_GEOMFROMTEXT('POINT(1)') AS v`,
		`SELECT ST_GEOMFROMTEXT('POINT(1 2') AS v`,
		`SELECT ST_GEOMFROMTEXT('LINESTRING(0 0, 1 1 1)') AS v`, // ragged dimensionality
		`SELECT ST_GEOMFROMTEXT('POINT(1 2) extra') AS v`,
	}
	for _, sql := range bad {
		execExpectError(t, db, sql)
	}
}

func TestGeoAsGeoJSONRounding(t *testing.T) {
	db := storage.NewDB()
	got := execScalarString(t, db, `SELECT ST_ASGEOJSON(ST_MAKEPOINT(1.23456789, 2.3456789), 2) AS v`)
	if !strings.Contains(got, "1.23") || !strings.Contains(got, "2.35") {
		t.Errorf("rounded GeoJSON: got %q", got)
	}
	execExpectError(t, db, `SELECT ST_ASGEOJSON(ST_MAKEPOINT(1,2), 16) AS v`)
}

func TestGeoFromGeoJSONRejectsFeature(t *testing.T) {
	db := storage.NewDB()
	execExpectError(t, db, `SELECT ST_GEOMFROMGEOJSON('{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{}}') AS v`)
}
