package engine

import (
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoTransformKnownValue(t *testing.T) {
	db := storage.NewDB()
	// (0,0) in WGS84 is the Web Mercator origin (0,0).
	rs := execSQL(t, db, `SELECT GEO_X(GEO_TRANSFORM(ST_MAKEPOINT(0,0), 3857)) AS x, GEO_Y(GEO_TRANSFORM(ST_MAKEPOINT(0,0), 3857)) AS y`)
	x, _ := rs.Rows[0]["x"].(float64)
	y, _ := rs.Rows[0]["y"].(float64)
	if math.Abs(x) > 1e-6 || math.Abs(y) > 1e-6 {
		t.Errorf("origin should map to (0,0): got (%v, %v)", x, y)
	}
}

func TestGeoTransformInverseRoundTrip(t *testing.T) {
	db := storage.NewDB()
	lon, lat := 13.4050, 52.5200
	sql := `SELECT GEO_LON(v) AS lon, GEO_LAT(v) AS lat FROM (SELECT ST_TRANSFORM(ST_TRANSFORM(ST_MAKEPOINT(13.4050, 52.5200), 3857), 4326) AS v) t`
	rs := execSQL(t, db, sql)
	gotLon, _ := rs.Rows[0]["lon"].(float64)
	gotLat, _ := rs.Rows[0]["lat"].(float64)
	if math.Abs(gotLon-lon) > 1e-6 {
		t.Errorf("lon round-trip: got %v, want %v", gotLon, lon)
	}
	if math.Abs(gotLat-lat) > 1e-6 {
		t.Errorf("lat round-trip: got %v, want %v", gotLat, lat)
	}
}

func TestGeoTransformUnsupportedSRID(t *testing.T) {
	db := storage.NewDB()
	execExpectError(t, db, `SELECT ST_TRANSFORM(ST_MAKEPOINT(1,2), 2154) AS v`)
}

func TestGeoTransformClampsPoles(t *testing.T) {
	db := storage.NewDB()
	// A latitude beyond Web Mercator's +/-85.05 bound must clamp, not diverge to Inf/NaN.
	rs := execSQL(t, db, `SELECT GEO_Y(ST_TRANSFORM(ST_MAKEPOINT(0,89.9), 3857)) AS y`)
	y, ok := rs.Rows[0]["y"].(float64)
	if !ok || math.IsInf(y, 0) || math.IsNaN(y) {
		t.Errorf("expected a finite clamped y, got %v (%T)", rs.Rows[0]["y"], rs.Rows[0]["y"])
	}
}
