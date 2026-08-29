package engine

import (
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoHashEncodeDecode(t *testing.T) {
	db := storage.NewDB()
	// Encoding then decoding at high precision should land within a tiny
	// fraction of a degree of the original point (well inside a single
	// 12-character cell, which is centimeter-scale).
	rs := execSQL(t, db, `SELECT GEO_LON(GEO_GEOHASH_DECODE(GEO_GEOHASH_ENCODE(ST_MAKEPOINT(-122.6, 45.6), 12))) AS lon, GEO_LAT(GEO_GEOHASH_DECODE(GEO_GEOHASH_ENCODE(ST_MAKEPOINT(-122.6, 45.6), 12))) AS lat`)
	lon, _ := rs.Rows[0]["lon"].(float64)
	lat, _ := rs.Rows[0]["lat"].(float64)
	if math.Abs(lon-(-122.6)) > 1e-6 || math.Abs(lat-45.6) > 1e-6 {
		t.Errorf("round-trip drifted too far: lon=%v lat=%v, want approximately (-122.6, 45.6)", lon, lat)
	}

	// A well-known reference: geohash.org's own worked example, "9q8yyk"
	// decodes to a point in San Francisco.
	rs2 := execSQL(t, db, `SELECT GEO_LON(GEO_GEOHASH_DECODE('9q8yyk')) AS lon, GEO_LAT(GEO_GEOHASH_DECODE('9q8yyk')) AS lat`)
	sfLon, _ := rs2.Rows[0]["lon"].(float64)
	sfLat, _ := rs2.Rows[0]["lat"].(float64)
	if sfLon < -123 || sfLon > -121 || sfLat < 36 || sfLat > 38 {
		t.Errorf("decoded 9q8yyk far from San Francisco: lon=%v lat=%v", sfLon, sfLat)
	}
}

func TestGeoHashPrecisionArg(t *testing.T) {
	db := storage.NewDB()
	five := execScalarString(t, db, `SELECT GEO_GEOHASH_ENCODE(ST_MAKEPOINT(13.4,52.5), 5) AS v`)
	if len(five) != 5 {
		t.Errorf("precision 5: got length %d (%q)", len(five), five)
	}
	twelve := execScalarString(t, db, `SELECT GEO_GEOHASH_ENCODE(ST_MAKEPOINT(13.4,52.5), 12) AS v`)
	if len(twelve) != 12 {
		t.Errorf("precision 12: got length %d (%q)", len(twelve), twelve)
	}
	if twelve[:5] != five {
		t.Errorf("longer encoding should share the shorter one's prefix: %q vs %q", twelve, five)
	}
}

func TestGeoHashBBoxContainsPoint(t *testing.T) {
	db := storage.NewDB()
	hash := execScalarString(t, db, `SELECT GEO_GEOHASH_ENCODE(ST_MAKEPOINT(13.4,52.5), 8) AS v`)
	rs := execSQL(t, db, `SELECT GEO_GEOHASH_BBOX('`+hash+`') AS v`)
	bbox, ok := rs.Rows[0]["v"].([]float64)
	if !ok {
		t.Fatalf("GEO_GEOHASH_BBOX: expected []float64, got %T", rs.Rows[0]["v"])
	}
	if !(bbox[0] <= 13.4 && 13.4 <= bbox[2] && bbox[1] <= 52.5 && 52.5 <= bbox[3]) {
		t.Errorf("bbox %v does not contain the encoded point", bbox)
	}
}

func TestGeoHashNeighborsIncludesSelf(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT GEO_GEOHASH_NEIGHBORS('u33dc') AS v`)
	neighbors, ok := rs.Rows[0]["v"].([]any)
	if !ok {
		t.Fatalf("GEO_GEOHASH_NEIGHBORS: expected []any, got %T", rs.Rows[0]["v"])
	}
	if len(neighbors) != 9 {
		t.Fatalf("expected 9 cells (self + 8 neighbors), got %d", len(neighbors))
	}
	if neighbors[4] != "u33dc" {
		t.Errorf("center element (index 4) should be the input hash itself: got %v", neighbors[4])
	}
}

func TestGeoHashInvalidCharacter(t *testing.T) {
	db := storage.NewDB()
	execExpectError(t, db, `SELECT GEO_GEOHASH_DECODE('abcli') AS v`) // 'a','i','l' are excluded from the alphabet
	execExpectError(t, db, `SELECT GEO_GEOHASH_ENCODE(ST_MAKEPOINT(200,52.5)) AS v`)
}
