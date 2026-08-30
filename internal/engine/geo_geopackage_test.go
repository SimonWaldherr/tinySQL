package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

func gpkgPointBlob(t *testing.T, srid int32, x, y float64) []byte {
	t.Helper()
	wkb, err := geoJSONToWKB(map[string]any{"type": "Point", "coordinates": []any{x, y}}, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 40, 40+len(wkb))
	copy(body, []byte{'G', 'P', 0, 0x03}) // little endian + XY envelope
	binary.LittleEndian.PutUint32(body[4:], uint32(srid))
	for i, value := range []float64{x, x, y, y} {
		binary.LittleEndian.PutUint64(body[8+i*8:], math.Float64bits(value))
	}
	return append(body, wkb...)
}

func TestGeoPackageScalarFunctions(t *testing.T) {
	body := gpkgPointBlob(t, 4326, 11.5, 48.25)
	hex := fmt.Sprintf("%x", body)
	if got := tileEval(t, "GPKG_SRID(BLOB_FROM_HEX('"+hex+"'))"); got != int64(4326) {
		t.Fatalf("GPKG_SRID = %v", got)
	}
	if got := tileEval(t, "GPKG_IS_EMPTY(BLOB_FROM_HEX('"+hex+"'))"); got != false {
		t.Fatalf("GPKG_IS_EMPTY = %v", got)
	}
	var bbox []float64
	if err := json.Unmarshal([]byte(tileEval(t, "GPKG_BBOX(BLOB_FROM_HEX('"+hex+"'))").(string)), &bbox); err != nil {
		t.Fatal(err)
	}
	if len(bbox) != 4 || bbox[0] != 11.5 || bbox[1] != 48.25 || bbox[2] != 11.5 || bbox[3] != 48.25 {
		t.Fatalf("GPKG_BBOX = %v", bbox)
	}
	if got := tileEval(t, "ST_ASTEXT(GEO_FROM_GPKG(BLOB_FROM_HEX('"+hex+"')))"); got != "POINT(11.5 48.25)" {
		t.Fatalf("GEO_FROM_GPKG = %v", got)
	}
	wkb := tileEval(t, "GPKG_AS_WKB(BLOB_FROM_HEX('"+hex+"'))")
	if fmt.Sprintf("%x", wkb) != fmt.Sprintf("%x", body[40:]) {
		t.Fatalf("GPKG_AS_WKB = %x, want %x", wkb, body[40:])
	}
}

func TestGeoPackageDoesNotRelabelProjectedCoordinates(t *testing.T) {
	body := gpkgPointBlob(t, 25832, 691875, 5335575)
	hex := fmt.Sprintf("%x", body)
	if err := tileEvalErr(t, "GEO_FROM_GPKG(BLOB_FROM_HEX('"+hex+"'))"); err == nil {
		t.Fatal("GEO_FROM_GPKG should reject a projected CRS instead of treating metres as lon/lat")
	}
	if got := tileEval(t, "GPKG_SRID(BLOB_FROM_HEX('"+hex+"'))"); got != int64(25832) {
		t.Fatalf("projected GPKG_SRID = %v", got)
	}
}

func TestGeoWKBISODimensions(t *testing.T) {
	// ISO SQL/MM Point ZM type 3001. The M value is consumed but GeoJSON has
	// nowhere to store it; Z remains the third coordinate.
	body := make([]byte, 1+4+4*8)
	body[0] = 1
	binary.LittleEndian.PutUint32(body[1:], 3001)
	for i, value := range []float64{11, 48, 500, 12345} {
		binary.LittleEndian.PutUint64(body[5+i*8:], math.Float64bits(value))
	}
	object, err := DecodeWKBGeometry(body)
	if err != nil {
		t.Fatal(err)
	}
	coords := object["coordinates"].([]any)
	if len(coords) != 3 || coords[0] != 11.0 || coords[1] != 48.0 || coords[2] != 500.0 {
		t.Fatalf("ISO ZM point coordinates = %#v", coords)
	}
}
