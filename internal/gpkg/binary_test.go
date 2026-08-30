package gpkg

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestParseGeometryXYEnvelope(t *testing.T) {
	data := make([]byte, 8+32)
	copy(data, []byte{'G', 'P', 0, 0x03}) // little endian, XY envelope
	binary.LittleEndian.PutUint32(data[4:], uint32(25832))
	for i, value := range []float64{100, 120, 200, 230} {
		binary.LittleEndian.PutUint64(data[8+i*8:], math.Float64bits(value))
	}
	data = append(data, 1, 1, 0, 0, 0) // enough to prove payload splitting

	g, err := ParseGeometry(data)
	if err != nil {
		t.Fatal(err)
	}
	if g.SRID != 25832 || g.HeaderLen != 40 || g.Empty || g.Extended {
		t.Fatalf("header = %#v", g)
	}
	want := []float64{100, 200, 120, 230}
	for i := range want {
		if g.BBox[i] != want[i] {
			t.Fatalf("bbox = %v, want %v", g.BBox, want)
		}
	}
	if len(g.WKB) != 5 || g.WKB[0] != 1 {
		t.Fatalf("WKB = %x", g.WKB)
	}
}

func TestParseGeometryBigEndianEmpty(t *testing.T) {
	data := []byte{'G', 'P', 0, 0x10, 0, 0, 0x10, 0xe6} // SRID 4326
	g, err := ParseGeometry(data)
	if err != nil {
		t.Fatal(err)
	}
	if !g.Empty || g.SRID != 4326 || len(g.WKB) != 0 {
		t.Fatalf("geometry = %#v", g)
	}
}

func TestParseGeometryRejectsMalformedHeaders(t *testing.T) {
	cases := [][]byte{
		{},
		[]byte("not-gpkg"),
		{'G', 'P', 1, 0, 0, 0, 0, 0},
		{'G', 'P', 0, 0xc0, 0, 0, 0, 0},
		{'G', 'P', 0, 0x0a, 0, 0, 0, 0}, // invalid envelope indicator 5
		{'G', 'P', 0, 0, 0, 0, 0, 0},    // non-empty, missing WKB
	}
	for _, data := range cases {
		if _, err := ParseGeometry(data); err == nil {
			t.Errorf("ParseGeometry(%x) should fail", data)
		}
	}
}
