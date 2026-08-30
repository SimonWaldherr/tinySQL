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

func TestParseGeometryRejectsEnvelopeOnEmptyGeometry(t *testing.T) {
	data := make([]byte, 8+32)
	copy(data, []byte{'G', 'P', 0, 0x13}) // little endian, empty, XY envelope
	binary.LittleEndian.PutUint32(data[4:], uint32(4326))
	if _, err := ParseGeometry(data); err == nil {
		t.Fatal("empty GeoPackageBinary geometry with an envelope should fail")
	}
}

func TestParseGeometryValidatesEveryEnvelopeOrdinate(t *testing.T) {
	tests := []struct {
		name   string
		flags  byte
		values []float64
	}{
		{"non-finite Z", 0x05, []float64{0, 1, 0, 1, math.NaN(), 2}},
		{"inverted Z", 0x05, []float64{0, 1, 0, 1, 3, 2}},
		{"non-finite M", 0x07, []float64{0, 1, 0, 1, 0, math.Inf(1)}},
		{"inverted M in XYZM", 0x09, []float64{0, 1, 0, 1, 0, 2, 4, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, 8+len(tc.values)*8)
			copy(data, []byte{'G', 'P', 0, tc.flags})
			binary.LittleEndian.PutUint32(data[4:], uint32(4326))
			for i, value := range tc.values {
				binary.LittleEndian.PutUint64(data[8+i*8:], math.Float64bits(value))
			}
			data = append(data, 1) // non-empty payload; header validation runs first
			if _, err := ParseGeometry(data); err == nil {
				t.Fatalf("ParseGeometry accepted invalid envelope %v", tc.values)
			}
		})
	}
}
