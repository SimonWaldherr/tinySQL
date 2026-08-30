package geoencoding

import (
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeWKBPointByteOrdersAndDimensions(t *testing.T) {
	srid3857 := uint32(3857)
	tests := []struct {
		name       string
		marker     byte
		rawType    uint32
		srid       *uint32
		ordinates  []float64
		wantCoords []any
		wantSRIDs  []uint32
	}{
		{"little endian 2D", 1, 1, nil, []float64{11, 48}, []any{11.0, 48.0}, nil},
		{"big endian 2D", 0, 1, nil, []float64{12, 49}, []any{12.0, 49.0}, nil},
		{"ISO Z", 1, 1001, nil, []float64{11, 48, 500}, []any{11.0, 48.0, 500.0}, nil},
		{"ISO M", 1, 2001, nil, []float64{11, 48, 7}, []any{11.0, 48.0}, nil},
		{"ISO ZM", 1, 3001, nil, []float64{11, 48, 500, 7}, []any{11.0, 48.0, 500.0}, nil},
		{"EWKB ZM and SRID", 1, ewkbZFlag | ewkbMFlag | ewkbSRIDFlag | 1, &srid3857, []float64{1, 2, 3, 4}, []any{1.0, 2.0, 3.0}, []uint32{3857}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DecodeWKB(testWKBPoint(tc.marker, tc.rawType, tc.srid, tc.ordinates...))
			if err != nil {
				t.Fatal(err)
			}
			if result.Geometry["type"] != "Point" || !reflect.DeepEqual(result.Geometry["coordinates"], tc.wantCoords) {
				t.Fatalf("geometry = %#v, want coordinates %#v", result.Geometry, tc.wantCoords)
			}
			if !reflect.DeepEqual(result.SRIDs, tc.wantSRIDs) {
				t.Fatalf("SRIDs = %v, want %v", result.SRIDs, tc.wantSRIDs)
			}
		})
	}
}

func TestDecodeWKBAllCoreGeometryTypes(t *testing.T) {
	p1 := testWKBPoint(1, 1, nil, 0, 0)
	p2 := testWKBPoint(0, 1, nil, 1, 1)
	line := testWKBLineString(1, [][2]float64{{0, 0}, {1, 1}})
	ring := [][2]float64{{0, 0}, {2, 0}, {2, 2}, {0, 0}}
	polygon := testWKBPolygon(1, [][][2]float64{ring})

	tests := []struct {
		name string
		body []byte
		want string
	}{
		{"LineString", line, "LineString"},
		{"Polygon", polygon, "Polygon"},
		{"MultiPoint", testWKBCollection(1, 4, p1, p2), "MultiPoint"},
		{"MultiLineString", testWKBCollection(0, 5, line), "MultiLineString"},
		{"MultiPolygon", testWKBCollection(1, 6, polygon), "MultiPolygon"},
		{"GeometryCollection", testWKBCollection(1, 7, p1, line, polygon), "GeometryCollection"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DecodeWKB(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			if result.Geometry["type"] != tc.want {
				t.Fatalf("type = %v, want %s", result.Geometry["type"], tc.want)
			}
		})
	}
}

func TestDecodeWKBNestedSRIDs(t *testing.T) {
	srid4326 := uint32(4326)
	srid3857 := uint32(3857)
	body := testWKBCollection(1, 7,
		testWKBPoint(1, ewkbSRIDFlag|1, &srid4326, 11, 48),
		testWKBPoint(0, ewkbSRIDFlag|1, &srid3857, 1, 2),
	)
	result, err := DecodeWKB(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.SRIDs, []uint32{4326, 3857}) {
		t.Fatalf("nested SRIDs = %v", result.SRIDs)
	}
}

func TestDecodeWKBRejectsMalformedInput(t *testing.T) {
	unsupported := testWKBHeader(1, 99, nil)
	trailing := append(testWKBPoint(1, 1, nil, 1, 2), 0xff)
	nonFinite := testWKBPoint(1, 1, nil, math.NaN(), 2)
	tooManyPositions := append(testWKBHeader(1, 2, nil), 0xff, 0xff, 0xff, 0xff)
	wrongMultiMember := testWKBCollection(1, 4, testWKBLineString(1, [][2]float64{{0, 0}, {1, 1}}))

	tests := []struct {
		name string
		body []byte
		want string
	}{
		{"empty", nil, "unexpected end"},
		{"invalid byte order", []byte{2}, "invalid byte order"},
		{"unsupported type", unsupported, "unsupported WKB geometry type"},
		{"trailing data", trailing, "trailing byte"},
		{"non-finite coordinate", nonFinite, "non-finite coordinate"},
		{"oversized position count", tooManyPositions, "position count"},
		{"wrong MultiPoint member", wrongMultiMember, "MultiPoint member"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeWKB(tc.body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeWKB error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestDecodeWKBRejectsExcessiveNesting(t *testing.T) {
	body := testWKBPoint(1, 1, nil, 0, 0)
	for i := 0; i < maxWKBDepth+2; i++ {
		body = testWKBCollection(1, 7, body)
	}
	if _, err := DecodeWKB(body); err == nil || !strings.Contains(err.Error(), "nested more than") {
		t.Fatalf("nesting error = %v", err)
	}
}

func testWKBPoint(marker byte, rawType uint32, srid *uint32, ordinates ...float64) []byte {
	body := testWKBHeader(marker, rawType, srid)
	for _, ordinate := range ordinates {
		body = testWKBAppendFloat64(body, marker, ordinate)
	}
	return body
}

func testWKBLineString(marker byte, positions [][2]float64) []byte {
	body := testWKBHeader(marker, 2, nil)
	body = testWKBAppendUint32(body, marker, uint32(len(positions)))
	for _, position := range positions {
		body = testWKBAppendFloat64(body, marker, position[0])
		body = testWKBAppendFloat64(body, marker, position[1])
	}
	return body
}

func testWKBPolygon(marker byte, rings [][][2]float64) []byte {
	body := testWKBHeader(marker, 3, nil)
	body = testWKBAppendUint32(body, marker, uint32(len(rings)))
	for _, ring := range rings {
		body = testWKBAppendUint32(body, marker, uint32(len(ring)))
		for _, position := range ring {
			body = testWKBAppendFloat64(body, marker, position[0])
			body = testWKBAppendFloat64(body, marker, position[1])
		}
	}
	return body
}

func testWKBCollection(marker byte, rawType uint32, children ...[]byte) []byte {
	body := testWKBHeader(marker, rawType, nil)
	body = testWKBAppendUint32(body, marker, uint32(len(children)))
	for _, child := range children {
		body = append(body, child...)
	}
	return body
}

func testWKBHeader(marker byte, rawType uint32, srid *uint32) []byte {
	body := []byte{marker}
	body = testWKBAppendUint32(body, marker, rawType)
	if srid != nil {
		body = testWKBAppendUint32(body, marker, *srid)
	}
	return body
}

func testWKBAppendUint32(body []byte, marker byte, value uint32) []byte {
	var encoded [4]byte
	testWKBOrder(marker).PutUint32(encoded[:], value)
	return append(body, encoded[:]...)
}

func testWKBAppendFloat64(body []byte, marker byte, value float64) []byte {
	var encoded [8]byte
	testWKBOrder(marker).PutUint64(encoded[:], math.Float64bits(value))
	return append(body, encoded[:]...)
}

func testWKBOrder(marker byte) binary.ByteOrder {
	if marker == 0 {
		return binary.BigEndian
	}
	return binary.LittleEndian
}
