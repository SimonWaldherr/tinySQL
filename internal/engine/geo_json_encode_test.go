package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func checkGeoMarshal(t *testing.T, v any) {
	t.Helper()
	want, wantErr := json.Marshal(v)
	got, gotErr := marshalGeoJSON(v)
	if (wantErr == nil) != (gotErr == nil) {
		t.Fatalf("error mismatch for %#v: json=%v fast=%v", v, wantErr, gotErr)
	}
	if wantErr == nil && !bytes.Equal(got, want) {
		t.Fatalf("output differs for %#v:\n fast %s\n json %s", v, got, want)
	}
}

func TestMarshalGeoJSONFloatsMatchEncodingJSON(t *testing.T) {
	special := []float64{
		0, math.Copysign(0, -1), 1, -1, 0.5, 1e-6, 9.99999e-7, 1e-7, 1.5e-7, 1e-9, 5e-324, 1e20, 9.999999999999999e20, 1e21,
		1.5e21, 1e22, 1e100, math.MaxFloat64, math.SmallestNonzeroFloat64, 123456789, 0.1, 0.30000000000000004,
		13.405, 52.52, -179.99999999999997, 1 << 53, 1<<53 + 2, math.NaN(), math.Inf(1), math.Inf(-1),
	}
	for _, f := range special {
		checkGeoMarshal(t, f)
		checkGeoMarshal(t, []float64{f, 1})
		checkGeoMarshal(t, map[string]any{"c": []any{f}})
	}
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 300000; i++ {
		var f float64
		switch rng.Intn(4) {
		case 0:
			f = (rng.Float64() - 0.5) * 360
		case 1:
			f = rng.NormFloat64() * math.Pow(10, float64(rng.Intn(60)-30))
		case 2:
			f = float64(rng.Int63n(1<<40)) / math.Pow(10, float64(rng.Intn(12)))
		default:
			f = math.Float64frombits(rng.Uint64())
		}
		checkGeoMarshal(t, f)
	}
}

func randomGeoValue(rng *rand.Rand, depth int) any {
	switch n := rng.Intn(12); {
	case depth > 4 || n < 5:
		switch rng.Intn(9) {
		case 0:
			return nil
		case 1:
			return rng.Intn(2) == 0
		case 2:
			return float64(rng.Intn(2000) - 1000)
		case 3:
			return (rng.Float64() - 0.5) * 360
		case 4:
			return rng.Intn(1000)
		case 5:
			return int64(rng.Intn(1000) - 500)
		case 6:
			return []float64{rng.Float64(), rng.Float64() * 90}
		case 7:
			return [...]string{"Point", "Polygon", "type", "a b", "x<y", "q\"uote", "caf\u00e9", "tab\t", ""}[rng.Intn(9)]
		default:
			return []float64(nil)
		}
	case n < 8:
		arr := make([]any, rng.Intn(5))
		for i := range arr {
			arr[i] = randomGeoValue(rng, depth+1)
		}
		return arr
	default:
		m := map[string]any{}
		for i := rng.Intn(6); i > 0; i-- {
			m[[...]string{"type", "coordinates", "bbox", "geometries", "a", "Z", "b c", "k1", "k2"}[rng.Intn(9)]] = randomGeoValue(rng, depth+1)
		}
		return m
	}
}

func TestMarshalGeoJSONMatchesEncodingJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 20000; i++ {
		checkGeoMarshal(t, randomGeoValue(rng, 0))
	}
	// Types outside the fast set must still marshal identically via the fallback.
	checkGeoMarshal(t, map[string]any{"n": json.Number("12.5"), "s": []string{"a"}, "p": geoPoint{Lon: 1, Lat: 2}})
	checkGeoMarshal(t, map[string]any{"type": "Point", "coordinates": [][]float64{{1, 2}, {3, 4}}})
	checkGeoMarshal(t, geoJSONPoint{Coordinates: []float64{1, 2}, Type: "Point"})
	checkGeoMarshal(t, map[string]any(nil))
	checkGeoMarshal(t, []any(nil))
	checkGeoMarshal(t, []any{})
	checkGeoMarshal(t, map[string]any{})
}

func BenchmarkMarshalGeoJSON(b *testing.B) {
	ring := make([]any, 0, 17)
	for i := 0; i < 17; i++ {
		ring = append(ring, []float64{13.4 + float64(i)*0.01, 52.5 + float64(i)*0.013})
	}
	obj := map[string]any{"type": "Polygon", "coordinates": []any{ring}}
	b.Run("json.Marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(obj); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("marshalGeoJSON", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := marshalGeoJSON(obj); err != nil {
				b.Fatal(err)
			}
		}
	})
	_ = fmt.Sprint
}

func TestGeoPointJSONMatchesEncodingJSON(t *testing.T) {
	z := 12.5
	nan := math.NaN()
	for _, tc := range []struct {
		lon, lat float64
		z        *float64
	}{
		{13.405, 52.52, nil}, {13.405, 52.52, &z}, {0, 0, nil}, {-180, 90, &z}, {1e-7, -1e22, nil},
		{math.Copysign(0, -1), 5e-324, &z}, {nan, 1, nil}, {1, math.Inf(1), nil}, {1, 2, &nan},
	} {
		coords := []float64{tc.lon, tc.lat}
		if tc.z != nil {
			coords = append(coords, *tc.z)
		}
		want, wantErr := json.Marshal(geoJSONPoint{Coordinates: coords, Type: "Point"})
		got, gotErr := geoPointJSON(tc.lon, tc.lat, tc.z)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("%+v: error mismatch json=%v fast=%v", tc, wantErr, gotErr)
		}
		if wantErr == nil && got.(string) != string(want) {
			t.Fatalf("%+v:\n fast %s\n json %s", tc, got, want)
		}
	}
}

// CRS_INFO and GPKG_HEADER contain typed metadata alongside coordinates.
// Their complete response must use the fast encoder, rather than allocate a
// partial result and then repeat the entire encoding through reflection.
func TestMarshalGeoMetadata(t *testing.T) {
	for _, value := range []any{
		map[string]any{"axes": []string{"latitude", "longitude"}, "code": "4326"},
		map[string]any{"version": byte(0), "srid": int32(4326), "empty": false, "bbox": []float64{1, 2, 3, 4}},
		[]string(nil), []string{}, int32(-2147483648), uint8(255),
	} {
		checkGeoMarshal(t, value)
		if _, ok := appendGeoJSON(nil, value, 0); !ok {
			t.Fatalf("ordinary geometry metadata takes the fallback: %#v", value)
		}
	}
	// []byte must retain encoding/json's base64 semantics, and escaped or
	// non-ASCII axes must retain its string handling.
	checkGeoMarshal(t, []byte{1, 2, 3})
	checkGeoMarshal(t, []string{"nördlich", "a<b", "a\"b"})
}

func BenchmarkMarshalGeoMetadata(b *testing.B) {
	values := map[string]any{
		"CRS":  map[string]any{"authority": "EPSG", "code": "4326", "axes": []string{"latitude", "longitude"}, "unit": "degree"},
		"GPKG": map[string]any{"version": byte(0), "srid": int32(4326), "empty": false, "header_size": 40, "bbox": []float64{1, 2, 3, 4}},
	}
	for name, value := range values {
		for _, encoder := range []struct {
			name   string
			encode func(any) ([]byte, error)
		}{{"json.Marshal", json.Marshal}, {"marshalGeoJSON", marshalGeoJSON}} {
			b.Run(name+"/"+encoder.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := encoder.encode(value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
