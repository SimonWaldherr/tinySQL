package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// slowDecodeObject is the reference: exactly what geoObjectFromJSON did before
// the fast decoder existed.
func slowDecodeObject(body string) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// checkFastObject asserts the contract: whenever the fast decoder accepts, the
// reference decoder accepts too and produces an identical value.
func checkFastObject(t *testing.T, body string) (accepted bool) {
	t.Helper()
	got, ok := decodeJSONObjectFast(body)
	if !ok {
		return false
	}
	want, err := slowDecodeObject(body)
	if err != nil {
		t.Fatalf("fast decoder accepted input encoding/json rejects (%v): %q", err, body)
	}
	if want == nil {
		t.Fatalf("fast decoder returned a map for input that decodes to nil: %q", body)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fast decode differs for %q:\n fast %#v\n slow %#v", body, got, want)
	}
	return true
}

var fastJSONCases = []string{
	`{"type":"Point","coordinates":[13.405,52.52]}`,
	` { "type" : "Point" , "coordinates" : [ 13.405 , 52.52 , 34.5 ] } `,
	"{\n\t\"type\":\"Polygon\",\r\n\"coordinates\":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}",
	`{"coordinates":[[0,0],[1,1]],"type":"LineString"}`,
	`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]],[[[5,5],[6,5],[6,6],[5,5]]]]}`,
	`{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]},{"type":"LineString","coordinates":[[0,0],[1,1]]}]}`,
	`{}`, `{"a":[]}`, `{"a":{}}`, `{"a":null,"b":true,"c":false}`,
	`{"n":0}`, `{"n":-0}`, `{"n":0.5}`, `{"n":-0.5e-7}`, `{"n":1E5}`, `{"n":1e+5}`, `{"n":123456789012345678901234567890}`,
	`{"n":1.7976931348623157e308}`, `{"n":5e-324}`, `{"n":1e999}`, `{"n":-1e999}`,
	`{"n":01}`, `{"n":-}`, `{"n":1.}`, `{"n":.5}`, `{"n":1e}`, `{"n":1e+}`, `{"n":+1}`, `{"n":--1}`, `{"n":0x10}`,
	`{"type":"Point","type":"Polygon"}`, `{"a":1,"a":2}`,
	`{"s":"caf\u00e9"}`, `{"s":"café"}`, `{"s":"a\"b"}`, "{\"s\":\"tab\there\"}", `{"s":"a\\b"}`,
	`{"a":[1,2,]}`, `{"a":[,1]}`, `{"a":1,}`, `{,}`, `{"a"}`, `{"a":}`, `{a:1}`, `{'a':1}`,
	`{"a":1} x`, `{"a":1}{"b":2}`, `[1,2]`, `null`, `"str"`, `1`, ``, `   `, `{`, `{"a":`, `{"a":[1,2`,
	`{"a":tru}`, `{"a":nul}`, `{"a":True}`, `{"a":NaN}`, `{"a":Infinity}`,
	"\ufeff{\"a\":1}", `{"a":1}` + "\x00",
	`{"a":[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]}`,
}

func TestFastJSONObjectCases(t *testing.T) {
	accepted := 0
	for _, body := range fastJSONCases {
		if checkFastObject(t, body) {
			accepted++
		}
	}
	// The compact GeoJSON forms must actually take the fast path.
	for _, body := range fastJSONCases[:6] {
		if _, ok := decodeJSONObjectFast(body); !ok {
			t.Fatalf("fast decoder declined ordinary GeoJSON: %q", body)
		}
	}
	if accepted == 0 {
		t.Fatal("fast decoder accepted nothing")
	}
}

func randomJSONValue(rng *rand.Rand, depth int) any {
	switch n := rng.Intn(10); {
	case depth > 4 || n < 4:
		switch rng.Intn(6) {
		case 0:
			return nil
		case 1:
			return rng.Intn(2) == 0
		case 2:
			return float64(rng.Intn(1000) - 500)
		case 3:
			return (rng.Float64() - 0.5) * math.Pow(10, float64(rng.Intn(40)-20))
		case 4:
			return fmt.Sprintf("s%d", rng.Intn(100))
		default:
			return "Pt \"q\" é\n"
		}
	case n < 7:
		arr := make([]any, rng.Intn(5))
		for i := range arr {
			arr[i] = randomJSONValue(rng, depth+1)
		}
		return arr
	default:
		m := map[string]any{}
		for i := rng.Intn(5); i > 0; i-- {
			m[fmt.Sprintf("k%d", rng.Intn(20))] = randomJSONValue(rng, depth+1)
		}
		return m
	}
}

// perturb damages valid JSON the way fuzzers do, to exercise the rejection
// paths of the fast decoder against encoding/json.
func perturb(rng *rand.Rand, s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	switch rng.Intn(5) {
	case 0:
		i := rng.Intn(len(b))
		b = append(b[:i], b[i+1:]...)
	case 1:
		i := rng.Intn(len(b))
		b = append(b[:i+1], b[i:]...)
	case 2:
		b[rng.Intn(len(b))] = "{}[],:\"\\ 0-e.tfn"[rng.Intn(16)]
	case 3:
		b = b[:rng.Intn(len(b))]
	default:
		i := rng.Intn(len(b))
		b = append(b[:i], append([]byte(" \n"), b[i:]...)...)
	}
	return string(b)
}

func TestFastJSONObjectRandomAgainstEncodingJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	accepted := 0
	for i := 0; i < 6000; i++ {
		obj := map[string]any{}
		for k := rng.Intn(4); k >= 0; k-- {
			obj[fmt.Sprintf("k%d", k)] = randomJSONValue(rng, 0)
		}
		for _, enc := range []func() ([]byte, error){
			func() ([]byte, error) { return json.Marshal(obj) },
			func() ([]byte, error) { return json.MarshalIndent(obj, " ", "\t") },
		} {
			body, err := enc()
			if err != nil {
				t.Fatal(err)
			}
			text := string(body)
			if checkFastObject(t, text) {
				accepted++
			}
			checkFastObject(t, perturb(rng, text))
			checkFastObject(t, perturb(rng, perturb(rng, text)))
		}
	}
	if accepted < 1000 {
		t.Fatalf("only %d random documents took the fast path; the test is not exercising it", accepted)
	}
}

func FuzzFastJSONObject(f *testing.F) {
	for _, c := range fastJSONCases {
		f.Add(c)
	}
	f.Fuzz(func(t *testing.T, body string) { checkFastObject(t, body) })
}

// The typed decoders must agree with the map-based path they shortcut: same
// value whenever they accept, and never accept what that path would reject.
func TestFastTypedGeoDecodersMatchMapPath(t *testing.T) {
	bodies := []string{
		`{"type":"Point","coordinates":[13.405,52.52]}`,
		`{"coordinates":[13.405,52.52,7],"type":"Point"}`,
		`{"type":"point","coordinates":[1,2]}`,
		`{"type":"POINT","coordinates":[1,2,3,4]}`,
		`{"type":"Point","coordinates":[1]}`,
		`{"type":"Point","coordinates":[]}`,
		`{"type":"Point","coordinates":[1,"2"]}`,
		`{"type":"Point","coordinates":[[1,2]]}`,
		`{"type":"LineString","coordinates":[[0,0],[1,1],[2,0]]}`,
		`{"type":"LineString","coordinates":[[0,0]]}`,
		`{"type":"linestring","coordinates":[[0,0,5],[1,1]]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]],[[.2,.2],[.4,.2],[.4,.4],[.2,.2]]]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[0,0]]]}`,
		`{"type":"Polygon","coordinates":[]}`,
		`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]],[[[5,5],[6,5],[6,6],[5,5]]]]}`,
		`{"type":"MultiPolygon","coordinates":[]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]],"bbox":[0,0,1,1]}`,
		`{"type":"Feature","coordinates":[1,2]}`,
		` {"type":"Polygon", "coordinates": [ [ [0,0] , [1,0] , [1,1] , [0,0] ] ] } `,
		`{"type":"Point","coordinates":[1e400,2]}`,
		`{"type":"Point","coordinates":[1,2]`,
		`{"type":"Point","coordinates":[1,2]]}`,
	}
	for _, body := range bodies {
		// Reference: the slow path, forced by handing it a decoded map.
		obj, objErr := slowDecodeObject(body)

		if p, ok := decodeGeoPointFast(body); ok {
			if objErr != nil {
				t.Fatalf("point fast path accepted invalid JSON %q", body)
			}
			want, err := geoPointFromMap(obj)
			if err != nil || !reflect.DeepEqual(p, want) {
				t.Fatalf("point %q: fast %+v, slow %+v (%v)", body, p, want, err)
			}
		}
		if ls, ok := decodeGeoLineStringFast(body); ok {
			if objErr != nil {
				t.Fatalf("linestring fast path accepted invalid JSON %q", body)
			}
			positions, _ := obj["coordinates"].([]any)
			if typ, _ := obj["type"].(string); !strings.EqualFold(typ, "LineString") || len(positions) < 2 {
				t.Fatalf("linestring fast path accepted %q that the slow path rejects", body)
			}
			want := make(geoLineString, 0, len(positions))
			for _, raw := range positions {
				p, err := geoPositionFromValue(raw)
				if err != nil {
					t.Fatalf("linestring %q: slow position error %v but fast accepted", body, err)
				}
				want = append(want, p)
			}
			if !reflect.DeepEqual(ls, want) {
				t.Fatalf("linestring %q: fast %+v, slow %+v", body, ls, want)
			}
		}
		if mp, ok := decodeGeoMultiPolygonFast(body); ok {
			if objErr != nil {
				t.Fatalf("polygon fast path accepted invalid JSON %q", body)
			}
			want, err := geoMultiPolygonFromValue(obj)
			if err != nil || !reflect.DeepEqual(mp, want) {
				t.Fatalf("polygon %q: fast %+v, slow %+v (%v)", body, mp, want, err)
			}
		}
	}

	// The shapes the hot functions rely on must not silently fall off the fast path.
	for _, body := range []string{bodies[0], bodies[8], bodies[11], bodies[15]} {
		ok := false
		switch {
		case strings.Contains(body, "Point"):
			_, ok = decodeGeoPointFast(body)
		case strings.Contains(body, "LineString"):
			_, ok = decodeGeoLineStringFast(body)
		default:
			_, ok = decodeGeoMultiPolygonFast(body)
		}
		if !ok {
			t.Fatalf("typed fast decoder declined ordinary geometry %q", body)
		}
	}
}

// The exact-arithmetic shortcut in fastJSON.number must agree with
// strconv.ParseFloat bit for bit, on every number it accepts.
func TestFastJSONNumberMatchesParseFloat(t *testing.T) {
	check := func(text string) {
		t.Helper()
		s := fastJSON{b: text}
		got, ok := s.number()
		if !ok {
			return
		}
		// number stops at the end of a valid JSON number ("00" is the number 0
		// followed by junk the caller rejects), so compare the consumed prefix.
		prefix := text[:s.i]
		want, err := strconv.ParseFloat(prefix, 64)
		if err != nil {
			t.Fatalf("number(%q) accepted %q but ParseFloat fails: %v", text, prefix, err)
		}
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("number(%q) = %v (%#x), ParseFloat = %v (%#x)", text, got, math.Float64bits(got), want, math.Float64bits(want))
		}
	}
	for _, text := range []string{
		"0", "-0", "0.0", "-0.0", "0.5", "-0.5", "1", "10", "100", "0.1", "0.2", "0.3", "1.1", "13.405", "52.52",
		"123456.789012", "9007199254740992", "9007199254740993", "9007199254740991.5", "12345678901234567890",
		"0.000001", "0.0000001", "1e22", "1e23", "1e-22", "1e-23", "1.5e3", "1.5E-3", "2.5e+10", "0e0", "0e5", "00",
		"4.35", "0.07", "5e-324", "1.7976931348623157e308", "99999999999999999999", "0.30000000000000004",
		"179.99999999999997", "-179.99999999999997", "0.1e1", "100e-2", "1000000e-6",
	} {
		check(text)
	}
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 400000; i++ {
		var text string
		switch rng.Intn(5) {
		case 0:
			text = strconv.FormatFloat((rng.Float64()-0.5)*360, 'f', rng.Intn(10), 64)
		case 1:
			text = strconv.FormatFloat(rng.NormFloat64()*math.Pow(10, float64(rng.Intn(30)-15)), 'g', -1, 64)
		case 2:
			text = fmt.Sprintf("%d.%0*d", rng.Intn(1000), 1+rng.Intn(18), rng.Int63n(1000000000000000000))
		case 3:
			text = fmt.Sprintf("%de%d", rng.Int63n(1<<uint(1+rng.Intn(62))), rng.Intn(50)-25)
		default:
			text = strconv.FormatFloat(math.Float64frombits(rng.Uint64()), 'g', -1, 64)
		}
		check(text)
		if len(text) > 1 && text[0] != '-' {
			check("-" + text)
		}
	}
}
