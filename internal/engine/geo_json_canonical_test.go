package engine

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// checkCanonicalFast asserts the contract: whenever the fast canonicalizer
// accepts, the original route accepts too and returns identical text.
func checkCanonicalFast(t *testing.T, text string) (accepted bool) {
	t.Helper()
	got, ok := canonicalGeoJSONFast(text)
	if !ok {
		return false
	}
	want, err := canonicalGeoJSONSlow(text)
	if err != nil {
		t.Fatalf("fast canonicalizer accepted input the original route rejects (%v): %q", err, text)
	}
	if got != want {
		t.Fatalf("canonical text differs for %q:\n fast %s\n slow %s", text, got, want)
	}
	return true
}

func randomCoordText(rng *rand.Rand) string {
	f := (rng.Float64() - 0.5) * 360
	switch rng.Intn(16) {
	case 0:
		return strconv.Itoa(rng.Intn(400) - 200)
	case 1:
		return strconv.FormatFloat(f, 'f', rng.Intn(8), 64)
	case 2:
		return strconv.FormatFloat(f, 'f', -1, 64)
	case 3:
		return strconv.FormatFloat(f, 'g', -1, 64)
	case 4:
		return strconv.FormatFloat(f, 'e', rng.Intn(6), 64)
	case 5:
		return strconv.FormatFloat(f, 'f', 15, 64) // trailing digits, long
	case 6:
		return strconv.FormatFloat(f, 'f', 17, 64)
	case 7:
		return [...]string{"0", "-0", "0.0", "-0.0", "0.5", "-0.5", "1.0", "10", "100", "1e2", "1E-2", "0.10", "5.250"}[rng.Intn(13)]
	case 8:
		return [...]string{"0.000001", "0.0000001", "0.00000012345", "0.00001", "0.000010", "1e-6", "1e-7", "-0.0000005"}[rng.Intn(8)]
	case 9:
		return [...]string{"1e20", "1e21", "1.5e21", "123456789012345", "1234567890123456", "12345678901234567890", "100000000000000000000"}[rng.Intn(7)]
	case 10:
		return fmt.Sprintf("%d.%d", rng.Intn(180), rng.Int63n(1000000000000000))
	default:
		return strconv.FormatFloat(f, 'f', 4+rng.Intn(5), 64)
	}
}

func randomPositionText(rng *rand.Rand) string {
	n := 2
	switch rng.Intn(12) {
	case 0:
		n = 3
	case 1:
		n = 4
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = randomCoordText(rng)
	}
	sep := [...]string{",", ", ", " , ", ",\n"}[rng.Intn(4)]
	open, closer := [...]string{"[", "[ ", "[\t"}[rng.Intn(3)], [...]string{"]", " ]"}[rng.Intn(2)]
	return open + strings.Join(parts, sep) + closer
}

func randomGroupText(rng *rand.Rand, depth int) string {
	if depth == 0 {
		return randomPositionText(rng)
	}
	n := rng.Intn(6)
	if rng.Intn(10) != 0 {
		n = 1 + rng.Intn(5)
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = randomGroupText(rng, depth-1)
	}
	return "[" + strings.Join(parts, [...]string{",", ", ", " ,\n"}[rng.Intn(3)]) + "]"
}

func randomGeoJSONText(rng *rand.Rand) string {
	types := []struct {
		name  string
		depth int
	}{
		{"Point", 0}, {"MultiPoint", 1}, {"LineString", 1}, {"MultiLineString", 2}, {"Polygon", 2}, {"MultiPolygon", 3},
		{"point", 0}, {"POLYGON", 2}, {"Feature", 0}, {"Circle", 1},
	}
	tp := types[rng.Intn(len(types))]
	coords := randomGroupText(rng, tp.depth)
	switch rng.Intn(6) {
	case 0:
		return fmt.Sprintf(`{"coordinates":%s,"type":"%s"}`, coords, tp.name)
	case 1:
		return fmt.Sprintf(` { "type" : "%s" , "coordinates" : %s } `, tp.name, coords)
	case 2:
		return fmt.Sprintf(`{"type":"%s","coordinates":%s,"bbox":[0,0,1,1]}`, tp.name, coords)
	default:
		return fmt.Sprintf(`{"type":"%s","coordinates":%s}`, tp.name, coords)
	}
}

func TestCanonicalGeoJSONFastMatchesOriginalRoute(t *testing.T) {
	rng := rand.New(rand.NewSource(61))
	accepted := 0
	for i := 0; i < 40000; i++ {
		text := randomGeoJSONText(rng)
		if checkCanonicalFast(t, text) {
			accepted++
		}
		checkCanonicalFast(t, perturb(rng, text))
	}
	if accepted < 20000 {
		t.Fatalf("only %d of 40000 documents took the fast path; the test is not exercising it", accepted)
	}
}

func TestCanonicalGeoJSONFastNumberFormats(t *testing.T) {
	rng := rand.New(rand.NewSource(62))
	for i := 0; i < 200000; i++ {
		text := fmt.Sprintf(`{"type":"Point","coordinates":[%s,%s]}`, randomCoordText(rng), randomCoordText(rng))
		checkCanonicalFast(t, text)
	}
	for _, c := range []string{
		`{"type":"Point","coordinates":[1]}`,
		`{"type":"Point","coordinates":[]}`,
		`{"type":"LineString","coordinates":[[1,2]]}`,
		`{"type":"LineString","coordinates":[]}`,
		`{"type":"Polygon","coordinates":[]}`,
		`{"type":"Polygon","coordinates":[[]]}`,
		`{"type":"MultiPoint","coordinates":[]}`,
		`{"type":"MultiPolygon","coordinates":[[[]]]}`,
		`{"type":"Point","coordinates":[1,2,3,4,5]}`,
		`{"type":"Point","coordinates":[1,"2"]}`,
		`{"type":"Point","coordinates":[1e999,2]}`,
		`{"type":"Point","coordinates":[[1,2]]}`,
		`{"type":"Polygon","coordinates":[[1,2]]}`,
		`{"type":"Feature","geometry":{}}`,
	} {
		checkCanonicalFast(t, c)
	}
	for _, text := range []string{
		`{"type":"Point","coordinates":[13.405,52.52]}`,
		`{"coordinates":[[0,0],[1,1]],"type":"LineString"}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`,
		` { "type" : "Polygon" , "coordinates" : [ [ [ 0 , 0 ] , [ 1 , 0 ] , [ 1 , 1 ] , [ 0 , 0 ] ] ] } `,
	} {
		if !checkCanonicalFast(t, text) {
			t.Fatalf("fast path declined ordinary geometry %q", text)
		}
	}
	_ = math.Pi
}

func FuzzCanonicalGeoJSON(f *testing.F) {
	for _, c := range []string{
		`{"type":"Point","coordinates":[13.405,52.52]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}`,
		`{"type":"LineString","coordinates":[[0,0],[1,1.50]]}`,
		`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]]]}`,
		`{"type":"Point","coordinates":[1e-7,-0]}`,
	} {
		f.Add(c)
	}
	f.Fuzz(func(t *testing.T, text string) { checkCanonicalFast(t, text) })
}

// slowBBox is the pre-existing route: decode, then walk the coordinates.
func slowBBox(text string) (geoEditBBox, error) {
	object, err := geoSimplifyObject(text)
	if err != nil {
		return geoEditBBox{}, err
	}
	bbox := geoEditBBox{}
	if err := collectGeoBBox(object, &bbox); err != nil {
		return geoEditBBox{}, err
	}
	if !bbox.Set {
		return geoEditBBox{}, fmt.Errorf("no coordinates")
	}
	return bbox, nil
}

func checkBBoxFast(t *testing.T, text string) (accepted bool) {
	t.Helper()
	got, ok := geoBBoxFast(text)
	if !ok {
		return false
	}
	want, err := slowBBox(text)
	if err != nil {
		t.Fatalf("fast bbox accepted input the slow route rejects (%v): %q", err, text)
	}
	if math.Float64bits(got.MinX) != math.Float64bits(want.MinX) || math.Float64bits(got.MinY) != math.Float64bits(want.MinY) ||
		math.Float64bits(got.MaxX) != math.Float64bits(want.MaxX) || math.Float64bits(got.MaxY) != math.Float64bits(want.MaxY) {
		t.Fatalf("bbox differs for %q:\n fast %+v\n slow %+v", text, got, want)
	}
	return true
}

func TestGeoBBoxFastMatchesSlowRoute(t *testing.T) {
	rng := rand.New(rand.NewSource(71))
	accepted := 0
	for i := 0; i < 40000; i++ {
		text := randomGeoJSONText(rng)
		if checkBBoxFast(t, text) {
			accepted++
		}
		checkBBoxFast(t, perturb(rng, text))
	}
	if accepted < 15000 {
		t.Fatalf("only %d documents took the fast bbox path", accepted)
	}
	// Signed zeros and equal extremes are where math.Min/math.Max matter.
	for _, c := range []string{
		`{"type":"MultiPoint","coordinates":[[0,0],[-0,-0],[0,-0]]}`,
		`{"type":"MultiPoint","coordinates":[[-0,5],[0,5]]}`,
		`{"type":"Point","coordinates":[-0,-0]}`,
		`{"type":"LineString","coordinates":[[1,1]]}`,
	} {
		checkBBoxFast(t, c)
	}
}

func slowWKT(text string) (string, error) {
	obj, err := geoObjectFromValue(text)
	if err != nil {
		return "", err
	}
	return geoJSONToWKT(obj)
}

func checkWKTFast(t *testing.T, text string) (accepted bool) {
	t.Helper()
	got, ok := geoWKTFromTextFast(text)
	if !ok {
		return false
	}
	want, err := slowWKT(text)
	if err != nil {
		t.Fatalf("fast WKT accepted input the slow route rejects (%v): %q", err, text)
	}
	if got != want {
		t.Fatalf("WKT differs for %q:\n fast %s\n slow %s", text, got, want)
	}
	return true
}

func TestGeoWKTFromTextFastMatchesSlowRoute(t *testing.T) {
	rng := rand.New(rand.NewSource(91))
	accepted := 0
	for i := 0; i < 40000; i++ {
		text := randomGeoJSONText(rng)
		if checkWKTFast(t, text) {
			accepted++
		}
		checkWKTFast(t, perturb(rng, text))
	}
	if accepted < 15000 {
		t.Fatalf("only %d documents took the fast WKT path", accepted)
	}
	for _, c := range []string{
		`{"type":"Point","coordinates":[1,2]}`,
		`{"type":"Point","coordinates":[1,2,3]}`,
		`{"type":"MultiPoint","coordinates":[[1,2],[3,4,5]]}`,
		`{"type":"MultiPoint","coordinates":[]}`,
		`{"type":"Polygon","coordinates":[]}`,
		`{"type":"Polygon","coordinates":[[]]}`,
		`{"type":"LineString","coordinates":[[1,2],[3,4]]}`,
		`{"type":"linestring","coordinates":[[0.1,0.2],[1e-7,1e15]]}`,
		`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]],[[[5,5],[6,5],[6,6],[5,5]]]]}`,
		`{"coordinates":[13.405,52.52],"type":"Point"}`,
	} {
		if !checkWKTFast(t, c) {
			t.Logf("fast WKT declined %q", c)
		}
	}
}
