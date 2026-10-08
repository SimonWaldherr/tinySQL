package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func randomRegionText(rng *rand.Rand, parts, holes int) string {
	ring := func(cx, cy, r float64, n int) string { return geoBenchRing(cx, cy, r, n) }
	var polys []string
	for i := 0; i < parts; i++ {
		cx, cy := rng.Float64()*20-10, rng.Float64()*20-10
		rings := []string{ring(cx, cy, 3+rng.Float64()*2, 20+rng.Intn(60))}
		for h := 0; h < holes; h++ {
			rings = append(rings, ring(cx, cy, 0.5+rng.Float64(), 8+rng.Intn(10)))
		}
		polys = append(polys, "["+strings.Join(rings, ",")+"]")
	}
	if parts == 1 {
		return `{"type":"Polygon","coordinates":` + polys[0] + `}`
	}
	return `{"type":"MultiPolygon","coordinates":[` + strings.Join(polys, ",") + `]}`
}

// The cached entry with its bounding-box shortcut must answer exactly like the
// plain point-in-polygon test, on and around the box edges as well.
func TestGeoPolygonEntryContainsMatchesPlainTest(t *testing.T) {
	rng := rand.New(rand.NewSource(21))
	for trial := 0; trial < 60; trial++ {
		text := randomRegionText(rng, 1+rng.Intn(3), rng.Intn(3))
		mp, err := geoMultiPolygonFromValue(text)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := geoPolygonFromValueCached(text)
		if err != nil {
			t.Fatal(err)
		}
		points := []geoPoint{{Lon: math.Inf(1)}, {Lon: math.Inf(-1), Lat: 1}, {Lat: math.Inf(1)}, {Lon: math.NaN()}, {Lat: math.NaN()}}
		if entry.hasBounds {
			for _, dx := range []float64{-1, 0, 1} {
				for _, dy := range []float64{-1, 0, 1} {
					points = append(points,
						geoPoint{Lon: entry.minX + dx*1e-9, Lat: entry.minY + dy*1e-9},
						geoPoint{Lon: entry.maxX + dx*1e-9, Lat: entry.maxY + dy*1e-9},
						geoPoint{Lon: entry.minX + dx*1e-9, Lat: entry.maxY + dy*1e-9})
				}
			}
		}
		for i := 0; i < 400; i++ {
			points = append(points, geoPoint{Lon: rng.Float64()*30 - 15, Lat: rng.Float64()*30 - 15})
		}
		for _, p := range points {
			if got, want := entry.contains(p), pointInMultiPolygon(p, mp); got != want {
				t.Fatalf("trial %d: contains(%v) = %v, plain test says %v", trial, p, got, want)
			}
		}
	}
}

// Alternating between regions, and between a region and a short (uncached)
// polygon, must never serve a stale parse.
func TestGeoPolygonCacheNeverServesStaleRegion(t *testing.T) {
	rng := rand.New(rand.NewSource(33))
	var texts []string
	for i := 0; i < geoPolyCacheSlots*3; i++ { // more regions than cache slots
		texts = append(texts, randomRegionText(rng, 1, 0))
	}
	short := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}`
	if len(short) >= geoPolyCacheMinText {
		t.Fatalf("fixture polygon is %d bytes; the test needs one below the cache threshold", len(short))
	}
	texts = append(texts, short)
	for round := 0; round < 4; round++ {
		for _, i := range rng.Perm(len(texts)) {
			text := texts[i]
			want, err := geoMultiPolygonFromValue(text)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := geoPolygonFromValueCached(text)
			if err != nil {
				t.Fatal(err)
			}
			for k := 0; k < 50; k++ {
				p := geoPoint{Lon: rng.Float64()*20 - 10, Lat: rng.Float64()*20 - 10}
				if entry.contains(p) != pointInMultiPolygon(p, want) {
					t.Fatalf("round %d region %d: stale or wrong answer at %v", round, i, p)
				}
			}
		}
	}
}

// Invalid input must keep failing with the same error, cached path or not.
func TestGeoPolygonCacheInvalidInputStillErrors(t *testing.T) {
	long := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]],"extra":"` + strings.Repeat("x", 200) + `"}`
	for _, bad := range []string{long, `{"type":"Point","coordinates":[1,2]}` + strings.Repeat(" ", 200), "not json at all " + strings.Repeat("y", 200)} {
		for i := 0; i < 3; i++ {
			_, cachedErr := geoPolygonFromValueCached(bad)
			_, plainErr := geoMultiPolygonFromValue(bad)
			if cachedErr == nil || plainErr == nil {
				continue // an extra member is legal; only compare when the plain path fails
			}
			if cachedErr.Error() != plainErr.Error() {
				t.Fatalf("error changed: cached %q, plain %q", cachedErr, plainErr)
			}
		}
	}
}

func TestGeoWithinPolygonSQLWithCachedRegion(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	exec := func(q string) *ResultSet {
		t.Helper()
		rs, err := Execute(ctx, db, "default", mustParse(q))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return rs
	}
	exec(`CREATE TABLE pts (id INT, pt GEOMETRY)`)
	rng := rand.New(rand.NewSource(8))
	for i := 0; i < 300; i++ {
		exec(fmt.Sprintf(`INSERT INTO pts VALUES (%d, '{"type":"Point","coordinates":[%.4f,%.4f]}')`, i, rng.Float64()*20-10, rng.Float64()*20-10))
	}
	region := randomRegionText(rng, 2, 1)
	mp, err := geoMultiPolygonFromValue(region)
	if err != nil {
		t.Fatal(err)
	}
	for _, expr := range []string{
		fmt.Sprintf(`ST_WITHIN(pt, '%s')`, region),
		fmt.Sprintf(`ST_CONTAINS('%s', pt)`, region),
		fmt.Sprintf(`GEO_WITHIN_POLYGON(pt, '%s')`, region),
	} {
		rs := exec(`SELECT id, ` + expr + ` AS inside FROM pts ORDER BY id`)
		if len(rs.Rows) != 300 {
			t.Fatalf("%s: %d rows", expr, len(rs.Rows))
		}
		inside := 0
		for _, row := range rs.Rows {
			p, err := geoPointFromValue(exec(fmt.Sprintf(`SELECT pt FROM pts WHERE id = %d`, row["id"])).Rows[0]["pt"])
			if err != nil {
				t.Fatal(err)
			}
			if want := pointInMultiPolygon(p, mp); row["inside"] != want {
				t.Fatalf("%s id=%v: %v, want %v", expr, row["id"], row["inside"], want)
			}
			if row["inside"] == true {
				inside++
			}
		}
		if inside == 0 || inside == 300 {
			t.Fatalf("%s: %d of 300 inside; the fixture does not exercise both outcomes", expr, inside)
		}
	}
}

func TestGeoPolygonCacheConcurrentUse(t *testing.T) {
	rng := rand.New(rand.NewSource(77))
	var texts []string
	for i := 0; i < 20; i++ {
		texts = append(texts, randomRegionText(rng, 1, 1))
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 400; i++ {
				text := texts[r.Intn(len(texts))]
				entry, err := geoPolygonFromValueCached(text)
				if err != nil {
					t.Error(err)
					return
				}
				p := geoPoint{Lon: r.Float64()*20 - 10, Lat: r.Float64()*20 - 10}
				if entry.text != "" && entry.text != text {
					t.Errorf("cache returned an entry for a different text")
					return
				}
				_ = entry.contains(p)
			}
		}(int64(g))
	}
	wg.Wait()
}

// Cached geometry values are shared between rows and statements, so no GIS
// function may modify one. Run every registered geometry function with the same
// large cached polygon (and a second one) in every argument position it might
// accept, then check that the cached decoded maps are exactly what a fresh
// decode gives.
func TestGeoObjectCacheIsNeverMutated(t *testing.T) {
	rng := rand.New(rand.NewSource(404))
	big := randomRegionText(rng, 2, 1)
	big2 := randomRegionText(rng, 1, 0)
	line := `{"type":"LineString","coordinates":[[0,0],[1,1],[2,0],[3,2]]}` + strings.Repeat(" ", geoObjCacheMinText)
	if len(big) < geoObjCacheMinText || len(big2) < geoObjCacheMinText {
		t.Fatalf("fixtures must be at least %d bytes", geoObjCacheMinText)
	}
	texts := []string{big, big2, line}

	pristine := map[string]map[string]any{}
	cached := map[string]map[string]any{}
	for _, text := range texts {
		obj, err := geoObjectFromValue(text)
		if err != nil {
			t.Fatal(err)
		}
		cached[text] = obj
		fresh, err := slowDecodeObject(text)
		if err != nil {
			t.Fatal(err)
		}
		pristine[text] = fresh
		if got := geoObjCacheLookup(text); got == nil {
			t.Fatalf("fixture of %d bytes did not enter the cache", len(text))
		}
	}

	names := map[string]bool{}
	for _, reg := range []map[string]funcHandler{
		getGeoFunctions(), getGeoClipFunctions(), getGeoPackageFunctions(), getGeoEditingFunctions(),
		getGeoHashFunctions(), getGeoRelateFunctions(), getGeoExtraRelateFunctions(), getGeoSimplifyFunctions(),
		getGeoTransformFunctions(), getGeoWKTFunctions(), getGeoWKBFunctions(), getCRSFunctions(),
	} {
		for name := range reg {
			names[name] = true
		}
	}
	if len(names) < 60 {
		t.Fatalf("collected only %d GIS function names", len(names))
	}

	db := storage.NewDB()
	ctx := context.Background()
	quote := func(s string) string { return "'" + s + "'" }
	extras := []string{"1", "2.5", "'dp'", "0.001", "4326", "3857", "true", "8"}
	pt := `'{"type":"Point","coordinates":[1,1]}'`
	calls := 0
	for name := range names {
		for _, a := range texts {
			args := [][]string{
				{quote(a)},
				{quote(a), pt}, {pt, quote(a)},
				{quote(a), quote(big2)}, {quote(big2), quote(a)}, {quote(a), quote(a)},
				{quote(a), quote(line)}, {quote(line), quote(a)},
			}
			for _, e := range extras {
				args = append(args, []string{quote(a), e}, []string{quote(a), pt, e}, []string{quote(a), quote(big2), e}, []string{quote(a), e, e})
			}
			for _, list := range args {
				stmt, err := ParseSQL0Safe(fmt.Sprintf(`SELECT %s(%s) AS v`, name, strings.Join(list, ", ")))
				if err != nil {
					continue
				}
				_, _ = Execute(ctx, db, "default", stmt) // errors are fine: only mutation matters
				calls++
			}
		}
	}
	if calls < 1000 {
		t.Fatalf("only %d calls were made", calls)
	}
	// The typed polygon cache is shared the same way.
	for _, text := range []string{big, big2} {
		entry, err := geoPolygonFromValueCached(text)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := geoMultiPolygonFromValue(text)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(entry.mp, fresh) {
			t.Fatalf("a GIS function modified a cached parsed polygon (%d-byte text)", len(text))
		}
	}
	// The typed form kept with a cached object is shared too.
	for _, text := range []string{big, big2} {
		obj := geoObjCacheLookup(text)
		if obj == nil {
			continue // evicted by the calls above; nothing left to check
		}
		entry, ok := geoCachedObjectPolygon(obj)
		if !ok {
			t.Fatalf("cached object of %d bytes is not found by its own address", len(text))
		}
		fresh, err := geoMultiPolygonFromObject(pristine[text])
		if (err == nil) != (entry.err == nil) || (err == nil && !reflect.DeepEqual(entry.mp, fresh)) {
			t.Fatalf("a GIS function modified a cached typed polygon (%d-byte text)", len(text))
		}
	}
	for _, text := range texts {
		if !reflect.DeepEqual(cached[text], pristine[text]) {
			t.Fatalf("a GIS function modified a cached geometry map (%d-byte text)", len(text))
		}
		if got := geoObjCacheLookup(text); got != nil && !reflect.DeepEqual(got, pristine[text]) {
			t.Fatalf("the cache now holds a modified map (%d-byte text)", len(text))
		}
	}
}

// ParseSQL0Safe parses a statement, returning an error instead of panicking.
func ParseSQL0Safe(q string) (Statement, error) {
	p := NewParser(q)
	return p.ParseStatement()
}
