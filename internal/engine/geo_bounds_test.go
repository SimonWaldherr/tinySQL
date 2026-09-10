package engine

import (
	"math/rand"
	"testing"
)

func referenceSegmentIntersection(p1, p2, p3, p4 geoPoint) bool {
	d1, d2, d3, d4 := orientation(p3, p4, p1), orientation(p3, p4, p2), orientation(p1, p2, p3), orientation(p1, p2, p4)
	return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) || d1 == 0 && onSegment(p1, p3, p4) || d2 == 0 && onSegment(p2, p3, p4) || d3 == 0 && onSegment(p3, p1, p2) || d4 == 0 && onSegment(p4, p1, p2)
}
func TestGeoBoundsMatchExactReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 500; trial++ {
		a, b := make([]geoPoint, 10), make([]geoPoint, 10)
		for i := range a {
			a[i] = geoPoint{Lon: float64(rng.Intn(20)), Lat: float64(rng.Intn(20))}
			b[i] = geoPoint{Lon: float64(rng.Intn(20)), Lat: float64(rng.Intn(20))}
		}
		expected := false
		for i := 0; i+1 < len(a); i++ {
			for j := 0; j+1 < len(b); j++ {
				want := referenceSegmentIntersection(a[i], a[i+1], b[j], b[j+1])
				if got := segmentsIntersect(a[i], a[i+1], b[j], b[j+1]); got != want {
					t.Fatalf("segment mismatch: %v %v", a, b)
				}
				expected = expected || want
			}
		}
		if got := geoPathsIntersect(a, b); got != expected {
			t.Fatalf("path mismatch: %v %v", a, b)
		}
	}
	for _, path := range [][]geoPoint{{{Lon: 1, Lat: 1}, {Lon: 2, Lat: 2}}, {{Lon: 0, Lat: 0}, {Lon: 1, Lat: 1}}, {{Lon: 1, Lat: 1}, {Lon: 1, Lat: 1}}} {
		if !geoPathsIntersect([]geoPoint{{Lon: 0, Lat: 0}, {Lon: 1, Lat: 1}}, path) {
			t.Fatal("lost boundary contact")
		}
	}
}
