// Benchmarks for the sort-heavy geo functions (convex hull, edge sorting for
// dissolve, point-set comparison for relate). geoPoint carries a pointer
// field (Z), so these exist to catch a regression back to sort.Slice's
// reflect.Swapper -- see geoPointsByLonLat/geoDissolveEdgesByLess in
// geo_functions.go/geo_dissolve.go for why that matters here specifically.
package engine

import (
	"math/rand"
	"strconv"
	"testing"
)

func randomGeoPoints(n int, seed int64) []geoPoint {
	rng := rand.New(rand.NewSource(seed))
	pts := make([]geoPoint, n)
	for i := range pts {
		pts[i] = geoPoint{Lon: rng.Float64() * 360, Lat: rng.Float64() * 180}
	}
	return pts
}

func BenchmarkConvexHull(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		pts := randomGeoPoints(n, 1)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cp := make([]geoPoint, len(pts))
				copy(cp, pts)
				if _, err := convexHull(cp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSortGeoPoints(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		pts := randomGeoPoints(n, 2)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cp := make([]geoPoint, len(pts))
				copy(cp, pts)
				sortGeoPoints(cp)
			}
		})
	}
}
