package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var geoCandidateBenchmarkSink []int32

func geoSpeedTable(tb testing.TB, polygons bool) *storage.Table {
	tb.Helper()
	table := storage.NewTable("geo_speed", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "geom", Type: storage.GeometryType}}, false)
	for y := 0; y < 100; y++ {
		for x := 0; x < 200; x++ {
			lon, lat := float64(x)/10, float64(y)/10
			geom := fmt.Sprintf(`{"type":"Point","coordinates":[%v,%v]}`, lon, lat)
			if polygons {
				geom = fmt.Sprintf(`{"type":"Polygon","coordinates":[[[%v,%v],[%v,%v],[%v,%v],[%v,%v],[%v,%v]]]}`, lon, lat, lon+0.4, lat, lon+0.4, lat+0.4, lon, lat+0.4, lon, lat)
			}
			table.Rows = append(table.Rows, []any{len(table.Rows), geom})
		}
	}
	table.Version++
	return table
}

func BenchmarkGeoGridCandidates(b *testing.B) {
	for _, polygons := range []bool{false, true} {
		table := geoSpeedTable(b, polygons)
		idx, err := buildGeoGridIndex(context.Background(), table, 1)
		if err != nil {
			b.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			bounds [4]float64
		}{
			{"local", [4]float64{4, 4, 5, 5}},
			{"regional", [4]float64{2, 2, 18, 8}},
			{"global", [4]float64{-180, -90, 180, 90}},
		} {
			b.Run(fmt.Sprintf("polygons=%t/%s", polygons, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					geoCandidateBenchmarkSink = idx.candidatesBBox(tc.bounds[0], tc.bounds[1], tc.bounds[2], tc.bounds[3])
				}
			})
		}
	}
}

// BenchmarkGeoGridOverflowCandidates measures candidatesBBox when every row is
// an overflow row (a geometry whose own bbox spans more cells than
// geoGridMaxCellsPerGeometry allows, e.g. one huge polygon sharing a table
// with many small ones -- see buildGeoGridIndex). Each row's bbox is scattered
// across the full extent, so a small local query window can only possibly
// overlap a handful of them; the rest were previously added as candidates
// unconditionally on every query regardless of locality.
func BenchmarkGeoGridOverflowCandidates(b *testing.B) {
	const numOverflow = 5000
	idx := &geoGridIndex{
		uniqueCells: true,
		cells:       map[geoCellID][]int32{},
		bounds:      geoEditBBox{MinX: -180, MinY: -90, MaxX: 180, MaxY: 90, Set: true},
		cellSizeLon: 1, cellSizeLat: 1,
		valid:     make([]bool, numOverflow),
		bboxes:    make([]geoEditBBox, numOverflow),
		centroids: make([]geoPoint, numOverflow),
	}
	for i := 0; i < numOverflow; i++ {
		lon := -180 + float64(i%360)
		lat := -90 + float64(i%180)
		idx.valid[i] = true
		idx.bboxes[i] = geoEditBBox{MinX: lon, MinY: lat, MaxX: lon + 0.5, MaxY: lat + 0.5, Set: true}
		idx.centroids[i] = geoPoint{Lon: lon + 0.25, Lat: lat + 0.25}
		idx.overflow = append(idx.overflow, int32(i))
	}
	b.ReportAllocs()
	for b.Loop() {
		geoCandidateBenchmarkSink = idx.candidatesBBox(0, 0, 1, 1)
	}
}

func BenchmarkGeoSearchWarm(b *testing.B) {
	db := storage.NewDB()
	if err := db.Put("default", geoSpeedTable(b, false)); err != nil {
		b.Fatal(err)
	}
	runRAGBench(b, db, `SELECT id FROM GEO_SEARCH('geo_speed','geom','bbox',2,2,18,8)`, 9821)
}
