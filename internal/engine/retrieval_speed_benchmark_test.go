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

func BenchmarkGeoSearchWarm(b *testing.B) {
	db := storage.NewDB()
	if err := db.Put("default", geoSpeedTable(b, false)); err != nil {
		b.Fatal(err)
	}
	runRAGBench(b, db, `SELECT id FROM GEO_SEARCH('geo_speed','geom','bbox',2,2,18,8)`, 9821)
}
