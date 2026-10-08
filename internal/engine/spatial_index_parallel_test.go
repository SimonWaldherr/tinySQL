package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func geoIndexTestTable(n int) *storage.Table {
	table := storage.NewTable("layer", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "geom", Type: storage.TextType},
	}, false)
	rng := rand.New(rand.NewSource(17))
	for i := 0; i < n; i++ {
		cx, cy := rng.Float64()*20-10, rng.Float64()*20-10
		var g any
		switch rng.Intn(9) {
		case 0:
			g = nil
		case 1:
			g = "not geojson"
		case 2:
			g = fmt.Sprintf(`{"type":"Point","coordinates":[%.6f,%.6f]}`, cx, cy)
		case 3:
			g = fmt.Sprintf(`{"type":"LineString","coordinates":[[%.6f,%.6f],[%.6f,%.6f],[%.6f,%.6f]]}`, cx, cy, cx+1, cy, cx+1, cy+2)
		case 4:
			g = fmt.Sprintf(`{"type":"MultiPoint","coordinates":[[%.6f,%.6f],[%.6f,%.6f]]}`, cx, cy, cx-1, cy-1)
		case 5:
			g = fmt.Sprintf(`{"type":"Feature","geometry":{"type":"Point","coordinates":[%.6f,%.6f]},"properties":{}}`, cx, cy)
		default:
			g = fmt.Sprintf(`{"type":"Polygon","coordinates":[%s]}`, geoBenchRing(cx, cy, 0.05+rng.Float64()*0.5, 6+rng.Intn(10)))
		}
		table.Rows = append(table.Rows, []any{i, g})
	}
	return table
}

// The parallel first pass must produce exactly what one sequential pass over
// all rows produces.
func TestSpatialIndexParallelBuildMatchesSequential(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{10, geoGridParallelMinRows, 2 * geoGridParallelMinRows, 25000} {
		table := geoIndexTestTable(n)
		newIdx := func() *geoGridIndex {
			return &geoGridIndex{
				table:     table,
				valid:     make([]bool, n),
				centroids: make([]geoPoint, n),
				bboxes:    make([]geoEditBBox, n),
			}
		}
		seq, par := newIdx(), newIdx()
		su, sv, err := seq.scanRange(ctx, table, 1, 0, n)
		if err != nil {
			t.Fatal(err)
		}
		pu, pv, err := par.scanRows(ctx, table, 1)
		if err != nil {
			t.Fatal(err)
		}
		if sv != pv || sv == 0 {
			t.Fatalf("n=%d: valid count sequential %d, parallel %d", n, sv, pv)
		}
		if !reflect.DeepEqual(seq.valid, par.valid) || !reflect.DeepEqual(seq.bboxes, par.bboxes) {
			t.Fatalf("n=%d: per-row validity or bboxes differ", n)
		}
		for i := range seq.centroids {
			if math.Float64bits(seq.centroids[i].Lon) != math.Float64bits(par.centroids[i].Lon) ||
				math.Float64bits(seq.centroids[i].Lat) != math.Float64bits(par.centroids[i].Lat) {
				t.Fatalf("n=%d: centroid of row %d differs", n, i)
			}
		}
		if su != pu {
			t.Fatalf("n=%d: union bbox sequential %+v, parallel %+v", n, su, pu)
		}
	}
}

func TestSpatialIndexParallelBuildHonorsCancellation(t *testing.T) {
	table := geoIndexTestTable(30000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := buildGeoGridIndex(ctx, table, 1); err == nil {
		t.Fatal("a cancelled context must stop the build")
	}
}
