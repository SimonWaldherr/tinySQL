package engine

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoSearchNearestMatchesRadiusOrder(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE points (id INT, geom TEXT)`)
	// IDs descend within equal-distance groups, so the tie break deliberately
	// differs from primary-key order.
	execSQL(t, db, geoSearchPoint(50, 179.5, 70))
	execSQL(t, db, geoSearchPoint(40, -179.5, 70))
	execSQL(t, db, geoSearchPoint(30, 179.5, 70))
	execSQL(t, db, geoSearchPoint(20, 0, 75))
	execSQL(t, db, `INSERT INTO points VALUES (5, NULL), (1, 'not a geometry')`)
	boundary := haversineMeters(70, 179, 70, 179.5)
	for _, tc := range []struct {
		lon, lat, radius float64
		k, wantRows      int
	}{
		{179, 70, 80_000, 2, 2},
		{179, 70, 80_000, 2147483647, 3},
		{179, 70, math.Nextafter(boundary, 0), 10, 0},
		{179, 70, boundary, 10, 2},
		{180, 89, 3_000_000, 10, 4},
		{0, 0, 1, 3, 0},
	} {
		center := fmt.Sprintf("%v,%v", tc.lon, tc.lat)
		actual := execSQL(t, db, fmt.Sprintf(`SELECT id, _geo_rank, _geo_distance_m AS meters
			FROM GEO_SEARCH('points', 'geom', 'nearest', %s, %v, %d) ORDER BY _geo_rank`, center, tc.radius, tc.k))
		reference := execSQL(t, db, fmt.Sprintf(`SELECT id, GEO_DISTANCE(GEO_POINT(%s), geom) AS meters
			FROM GEO_SEARCH('points', 'geom', 'radius', %s, %v) ORDER BY meters, id DESC LIMIT %d`,
			center, center, tc.radius, tc.k))
		if len(actual.Rows) != tc.wantRows || len(reference.Rows) != tc.wantRows {
			t.Fatalf("case %+v: got %d nearest rows, radius returned %d, want %d", tc, len(actual.Rows), len(reference.Rows), tc.wantRows)
		}
		for i, got := range actual.Rows {
			if got["_geo_rank"] != i+1 {
				t.Fatalf("unexpected rank in %v", got)
			}
			delete(got, "_geo_rank")
			if !reflect.DeepEqual(got, reference.Rows[i]) {
				t.Fatalf("case %+v rank %d: got %v, radius returned %v", tc, i+1, got, reference.Rows[i])
			}
		}
	}
}

func TestGeoSearchNearestValidatesInputs(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY)`)
	for _, args := range []string{"0,0,1", "0,0,1,0", "0,0,1,1.5", "0,0,1,2147483648", "0,0,1,NULL", "0,0,0,1", "181,0,1,1", "0,91,1,1"} {
		q := `SELECT id FROM GEO_SEARCH('points', 'geom', 'nearest', ` + args + `)`
		if _, err := Execute(t.Context(), db, "default", mustParse(q)); err == nil {
			t.Errorf("invalid nearest query unexpectedly succeeded: %s", q)
		}
	}
}

func TestGeoSearchFindsCentroidOutsideExtent(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE layer (id INT, geom GEOMETRY)`)
	// A hole overlapping the exterior leaves a positive signed mass but
	// places the computed centroid west of the entire geometry.
	execSQL(t, db, `INSERT INTO layer VALUES (1, '{"type":"Polygon","coordinates":[
		[[0,0],[1,0],[1,1],[0,1],[0,0]],[[0.5,0],[1.48,0],[1.48,1],[0.5,1],[0.5,0]]
	]}')`)
	for _, tc := range []struct {
		query string
		want  int
	}{
		{`'bbox', -24,0,-23,1`, 1},
		{`'bbox_intersects', -24,0,-23,1`, 0},
		{`'bbox_intersects', 0,0,1,1`, 1},
		{`'nearest', -23.51,0.5,10,1`, 1},
		{`'radius', -23.51,0.5,10`, 1},
		{`'bbox', -25,-1,2,1`, 1}, // present in both cells and the outlier list
	} {
		rs := execSQL(t, db, `SELECT id FROM GEO_SEARCH('layer', 'geom', `+tc.query+`)`)
		if len(rs.Rows) != tc.want {
			t.Errorf("%s returned %v, want %d rows", tc.query, rs.Rows, tc.want)
		}
	}
}

func TestGeoGridPointDecodeMatchesGeneralPath(t *testing.T) {
	for _, coords := range []string{
		`[11.576124,48.137154]`,
		`[-0,-0,42]`,
		`[0.00000001,1.2345678901234567,0,9]`,
		`[1,2,1e309]`, // all ordinates must be finite
		`[1,2,[]]`,
	} {
		fastBox, fastPoint, fastOK := geoGridGeometry(`{"coordinates":` + coords + `,"type":"Point"}`)
		slowBox, slowPoint, slowOK := geoGridGeometry(`{"type":"Point","coordinates":` + coords + `}`)
		if fastOK != slowOK || fastBox != slowBox || fastPoint != slowPoint {
			t.Fatalf("%s: compact point = %v %v %t, general = %v %v %t", coords, fastBox, fastPoint, fastOK, slowBox, slowPoint, slowOK)
		}
		if math.Float64bits(fastBox.MinX) != math.Float64bits(slowBox.MinX) ||
			math.Float64bits(fastBox.MinY) != math.Float64bits(slowBox.MinY) ||
			math.Float64bits(fastPoint.Lon) != math.Float64bits(slowPoint.Lon) ||
			math.Float64bits(fastPoint.Lat) != math.Float64bits(slowPoint.Lat) {
			t.Fatalf("%s: point decoding changed signed zero", coords)
		}
	}
}

func geoSearchBenchmarkPointTable() *storage.Table {
	table := storage.NewTable("search_points", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "geom", Type: storage.GeometryType},
	}, false)
	// A point layer covering roughly the extent of Bavaria.
	for id := range 20_000 {
		lon, lat := 9+0.025*float64(id%200), 47+0.038*float64(id/200)
		geom, _ := geoPointJSON(lon, lat, nil)
		table.Rows = append(table.Rows, []any{id, geom})
	}
	return table
}

func BenchmarkGeoGridPointBuild(b *testing.B) {
	table := geoSearchBenchmarkPointTable()
	b.ReportAllocs()
	for b.Loop() {
		idx, err := buildGeoGridIndex(b.Context(), table, 1)
		if err != nil || len(idx.centroids) != len(table.Rows) {
			b.Fatalf("build failed: %v", err)
		}
	}
}

func BenchmarkGeoSearchNearest(b *testing.B) {
	db := storage.NewDB()
	defer db.Close()
	table := geoSearchBenchmarkPointTable()
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"radius-sort", `SELECT id, GEO_DISTANCE(GEO_POINT(11.3,49.1),geom) AS meters
			FROM GEO_SEARCH('search_points','geom','radius',11.3,49.1,125000) ORDER BY meters,id LIMIT 20`},
		{"nearest", `SELECT id, _geo_distance_m AS meters
			FROM GEO_SEARCH('search_points','geom','nearest',11.3,49.1,125000,20) ORDER BY _geo_rank`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			query := func() {
				rs, err := Execute(b.Context(), db, "default", stmt)
				if err != nil || len(rs.Rows) != 20 {
					b.Fatalf("query failed: %v", err)
				}
			}
			query() // Ensure a warm index in both variants.
			b.ReportAllocs()
			for b.Loop() {
				query()
			}
		})
	}
}

func geoSearchIDSet(t *testing.T, db *storage.DB, sql string) map[int]bool {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	got := make(map[int]bool, len(rs.Rows))
	for _, row := range rs.Rows {
		id, ok := row["id"].(int)
		if !ok {
			t.Fatalf("%s: id column is %T, want int", sql, row["id"])
		}
		got[id] = true
	}
	return got
}

func geoSearchPoint(id int, lon, lat float64) string {
	return fmt.Sprintf(`INSERT INTO points VALUES (%d, '{"type":"Point","coordinates":[%v,%v]}')`, id, lon, lat)
}

func TestGeoSearchBBoxMode(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY)`)

	inside := map[int][2]float64{1: {0.1, 0.1}, 2: {0.5, 0.9}, 5: {0.99, 0.01}}
	outside := map[int][2]float64{3: {5, 5}, 4: {-3, -3}, 6: {1.5, 1.5}}
	for id, p := range inside {
		execSQL(t, db, geoSearchPoint(id, p[0], p[1]))
	}
	for id, p := range outside {
		execSQL(t, db, geoSearchPoint(id, p[0], p[1]))
	}
	// A wider scatter of far-away points so the grid index buckets across
	// more than one cell.
	nextID := 7
	for lon := 2.0; lon <= 8.0; lon += 2.0 {
		for lat := 2.0; lat <= 8.0; lat += 2.0 {
			execSQL(t, db, geoSearchPoint(nextID, lon, lat))
			nextID++
		}
	}

	got := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH('points', 'geom', 'bbox', 0, 0, 1, 1)`)
	for id := range inside {
		if !got[id] {
			t.Errorf("GEO_SEARCH bbox missing expected inside point id=%d; got %v", id, got)
		}
	}
	for id := range outside {
		if got[id] {
			t.Errorf("GEO_SEARCH bbox unexpectedly returned outside point id=%d; got %v", id, got)
		}
	}
	if len(got) != len(inside) {
		t.Errorf("GEO_SEARCH bbox returned %d rows, want %d: %v", len(got), len(inside), got)
	}

	// Inserting a new matching row must invalidate the cached grid index
	// (keyed by table.Version) so a later query sees it.
	execSQL(t, db, geoSearchPoint(100, 0.05, 0.05))
	got2 := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH('points', 'geom', 'bbox', 0, 0, 1, 1)`)
	if !got2[100] {
		t.Errorf("GEO_SEARCH bbox did not see a row inserted after the index was first built; got %v", got2)
	}
}

func TestGeoSearchRadiusMode(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY)`)
	execSQL(t, db, geoSearchPoint(1, 0, 0.01)) // ~1113 m from (0,0)
	execSQL(t, db, geoSearchPoint(2, 0, 0.1))  // ~11132 m from (0,0)
	execSQL(t, db, geoSearchPoint(3, 0.01, 0)) // ~1113 m from (0,0)
	execSQL(t, db, geoSearchPoint(4, -5, -5))  // far away

	got := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH('points', 'geom', 'radius', 0, 0, 2000)`)
	want := map[int]bool{1: true, 3: true}
	if len(got) != len(want) {
		t.Fatalf("GEO_SEARCH radius returned %v, want %v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Errorf("GEO_SEARCH radius missing expected point id=%d; got %v", id, got)
		}
	}
}

func TestGeoSearchRadiusCrossesAntimeridian(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY)`)
	execSQL(t, db, geoSearchPoint(1, 179, 0))
	execSQL(t, db, geoSearchPoint(2, -179, 0))
	execSQL(t, db, geoSearchPoint(3, 160, 0))
	got := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH('points', 'geom', 'radius', 179, 0, 300000)`)
	if len(got) != 2 || !got[1] || !got[2] {
		t.Fatalf("dateline radius returned %v, want ids 1 and 2", got)
	}
}

func TestGeoSearchBBoxIntersectsUsesGeometryExtent(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE features (id INT, geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO features VALUES
		(1, '{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}'),
		(2, '{"type":"Point","coordinates":[20,20]}')`)

	// The polygon centroid (5,5) is outside this viewport, but its actual
	// extent intersects it. The GIS-specific mode must retain the feature.
	got := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH(
		'features', 'geom', 'bbox_intersects', 9.5, 9.5, 10.5, 10.5)`)
	if len(got) != 1 || !got[1] {
		t.Fatalf("bbox_intersects returned %v, want polygon id=1", got)
	}
	legacy := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH(
		'features', 'geom', 'bbox', 9.5, 9.5, 10.5, 10.5)`)
	if legacy[1] {
		t.Fatalf("legacy centroid bbox unexpectedly matched polygon: %v", legacy)
	}
}

func TestGeoSearchBBoxIntersectsCrossesAntimeridian(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY)`)
	execSQL(t, db, geoSearchPoint(1, 175, 10))
	execSQL(t, db, geoSearchPoint(2, -175, 10))
	execSQL(t, db, geoSearchPoint(3, 0, 10))
	got := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH(
		'points', 'geom', 'bbox_intersects', 170, 0, -170, 20)`)
	if len(got) != 2 || !got[1] || !got[2] {
		t.Fatalf("antimeridian bbox_intersects returned %v, want ids 1 and 2", got)
	}
}

func TestGeoSearchRejectsUnknownMode(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY)`)
	execSQL(t, db, geoSearchPoint(1, 0, 0))
	if _, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT id FROM GEO_SEARCH('points', 'geom', 'nonsense', 0, 0, 1, 1)`)); err == nil {
		t.Errorf("GEO_SEARCH with an unknown mode succeeded, want an error")
	}
}

func TestGeoSearchBroadBBoxOnZeroExtentCorpus(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE colocated_points (id INT, geom GEOMETRY)`)
	for id := 1; id <= 4; id++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO colocated_points VALUES (%d,
			'{"type":"Point","coordinates":[13.405,52.52]}')`, id))
	}

	// A zero-extent corpus uses the grid's minimum 1e-6-degree cell size. The
	// world-sized query must clamp to the occupied extent instead of iterating
	// hundreds of millions of empty cells along each axis.
	got := geoSearchIDSet(t, db, `SELECT id FROM GEO_SEARCH(
		'colocated_points', 'geom', 'bbox', -180, -90, 180, 90)`)
	if len(got) != 4 {
		t.Fatalf("broad bbox returned %v, want all four colocated points", got)
	}
}

func TestGeoSearchRejectsNonFiniteQueryBeforeIndexBuild(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE finite_geo (id INT, geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO finite_geo VALUES (1,
		'{"type":"Point","coordinates":[0,0]}')`)

	args := []Expr{
		&Literal{Val: "finite_geo"}, &Literal{Val: "geom"}, &Literal{Val: "bbox"},
		&Literal{Val: math.NaN()}, &Literal{Val: 0}, &Literal{Val: 1}, &Literal{Val: 1},
	}
	_, err := (&GeoSearchTableFunc{}).Execute(context.Background(), args,
		ExecEnv{ctx: context.Background(), db: db, tenant: "default"}, nil)
	if err == nil {
		t.Fatal("GEO_SEARCH accepted a non-finite bbox coordinate")
	}
	geoGridCacheMu.RLock()
	_, built := geoGridCache[geoIndexCacheKey{tenant: "default", table: "finite_geo", colIdx: 1}]
	geoGridCacheMu.RUnlock()
	if built {
		t.Fatal("GEO_SEARCH built a grid before rejecting a non-finite bbox coordinate")
	}
}

// A one-point corpus makes an under-sized candidate bbox observable: a
// coarse cell containing unrelated points cannot accidentally hide the miss.
func TestGeoSearchRadiusSphericalBounds(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		lon, lat, pointLon, pointLat, radius float64
	}{
		{"equatorial_boundary", 0, 0, 0, 0.00899, 1000},
		{"north_pole", 0, 89.999, 180, 89.999, 300},
		{"south_pole", 0, -89.999, 180, -89.999, 300},
		{"wide_high_latitude", 0, 60, 60, 70, 3000000},
		{"dateline_east", 179.999, 0, -179.999, 0, 300},
		{"dateline_west", -179.999, 0, 179.999, 0, 300},
		{"whole_globe", 0, 0, 180, 0, 21000000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if distance := haversineMeters(tc.lat, tc.lon, tc.pointLat, tc.pointLon); distance > tc.radius {
				t.Fatalf("invalid fixture: distance=%v radius=%v", distance, tc.radius)
			}
			db := storage.NewDB()
			execSQL(t, db, `CREATE TABLE points (id INT, geom GEOMETRY, embedding VECTOR)`)
			execSQL(t, db, fmt.Sprintf(`INSERT INTO points VALUES (1, '{"type":"Point","coordinates":[%v,%v]}', '[1,0]')`, tc.pointLon, tc.pointLat))
			geo := geoSearchIDSet(t, db, fmt.Sprintf(`SELECT id FROM GEO_SEARCH('points','geom','radius',%v,%v,%v)`, tc.lon, tc.lat, tc.radius))
			if !geo[1] {
				t.Fatal("GEO_SEARCH excluded a point within the radius")
			}
			rag := execSQL(t, db, fmt.Sprintf(`SELECT id FROM RAG_SEARCH('points','embedding','[1,0]',1,
    '{"pre_filter":{"spatial":{"geometry_column":"geom","center":[%v,%v],"radius_meters":%v}}}')`, tc.lon, tc.lat, tc.radius))
			if len(rag.Rows) != 1 || rag.Rows[0]["id"] != 1 {
				t.Fatalf("RAG_SEARCH excluded a point within the radius: %#v", rag.Rows)
			}
		})
	}
}
