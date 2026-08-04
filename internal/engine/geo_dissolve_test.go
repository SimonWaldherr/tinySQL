package engine

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	dissolveSquareA = `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`
	dissolveSquareB = `{"type":"Polygon","coordinates":[[[1,0],[2,0],[2,1],[1,1],[1,0]]]}`
)

func execDissolveFloat(t *testing.T, db *storage.DB, sql string, col string) float64 {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	f, ok := rs.Rows[0][col].(float64)
	if !ok {
		t.Fatalf("%s: column %q is %T, want float64", sql, col, rs.Rows[0][col])
	}
	return f
}

// TestGeoDissolveMergesAdjacentSquares dissolves two unit squares sharing
// the edge x=1 with no GROUP BY at all -- proving GEO_DISSOLVE was added to
// isAggregate()'s name list, not just evalAggregateFuncCall's dispatch
// (the VEC_AVG gotcha: a function missing from isAggregate only aggregates
// when some other GROUP BY happens to force aggregation anyway).
func TestGeoDissolveMergesAdjacentSquares(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE tiles (geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+dissolveSquareA+`')`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+dissolveSquareB+`')`)

	rs := execSQL(t, db, `SELECT GEO_DISSOLVE(geom) AS d FROM tiles`)
	dissolved, ok := rs.Rows[0]["d"].(string)
	if !ok {
		t.Fatalf("GEO_DISSOLVE result is %T, want string", rs.Rows[0]["d"])
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(dissolved), &object); err != nil {
		t.Fatalf("dissolve result is not valid JSON: %v", err)
	}
	if object["type"] != "Polygon" {
		t.Errorf("dissolve of two adjacent squares produced %v, want a single Polygon", object["type"])
	}

	areaA := execDissolveFloat(t, db, `SELECT GEO_POLYGON_AREA('`+dissolveSquareA+`') AS a`, "a")
	areaB := execDissolveFloat(t, db, `SELECT GEO_POLYGON_AREA('`+dissolveSquareB+`') AS a`, "a")
	areaDissolved := execDissolveFloat(t, db, `SELECT GEO_POLYGON_AREA('`+dissolved+`') AS a`, "a")
	if math.Abs(areaDissolved-(areaA+areaB)) > 1e-6*areaDissolved {
		t.Errorf("dissolved area = %v, want approximately %v (sum of inputs)", areaDissolved, areaA+areaB)
	}
}

// TestGeoUnionAggMatchesDissolve proves GEO_DISSOLVE and GEO_UNION_AGG/
// ST_UNION share one handler by checking they produce byte-identical output
// for the same input.
func TestGeoUnionAggMatchesDissolve(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE tiles (geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+dissolveSquareA+`')`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+dissolveSquareB+`')`)

	dissolve := execSQL(t, db, `SELECT GEO_DISSOLVE(geom) AS d FROM tiles`).Rows[0]["d"]
	unionAgg := execSQL(t, db, `SELECT GEO_UNION_AGG(geom) AS d FROM tiles`).Rows[0]["d"]
	stUnion := execSQL(t, db, `SELECT ST_UNION(geom) AS d FROM tiles`).Rows[0]["d"]
	if dissolve != unionAgg || dissolve != stUnion {
		t.Errorf("GEO_DISSOLVE = %v, GEO_UNION_AGG = %v, ST_UNION = %v; want identical", dissolve, unionAgg, stUnion)
	}
}

// TestGeoDissolveGroupByProducesMultiPolygon dissolves two disjoint clusters
// (grouped by a region column) and expects each group's own pair of
// adjacent squares to merge, while the two groups themselves stay apart --
// i.e. GROUP BY dissolve, not a table-wide dissolve.
func TestGeoDissolveGroupByProducesMultiPolygon(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE tiles (region TEXT, geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('north', '`+dissolveSquareA+`')`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('north', '`+dissolveSquareB+`')`)
	farAway := `{"type":"Polygon","coordinates":[[[10,10],[11,10],[11,11],[10,11],[10,10]]]}`
	execSQL(t, db, `INSERT INTO tiles VALUES ('south', '`+farAway+`')`)

	rs := execSQL(t, db, `SELECT region, GEO_DISSOLVE(geom) AS d FROM tiles GROUP BY region ORDER BY region`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(rs.Rows))
	}
	var north, south map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["d"].(string)), &north); err != nil {
		t.Fatalf("north group: %v", err)
	}
	if err := json.Unmarshal([]byte(rs.Rows[1]["d"].(string)), &south); err != nil {
		t.Fatalf("south group: %v", err)
	}
	if rs.Rows[0]["region"] != "north" || north["type"] != "Polygon" {
		t.Errorf("north group = %v (%v), want a single merged Polygon", north["type"], rs.Rows[0]["region"])
	}
	if rs.Rows[1]["region"] != "south" || south["type"] != "Polygon" {
		t.Errorf("south group = %v (%v), want the lone square untouched", south["type"], rs.Rows[1]["region"])
	}
}

// TestGeoDissolveFullyCancellingHole covers a hole ring that exactly
// matches its outer boundary: every edge cancels, and the result should be
// an explicitly empty MultiPolygon, not an error and not NULL.
func TestGeoDissolveFullyCancellingHole(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE tiles (geom GEOMETRY)`)
	outer := `{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}`
	// A polygon whose sole ring is the exact reverse of `outer`'s ring --
	// dissolving them together cancels every edge to zero.
	reversed := `{"type":"Polygon","coordinates":[[[0,0],[0,2],[2,2],[2,0],[0,0]]]}`
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+outer+`')`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+reversed+`')`)

	rs := execSQL(t, db, `SELECT GEO_DISSOLVE(geom) AS d FROM tiles`)
	var object map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["d"].(string)), &object); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if object["type"] != "MultiPolygon" {
		t.Errorf("fully-cancelling dissolve type = %v, want MultiPolygon", object["type"])
	}
	coords, ok := object["coordinates"].([]any)
	if !ok || len(coords) != 0 {
		t.Errorf("fully-cancelling dissolve coordinates = %v, want an empty array", object["coordinates"])
	}
}

// TestGeoDissolveRejectsUnclosedRing checks that a malformed (not
// explicitly closed) ring is a hard error, not a best-effort guess.
func TestGeoDissolveRejectsUnclosedRing(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE tiles (geom GEOMETRY)`)
	unclosed := `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1]]]}`
	execSQL(t, db, `INSERT INTO tiles VALUES ('`+unclosed+`')`)
	if _, err := Execute(context.Background(), db, "default", mustParse(`SELECT GEO_DISSOLVE(geom) FROM tiles`)); err == nil {
		t.Errorf("dissolving an unclosed ring succeeded, want an error")
	}
}

func TestGeoBBoxAgg(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE tiles (geom GEOMETRY)`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('{"type":"Point","coordinates":[0,0]}')`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('{"type":"Point","coordinates":[5,-3]}')`)
	execSQL(t, db, `INSERT INTO tiles VALUES ('{"type":"Point","coordinates":[-2,7]}')`)

	rs := execSQL(t, db, `SELECT GEO_BBOX_AGG(geom) AS bbox FROM tiles`)
	var bbox []float64
	if err := json.Unmarshal([]byte(rs.Rows[0]["bbox"].(string)), &bbox); err != nil {
		t.Fatalf("bbox JSON: %v", err)
	}
	want := []float64{-2, -3, 5, 7}
	for i := range want {
		if bbox[i] != want[i] {
			t.Errorf("bbox = %v, want %v", bbox, want)
			break
		}
	}
}

func TestGeoCentroidAggWeighted(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE towns (geom GEOMETRY, population FLOAT)`)
	execSQL(t, db, `INSERT INTO towns VALUES ('{"type":"Point","coordinates":[0,0]}', 1)`)
	execSQL(t, db, `INSERT INTO towns VALUES ('{"type":"Point","coordinates":[10,0]}', 1)`)
	execSQL(t, db, `INSERT INTO towns VALUES ('{"type":"Point","coordinates":[0,0]}', 8)`)

	rs := execSQL(t, db, `SELECT GEO_CENTROID_AGG(geom, population) AS c FROM towns`)
	var p map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["c"].(string)), &p); err != nil {
		t.Fatalf("centroid JSON: %v", err)
	}
	coords := p["coordinates"].([]any)
	// (0*1 + 10*1 + 0*8) / (1+1+8) = 10/10 = 1
	if got := coords[0].(float64); math.Abs(got-1) > 1e-9 {
		t.Errorf("weighted centroid lon = %v, want 1", got)
	}

	unweighted := execSQL(t, db, `SELECT GEO_CENTROID_AGG(geom) AS c FROM towns`)
	var pu map[string]any
	if err := json.Unmarshal([]byte(unweighted.Rows[0]["c"].(string)), &pu); err != nil {
		t.Fatalf("unweighted centroid JSON: %v", err)
	}
	uc := pu["coordinates"].([]any)
	// plain average of centroids: (0+10+0)/3 = 3.333...
	if got := uc[0].(float64); math.Abs(got-10.0/3.0) > 1e-9 {
		t.Errorf("unweighted centroid lon = %v, want %v", got, 10.0/3.0)
	}
}
