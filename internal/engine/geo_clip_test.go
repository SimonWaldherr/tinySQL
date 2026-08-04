package engine

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoClipBoundaryFullyInsideSubject(t *testing.T) {
	db := storage.NewDB()
	subject := `{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}`
	boundary := `{"type":"Polygon","coordinates":[[[1,1],[3,1],[3,3],[1,3],[1,1]]]}`

	rs, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT GEO_CLIP('`+subject+`', '`+boundary+`') AS c`))
	if err != nil {
		t.Fatalf("GEO_CLIP: %v", err)
	}
	clipped, ok := rs.Rows[0]["c"].(string)
	if !ok {
		t.Fatalf("GEO_CLIP result is %T, want string", rs.Rows[0]["c"])
	}

	rs2, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT GEO_POLYGON_AREA('`+clipped+`') AS a, GEO_POLYGON_AREA('`+boundary+`') AS b`))
	if err != nil {
		t.Fatalf("area check: %v", err)
	}
	clippedArea := rs2.Rows[0]["a"].(float64)
	boundaryArea := rs2.Rows[0]["b"].(float64)
	if math.Abs(clippedArea-boundaryArea) > 1e-6*boundaryArea {
		t.Errorf("clip of a subject fully containing the boundary = area %v, want %v (the boundary's own area)", clippedArea, boundaryArea)
	}
}

// TestGeoClipCutsCornerAtExactHalf clips a square against a triangular
// convex boundary whose hypotenuse runs exactly through two of the
// square's own vertices, cutting it precisely in half.
func TestGeoClipCutsCornerAtExactHalf(t *testing.T) {
	db := storage.NewDB()
	square := `{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}`
	// x + y <= 2 inside this triangle; the hypotenuse from (3,-1) to (-1,3)
	// passes exactly through (2,0) and (0,2).
	triangle := `{"type":"Polygon","coordinates":[[[-1,-1],[3,-1],[-1,3],[-1,-1]]]}`

	rs, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT GEO_CLIP('`+square+`', '`+triangle+`') AS c, GEO_POLYGON_AREA('`+square+`') AS full_area`))
	if err != nil {
		t.Fatalf("GEO_CLIP: %v", err)
	}
	clipped := rs.Rows[0]["c"].(string)
	fullArea := rs.Rows[0]["full_area"].(float64)

	rs2, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT GEO_POLYGON_AREA('`+clipped+`') AS a`))
	if err != nil {
		t.Fatalf("clipped area: %v", err)
	}
	clippedArea := rs2.Rows[0]["a"].(float64)
	ratio := clippedArea / fullArea
	if math.Abs(ratio-0.5) > 1e-6 {
		t.Errorf("clipped/full area ratio = %v, want 0.5 (half the square)", ratio)
	}
}

func TestGeoClipRejectsNonConvexBoundaryByDefault(t *testing.T) {
	db := storage.NewDB()
	subject := `{"type":"Polygon","coordinates":[[[0.4,0.4],[0.6,0.4],[0.6,0.6],[0.4,0.6],[0.4,0.4]]]}`
	lShape := `{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,1],[1,1],[1,2],[0,2],[0,0]]]}`

	if _, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT GEO_CLIP('`+subject+`', '`+lShape+`') AS c`)); err == nil {
		t.Errorf("clipping against a non-convex boundary succeeded, want an error")
	}

	rs, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT GEO_CLIP('`+subject+`', '`+lShape+`', true) AS c`))
	if err != nil {
		t.Fatalf("GEO_CLIP with allow_nonconvex=true: %v", err)
	}
	if rs.Rows[0]["c"] == nil {
		t.Errorf("allow_nonconvex=true clip of a subject inside the boundary returned NULL, want a result")
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["c"].(string)), &object); err != nil {
		t.Fatalf("allow_nonconvex result is not valid JSON: %v", err)
	}
}
