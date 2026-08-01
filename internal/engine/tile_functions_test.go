package engine

// Tests for the Web Mercator tile functions.
//
// Tile math is easy to get subtly wrong and hard to notice: an off-by-one or a
// missing TMS flip still returns plausible tiles, just in the wrong places. So
// rather than assert numbers derived by hand, these tests lean on values that
// are exact by construction (the zoom-0 and zoom-1 tile corners), the quadkey
// example from Microsoft's own documentation, and round-trip/containment
// properties checked over a spread of coordinates.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func tileEval(t *testing.T, sql string) any {
	t.Helper()
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse("SELECT "+sql+" AS v"))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("%s returned %d rows, want 1", sql, len(rs.Rows))
	}
	v, _ := ragValue(rs.Rows[0], "v")
	return v
}

func tileEvalErr(t *testing.T, sql string) error {
	t.Helper()
	db := storage.NewDB()
	_, err := Execute(context.Background(), db, "default", mustParse("SELECT "+sql+" AS v"))
	return err
}

func tileEvalInt(t *testing.T, sql string) int {
	t.Helper()
	n, err := toInt(tileEval(t, sql))
	if err != nil {
		t.Fatalf("%s did not return an int: %v", sql, err)
	}
	return n
}

// TestTileExactCorners pins the cases that follow directly from the projection
// definition, with no arithmetic to get wrong.
func TestTileExactCorners(t *testing.T) {
	// Zoom 0 is a single tile covering the world.
	if got := tileEvalInt(t, "TILE_X(-180, 0)"); got != 0 {
		t.Errorf("TILE_X(-180,0) = %d, want 0", got)
	}
	if got := tileEvalInt(t, "TILE_X(179.999, 0)"); got != 0 {
		t.Errorf("TILE_X(179.999,0) = %d, want 0", got)
	}
	if got := tileEvalInt(t, "TILE_Y(85.0, 0)"); got != 0 {
		t.Errorf("TILE_Y(85,0) = %d, want 0", got)
	}
	if got := tileEvalInt(t, "TILE_Y(-85.0, 0)"); got != 0 {
		t.Errorf("TILE_Y(-85,0) = %d, want 0", got)
	}

	// At zoom 1 the prime meridian and equator are the shared tile corner, and
	// floor() puts that exact point in the south-east quadrant: x=1, y=1.
	if got := tileEvalInt(t, "TILE_X(0, 1)"); got != 1 {
		t.Errorf("TILE_X(0,1) = %d, want 1", got)
	}
	if got := tileEvalInt(t, "TILE_Y(0, 1)"); got != 1 {
		t.Errorf("TILE_Y(0,1) = %d, want 1", got)
	}
	// North-west quadrant at zoom 1.
	if got := tileEvalInt(t, "TILE_X(-90, 1)"); got != 0 {
		t.Errorf("TILE_X(-90,1) = %d, want 0", got)
	}
	if got := tileEvalInt(t, "TILE_Y(45, 1)"); got != 0 {
		t.Errorf("TILE_Y(45,1) = %d, want 0", got)
	}

	// Tile edges: west edge of column 0 is -180, of column 1 at zoom 1 is 0.
	if got := tileEval(t, "TILE_LON(0, 1)"); got != -180.0 {
		t.Errorf("TILE_LON(0,1) = %v, want -180", got)
	}
	if got := tileEval(t, "TILE_LON(1, 1)"); got != 0.0 {
		t.Errorf("TILE_LON(1,1) = %v, want 0", got)
	}
	// North edge of XYZ row 1 at zoom 1 is the equator; row 0 is the Mercator
	// latitude limit.
	if got, ok := tileEval(t, "TILE_LAT(1, 1)").(float64); !ok || math.Abs(got) > 1e-9 {
		t.Errorf("TILE_LAT(1,1) = %v, want 0", got)
	}
	if got, ok := tileEval(t, "TILE_LAT(0, 1)").(float64); !ok || math.Abs(got-tileMaxLat) > 1e-9 {
		t.Errorf("TILE_LAT(0,1) = %v, want %v", got, tileMaxLat)
	}
}

// TestTileBBoxZoom1 checks all four zoom-1 tiles, whose bounds are exact.
func TestTileBBoxZoom1(t *testing.T) {
	cases := []struct {
		x, y                     int
		west, south, east, north float64
	}{
		{0, 0, -180, 0, 0, tileMaxLat},
		{1, 0, 0, 0, 180, tileMaxLat},
		{0, 1, -180, -tileMaxLat, 0, 0},
		{1, 1, 0, -tileMaxLat, 180, 0},
	}
	for _, tc := range cases {
		raw := tileEval(t, fmt.Sprintf("TILE_BBOX(1, %d, %d)", tc.x, tc.y))
		s, ok := raw.(string)
		if !ok {
			t.Fatalf("TILE_BBOX returned %T, want a JSON string", raw)
		}
		var got []float64
		if err := json.Unmarshal([]byte(s), &got); err != nil {
			t.Fatalf("TILE_BBOX result %q is not a JSON array: %v", s, err)
		}
		want := []float64{tc.west, tc.south, tc.east, tc.north}
		if len(got) != 4 {
			t.Fatalf("TILE_BBOX returned %d values, want 4", len(got))
		}
		for i := range want {
			if math.Abs(got[i]-want[i]) > 1e-9 {
				t.Errorf("tile 1/%d/%d bbox[%d] = %v, want %v", tc.x, tc.y, i, got[i], want[i])
			}
		}
	}
}

// TestTileQuadkeyDocumentedExample uses the example from Microsoft's Bing Maps
// tile system documentation: tile (3, 5) at level 3 has quadkey "213".
func TestTileQuadkeyDocumentedExample(t *testing.T) {
	if got := tileEval(t, "TILE_QUADKEY(3, 3, 5)"); got != "213" {
		t.Errorf("TILE_QUADKEY(3,3,5) = %v, want \"213\"", got)
	}
	// The four zoom-1 quadrants, in quadkey digit order.
	for _, tc := range []struct {
		x, y int
		want string
	}{{0, 0, "0"}, {1, 0, "1"}, {0, 1, "2"}, {1, 1, "3"}} {
		got := tileEval(t, fmt.Sprintf("TILE_QUADKEY(1, %d, %d)", tc.x, tc.y))
		if got != tc.want {
			t.Errorf("TILE_QUADKEY(1,%d,%d) = %v, want %q", tc.x, tc.y, got, tc.want)
		}
	}
	// Zoom 0 has a single tile and an empty quadkey.
	if got := tileEval(t, "TILE_QUADKEY(0, 0, 0)"); got != "" {
		t.Errorf("TILE_QUADKEY(0,0,0) = %q, want empty", got)
	}
}

// TestTileQuadkeyRoundTrip checks TILE_FROM_QUADKEY inverts TILE_QUADKEY.
func TestTileQuadkeyRoundTrip(t *testing.T) {
	for _, zoom := range []int{0, 1, 5, 12, 18} {
		side := 1 << uint(zoom)
		for _, frac := range []float64{0, 0.25, 0.5, 0.9999} {
			x := int(frac * float64(side))
			y := int((1 - frac) * float64(side))
			if x >= side {
				x = side - 1
			}
			if y >= side {
				y = side - 1
			}
			qk := tileEval(t, fmt.Sprintf("TILE_QUADKEY(%d, %d, %d)", zoom, x, y))
			s, ok := qk.(string)
			if !ok {
				t.Fatalf("TILE_QUADKEY returned %T", qk)
			}
			if len(s) != zoom {
				t.Errorf("quadkey %q for zoom %d has length %d", s, zoom, len(s))
			}
			back := tileEval(t, fmt.Sprintf("TILE_FROM_QUADKEY('%s')", s))
			bs, ok := back.(string)
			if !ok {
				t.Fatalf("TILE_FROM_QUADKEY returned %T", back)
			}
			var got struct{ Z, X, Y int }
			if err := json.Unmarshal([]byte(bs), &got); err != nil {
				t.Fatalf("TILE_FROM_QUADKEY result %q: %v", bs, err)
			}
			if got.Z != zoom || got.X != x || got.Y != y {
				t.Errorf("round trip of %d/%d/%d via %q gave %d/%d/%d",
					zoom, x, y, s, got.Z, got.X, got.Y)
			}
		}
	}
}

// TestTileFlipYIsInvolution checks the XYZ<->TMS conversion is its own inverse
// and lands in range — the property that makes one function safe for both
// directions.
func TestTileFlipYIsInvolution(t *testing.T) {
	for _, zoom := range []int{0, 1, 2, 10, 20} {
		side := 1 << uint(zoom)
		for _, y := range []int{0, side / 3, side - 1} {
			flipped := tileEvalInt(t, fmt.Sprintf("TILE_FLIP_Y(%d, %d)", y, zoom))
			if flipped < 0 || flipped >= side {
				t.Fatalf("TILE_FLIP_Y(%d,%d) = %d, out of range 0..%d", y, zoom, flipped, side-1)
			}
			if got := tileEvalInt(t, fmt.Sprintf("TILE_FLIP_Y(%d, %d)", flipped, zoom)); got != y {
				t.Errorf("TILE_FLIP_Y is not an involution at zoom %d: %d -> %d -> %d",
					zoom, y, flipped, got)
			}
		}
		// Row 0 in one convention is the last row in the other.
		if got := tileEvalInt(t, fmt.Sprintf("TILE_FLIP_Y(0, %d)", zoom)); got != side-1 {
			t.Errorf("TILE_FLIP_Y(0,%d) = %d, want %d", zoom, got, side-1)
		}
		// TILE_ROW_TMS is documented as an alias.
		if got := tileEvalInt(t, fmt.Sprintf("TILE_ROW_TMS(0, %d)", zoom)); got != side-1 {
			t.Errorf("TILE_ROW_TMS(0,%d) = %d, want %d", zoom, got, side-1)
		}
	}
}

// TestTileContainsAgreesWithAddressing is the central consistency property: the
// tile a point maps to must be the tile that contains it, and its bounding box
// must bracket the point.
func TestTileContainsAgreesWithAddressing(t *testing.T) {
	lons := []float64{-179.9, -123.1, -0.1278, 0, 13.405, 100.5, 179.9}
	lats := []float64{-84.9, -33.86, -0.5, 0, 51.5074, 52.52, 84.9}

	for _, zoom := range []int{0, 1, 4, 10, 14} {
		for _, lon := range lons {
			for _, lat := range lats {
				x := tileEvalInt(t, fmt.Sprintf("TILE_X(%v, %d)", lon, zoom))
				y := tileEvalInt(t, fmt.Sprintf("TILE_Y(%v, %d)", lat, zoom))

				contains := tileEval(t, fmt.Sprintf("TILE_CONTAINS(%d, %d, %d, %v, %v)", zoom, x, y, lon, lat))
				if contains != true {
					t.Errorf("tile %d/%d/%d does not contain (%v,%v) it was derived from",
						zoom, x, y, lon, lat)
				}

				raw := tileEval(t, fmt.Sprintf("TILE_BBOX(%d, %d, %d)", zoom, x, y))
				var bbox []float64
				if err := json.Unmarshal([]byte(raw.(string)), &bbox); err != nil {
					t.Fatal(err)
				}
				// west <= lon <= east, south <= lat <= north, with a small
				// tolerance for the projection's round trip at tile edges.
				const eps = 1e-9
				if lon < bbox[0]-eps || lon > bbox[2]+eps {
					t.Errorf("lon %v outside bbox [%v,%v] of tile %d/%d/%d",
						lon, bbox[0], bbox[2], zoom, x, y)
				}
				if lat < bbox[1]-eps || lat > bbox[3]+eps {
					t.Errorf("lat %v outside bbox [%v,%v] of tile %d/%d/%d",
						lat, bbox[1], bbox[3], zoom, x, y)
				}
			}
		}
	}
}

// TestTileZXYIncludesTMSRow checks the convenience function reports both
// conventions consistently, since mixing them up is the failure this API exists
// to prevent.
func TestTileZXYIncludesTMSRow(t *testing.T) {
	raw := tileEval(t, "TILE_ZXY(13.405, 52.52, 14)")
	s, ok := raw.(string)
	if !ok {
		t.Fatalf("TILE_ZXY returned %T", raw)
	}
	var got struct {
		Z, X, Y int
		TileRow int `json:"tile_row"`
	}
	if err := json.Unmarshal([]byte(s), &got); err != nil {
		t.Fatalf("TILE_ZXY result %q: %v", s, err)
	}
	if got.Z != 14 {
		t.Errorf("z = %d, want 14", got.Z)
	}
	if want := tileEvalInt(t, "TILE_X(13.405, 14)"); got.X != want {
		t.Errorf("x = %d, want %d", got.X, want)
	}
	if want := tileEvalInt(t, "TILE_Y(52.52, 14)"); got.Y != want {
		t.Errorf("y = %d, want %d", got.Y, want)
	}
	if want := (1 << 14) - 1 - got.Y; got.TileRow != want {
		t.Errorf("tile_row = %d, want %d (2^14-1-%d)", got.TileRow, want, got.Y)
	}
}

// TestTileLatitudeClamping checks latitudes beyond the Web Mercator limit are
// clamped into the edge rows instead of producing an out-of-range tile.
func TestTileLatitudeClamping(t *testing.T) {
	for _, zoom := range []int{1, 8, 14} {
		side := 1 << uint(zoom)
		if got := tileEvalInt(t, fmt.Sprintf("TILE_Y(90, %d)", zoom)); got != 0 {
			t.Errorf("TILE_Y(90,%d) = %d, want 0 (clamped north)", zoom, got)
		}
		if got := tileEvalInt(t, fmt.Sprintf("TILE_Y(-90, %d)", zoom)); got != side-1 {
			t.Errorf("TILE_Y(-90,%d) = %d, want %d (clamped south)", zoom, got, side-1)
		}
	}
}

// TestTileLongitudeWraps checks longitude normalization rather than clamping, so
// a dateline-crossing value lands in a real column instead of collapsing onto
// the last one.
func TestTileLongitudeWraps(t *testing.T) {
	// +180 and -180 are the same meridian; both belong to column 0 going east.
	if got := tileEvalInt(t, "TILE_X(180, 2)"); got != 0 {
		t.Errorf("TILE_X(180,2) = %d, want 0", got)
	}
	if got := tileEvalInt(t, "TILE_X(-180, 2)"); got != 0 {
		t.Errorf("TILE_X(-180,2) = %d, want 0", got)
	}
	// 190 degrees east == -170.
	if got, want := tileEvalInt(t, "TILE_X(190, 4)"), tileEvalInt(t, "TILE_X(-170, 4)"); got != want {
		t.Errorf("TILE_X(190,4) = %d but TILE_X(-170,4) = %d; longitude should wrap", got, want)
	}
}

// TestTileParent walks a tile up one level and checks it stays inside its
// parent, with zoom 0 having no parent.
func TestTileParent(t *testing.T) {
	if got := tileEval(t, "TILE_PARENT(0, 0, 0)"); got != nil {
		t.Errorf("TILE_PARENT at zoom 0 = %v, want NULL", got)
	}
	raw := tileEval(t, "TILE_PARENT(14, 8802, 5373)")
	var got struct{ Z, X, Y int }
	if err := json.Unmarshal([]byte(raw.(string)), &got); err != nil {
		t.Fatal(err)
	}
	if got.Z != 13 || got.X != 4401 || got.Y != 2686 {
		t.Errorf("TILE_PARENT(14,8802,5373) = %d/%d/%d, want 13/4401/2686", got.Z, got.X, got.Y)
	}
}

func TestTileCount(t *testing.T) {
	for _, tc := range []struct {
		zoom int
		want int
	}{{0, 1}, {1, 4}, {2, 16}, {10, 1048576}} {
		if got := tileEvalInt(t, fmt.Sprintf("TILE_COUNT(%d)", tc.zoom)); got != tc.want {
			t.Errorf("TILE_COUNT(%d) = %d, want %d", tc.zoom, got, tc.want)
		}
	}
}

// TestTileArgumentValidation checks out-of-range inputs are rejected rather than
// silently producing a wrong tile.
func TestTileArgumentValidation(t *testing.T) {
	cases := []string{
		"TILE_X(0, -1)",             // negative zoom
		"TILE_X(0, 99)",             // zoom past the limit
		"TILE_FLIP_Y(4, 1)",         // y beyond 2^zoom-1
		"TILE_FLIP_Y(-1, 4)",        // negative y
		"TILE_BBOX(1, 2, 0)",        // x beyond 2^zoom-1
		"TILE_QUADKEY(2, 0, 4)",     // y beyond 2^zoom-1
		"TILE_FROM_QUADKEY('0129')", // invalid digit
	}
	for _, sql := range cases {
		if err := tileEvalErr(t, sql); err == nil {
			t.Errorf("%s should have failed but did not", sql)
		}
	}
}

// TestTileFunctionsAgainstTileTable exercises the functions the way a tileset
// query does: turning a coordinate into an MBTiles tile_row lookup.
func TestTileFunctionsAgainstTileTable(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE tiles (zoom_level INT, tile_column INT, tile_row INT, label TEXT)`,
		// tile_row values are TMS, as the MBTiles specification stores them: XYZ
		// row 0 (northernmost) is tile_row 2^zoom-1. Real tile_data BLOBs are
		// exercised by the MBTiles import/export round-trip test.
		`INSERT INTO tiles VALUES (1, 0, 1, 'north-west')`,
		`INSERT INTO tiles VALUES (1, 1, 1, 'north-east')`,
		`INSERT INTO tiles VALUES (1, 0, 0, 'south-west')`,
		`INSERT INTO tiles VALUES (1, 1, 0, 'south-east')`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Berlin is north of the equator and east of the meridian: north-east tile.
	rs, err := Execute(ctx, db, "default", mustParse(`
		SELECT label FROM tiles
		WHERE zoom_level = 1
		  AND tile_column = TILE_X(13.405, 1)
		  AND tile_row = TILE_FLIP_Y(TILE_Y(52.52, 1), 1)`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected exactly one tile, got %d", len(rs.Rows))
	}
	if v, _ := ragValue(rs.Rows[0], "label"); fmt.Sprintf("%s", v) != "north-east" {
		t.Errorf("Berlin at zoom 1 resolved to %v, want north-east", v)
	}

	// Cape Town is south and east: south-east tile. Without the TMS flip this
	// would return the northern tile, which is the bug the flip prevents.
	rs, err = Execute(ctx, db, "default", mustParse(`
		SELECT label FROM tiles
		WHERE zoom_level = 1
		  AND tile_column = TILE_X(18.42, 1)
		  AND tile_row = TILE_FLIP_Y(TILE_Y(-33.92, 1), 1)`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected exactly one tile, got %d", len(rs.Rows))
	}
	if v, _ := ragValue(rs.Rows[0], "label"); fmt.Sprintf("%s", v) != "south-east" {
		t.Errorf("Cape Town at zoom 1 resolved to %v, want south-east", v)
	}
}
