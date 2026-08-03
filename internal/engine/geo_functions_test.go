package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoSimplifyDouglasPeuckerAndAlias(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT
			GEO_SIMPLIFY('{"type":"LineString","coordinates":[[0,0],[1,0.1],[2,0],[3,2],[4,2]]}', 0.2, 'dp') AS geo,
			ST_SIMPLIFY('{"type":"LineString","coordinates":[[0,0],[1,0.1],[2,0],[3,2],[4,2]]}', 0.2, 'douglas-peucker') AS st
	`))
	if err != nil {
		t.Fatalf("GEO_SIMPLIFY: %v", err)
	}
	for _, column := range []string{"geo", "st"} {
		var object map[string]any
		if err := json.Unmarshal([]byte(rs.Rows[0][column].(string)), &object); err != nil {
			t.Fatalf("%s result is not GeoJSON: %v", column, err)
		}
		coordinates := object["coordinates"].([]any)
		if len(coordinates) != 4 {
			t.Errorf("%s retained %d positions, want 4: %v", column, len(coordinates), coordinates)
		}
		if coordinates[0].([]any)[0] != float64(0) || coordinates[len(coordinates)-1].([]any)[0] != float64(4) {
			t.Errorf("%s did not preserve endpoints: %v", column, coordinates)
		}
	}
}

func TestGeoSimplifyVisvalingamMethodsAndPolygonClosure(t *testing.T) {
	db := storage.NewDB()
	polygon := `{"type":"Polygon","coordinates":[[[0,0],[1,0.01],[2,0],[2,2],[1,2.01],[0,2],[0,0]]]}`
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_SIMPLIFY('%s', 0.02, 'visvalingam-effective') AS effective,
			GEO_SIMPLIFY('%s', 0.02, 'visvalingam-weighted') AS weighted
	`, polygon, polygon)))
	if err != nil {
		t.Fatalf("Visvalingam simplification: %v", err)
	}
	for _, column := range []string{"effective", "weighted"} {
		var object map[string]any
		if err := json.Unmarshal([]byte(rs.Rows[0][column].(string)), &object); err != nil {
			t.Fatalf("%s result is not GeoJSON: %v", column, err)
		}
		ring := object["coordinates"].([]any)[0].([]any)
		if len(ring) < 4 {
			t.Fatalf("%s collapsed polygon ring to %d positions", column, len(ring))
		}
		first, last := ring[0].([]any), ring[len(ring)-1].([]any)
		if first[0] != last[0] || first[1] != last[1] {
			t.Errorf("%s did not preserve ring closure: %v", column, ring)
		}
		if len(ring) >= 7 {
			t.Errorf("%s did not remove any low-area vertices: %v", column, ring)
		}
	}
}

func TestGeoSimplifyPreservesFeatureContainers(t *testing.T) {
	db := storage.NewDB()
	geojson := `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"road"},"geometry":{"type":"LineString","coordinates":[[0,0],[1,0],[2,0]]}}]}`
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT ST_SIMPLIFY('%s', 0.1) AS simplified
	`, geojson)))
	if err != nil {
		t.Fatalf("FeatureCollection simplification: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["simplified"].(string)), &object); err != nil {
		t.Fatalf("result is not GeoJSON: %v", err)
	}
	features := object["features"].([]any)
	properties := features[0].(map[string]any)["properties"].(map[string]any)
	if properties["name"] != "road" {
		t.Fatalf("feature properties were not preserved: %v", properties)
	}
	coordinates := features[0].(map[string]any)["geometry"].(map[string]any)["coordinates"].([]any)
	if len(coordinates) != 2 {
		t.Fatalf("expected collinear feature line to simplify to endpoints, got %d positions", len(coordinates))
	}
}

func TestGeoSimplifyRejectsInvalidMethodAndTolerance(t *testing.T) {
	db := storage.NewDB()
	cases := []string{
		`SELECT ST_SIMPLIFY('{"type":"LineString","coordinates":[[0,0],[1,1]]}', -1)`,
		`SELECT ST_SIMPLIFY('{"type":"LineString","coordinates":[[0,0],[1,1]]}', 1, 'unknown')`,
	}
	for _, sql := range cases {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err == nil {
			t.Errorf("%s succeeded, want an error", sql)
		}
	}
}

func TestGeoBBoxCentroidAndDropHoles(t *testing.T) {
	db := storage.NewDB()
	polygon := `{"type":"Polygon","coordinates":[[[-2,-2],[-2,2],[2,2],[2,-2],[-2,-2]],[[-1,-1],[-1,1],[1,1],[1,-1],[-1,-1]]]}`
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			ST_BBOX('%s') AS bbox,
			GEO_CENTROID('%s') AS centroid,
			ST_REMOVE_HOLES('%s') AS no_holes
	`, polygon, polygon, polygon)))
	if err != nil {
		t.Fatalf("geometry inspection/editing: %v", err)
	}
	var bbox []float64
	if err := json.Unmarshal([]byte(rs.Rows[0]["bbox"].(string)), &bbox); err != nil {
		t.Fatalf("bbox JSON: %v", err)
	}
	if fmt.Sprint(bbox) != "[-2 -2 2 2]" {
		t.Fatalf("bbox = %v, want [-2 -2 2 2]", bbox)
	}
	var centroid map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["centroid"].(string)), &centroid); err != nil {
		t.Fatalf("centroid JSON: %v", err)
	}
	coordinates := centroid["coordinates"].([]any)
	if coordinates[0] != float64(0) || coordinates[1] != float64(0) {
		t.Fatalf("centroid = %v, want [0 0]", coordinates)
	}
	var noHoles map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["no_holes"].(string)), &noHoles); err != nil {
		t.Fatalf("hole-free polygon JSON: %v", err)
	}
	if len(noHoles["coordinates"].([]any)) != 1 {
		t.Fatalf("hole removal retained %d rings", len(noHoles["coordinates"].([]any)))
	}
}

func TestGeoAffineAndSmooth(t *testing.T) {
	db := storage.NewDB()
	line := `{"type":"LineString","coordinates":[[0,0],[1,0],[2,0]]}`
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_AFFINE('%s', 1, 2, 2, 90, 0, 0) AS affine,
			ST_SMOOTH('%s', 1) AS smooth
	`, line, line)))
	if err != nil {
		t.Fatalf("affine/smooth: %v", err)
	}
	var affine map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["affine"].(string)), &affine); err != nil {
		t.Fatalf("affine JSON: %v", err)
	}
	affineCoords := affine["coordinates"].([]any)
	first := affineCoords[0].([]any)
	last := affineCoords[len(affineCoords)-1].([]any)
	if math.Abs(first[0].(float64)-1) > 1e-9 || math.Abs(first[1].(float64)-2) > 1e-9 || math.Abs(last[0].(float64)-1) > 1e-9 || math.Abs(last[1].(float64)-6) > 1e-9 {
		t.Fatalf("affine coordinates = %v, want endpoints [1 2] and [1 6]", affineCoords)
	}
	var smooth map[string]any
	if err := json.Unmarshal([]byte(rs.Rows[0]["smooth"].(string)), &smooth); err != nil {
		t.Fatalf("smooth JSON: %v", err)
	}
	if got := len(smooth["coordinates"].([]any)); got != 6 {
		t.Fatalf("smoothed line has %d positions, want 6", got)
	}
}

func TestGeoAffineRejectsPartialAnchor(t *testing.T) {
	db := storage.NewDB()
	if _, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT ST_AFFINE('{"type":"Point","coordinates":[0,0]}', 0, 0, 1, 0, 10)
	`)); err == nil {
		t.Fatal("partial affine anchor succeeded, want an error")
	}
}

func TestGeoDistanceCoordinates(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT GEO_DISTANCE(52.5200, 13.4050, 48.1372, 11.5755) AS dist
	`))
	if err != nil {
		t.Fatalf("GEO_DISTANCE failed: %v", err)
	}
	dist, ok := rs.Rows[0]["dist"].(float64)
	if !ok {
		t.Fatalf("dist = %T, want float64", rs.Rows[0]["dist"])
	}
	if math.Abs(dist-504000) > 3000 {
		t.Fatalf("Berlin-Munich distance = %v, want about 504km", dist)
	}
}

func TestGeoPointAccessorsAndDistanceAliases(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT
			ST_X(ST_MakePoint(13.4050, 52.5200)) AS lon,
			ST_Y(ST_MakePoint(13.4050, 52.5200)) AS lat,
			ST_DISTANCE(ST_MakePoint(13.4050, 52.5200), ST_MakePoint(11.5755, 48.1372)) AS dist,
			ST_DWITHIN(ST_MakePoint(13.4050, 52.5200), ST_MakePoint(11.5755, 48.1372), 600000) AS close
	`))
	if err != nil {
		t.Fatalf("geo aliases failed: %v", err)
	}
	expectFloat(t, rs.Rows[0]["lon"], 13.4050, 1e-9, "ST_X")
	expectFloat(t, rs.Rows[0]["lat"], 52.5200, 1e-9, "ST_Y")
	dist := rs.Rows[0]["dist"].(float64)
	if math.Abs(dist-504000) > 3000 {
		t.Fatalf("ST_DISTANCE = %v, want about 504km", dist)
	}
	if got, ok := rs.Rows[0]["close"].(bool); !ok || !got {
		t.Fatalf("ST_DWITHIN = %#v, want true", rs.Rows[0]["close"])
	}
}

func TestGeoWithinBBoxOnTableGeometry(t *testing.T) {
	db := storage.NewDB()
	for _, sql := range []string{
		`CREATE TABLE places (name TEXT, geometry JSON)`,
		`INSERT INTO places VALUES ('Berlin', GEO_POINT(13.4050, 52.5200))`,
		`INSERT INTO places VALUES ('Zurich', GEO_POINT(8.5417, 47.3769))`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err != nil {
			t.Fatalf("%s failed: %v", sql, err)
		}
	}

	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT name FROM places
		WHERE GEO_WITHIN_BBOX(geometry, 13.0, 52.0, 14.0, 53.0)
	`))
	if err != nil {
		t.Fatalf("GEO_WITHIN_BBOX failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Berlin" {
		t.Fatalf("unexpected bbox rows: %#v", rs.Rows)
	}
}

// TestGeoBearingCardinalDirections pins GEO_BEARING at the four cardinal
// directions, where the expected bearing has an exact, hand-verifiable value:
// moving along the equator or a meridian only changes one coordinate, so the
// great-circle bearing formula collapses to the compass direction itself.
func TestGeoBearingCardinalDirections(t *testing.T) {
	db := storage.NewDB()
	cases := []struct {
		name string
		sql  string
		want float64
	}{
		{"east along equator", `SELECT GEO_BEARING(0, 0, 0, 10) AS b`, 90},
		{"north along meridian", `SELECT GEO_BEARING(0, 0, 10, 0) AS b`, 0},
		{"west along equator", `SELECT GEO_BEARING(0, 0, 0, -10) AS b`, 270},
		{"south along meridian", `SELECT GEO_BEARING(0, 0, -10, 0) AS b`, 180},
	}
	for _, tc := range cases {
		rs, err := Execute(context.Background(), db, "default", mustParse(tc.sql))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		expectFloat(t, rs.Rows[0]["b"], tc.want, 1e-6, tc.name)
	}
}

// TestGeoBearingPointFormAndAzimuthAlias checks the (point, point) calling
// form and the ST_AZIMUTH alias agree with the 4-coordinate form.
func TestGeoBearingPointFormAndAzimuthAlias(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT
			GEO_BEARING(GEO_POINT(0, 0), GEO_POINT(10, 0)) AS point_form,
			ST_AZIMUTH(0, 0, 0, 10) AS azimuth_alias
	`))
	if err != nil {
		t.Fatalf("GEO_BEARING point form / ST_AZIMUTH: %v", err)
	}
	expectFloat(t, rs.Rows[0]["point_form"], 90, 1e-6, "GEO_BEARING(point, point) east")
	expectFloat(t, rs.Rows[0]["azimuth_alias"], 90, 1e-6, "ST_AZIMUTH")
}

// TestGeoMidpointEquatorAndMeridian uses the two cases where the great-circle
// midpoint has an exact, hand-verifiable answer: along the equator or a
// shared meridian, it coincides with the arithmetic mean of the coordinates.
func TestGeoMidpointEquatorAndMeridian(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT
			GEO_LON(GEO_MIDPOINT(0, 0, 0, 10)) AS eq_lon,
			GEO_LAT(GEO_MIDPOINT(0, 0, 0, 10)) AS eq_lat,
			GEO_LAT(GEO_MIDPOINT(0, 0, 10, 0)) AS mer_lat,
			GEO_LON(GEO_MIDPOINT(0, 0, 10, 0)) AS mer_lon
	`))
	if err != nil {
		t.Fatalf("GEO_MIDPOINT: %v", err)
	}
	expectFloat(t, rs.Rows[0]["eq_lon"], 5, 1e-6, "equator midpoint lon")
	expectFloat(t, rs.Rows[0]["eq_lat"], 0, 1e-6, "equator midpoint lat")
	expectFloat(t, rs.Rows[0]["mer_lat"], 5, 1e-6, "meridian midpoint lat")
	expectFloat(t, rs.Rows[0]["mer_lon"], 0, 1e-6, "meridian midpoint lon")
}

// TestGeoMidpointIsEquidistantFromBothEndpoints checks the general (non-axis-
// aligned) case the cardinal-direction test above cannot: the midpoint must
// be the same great-circle distance from each endpoint, and that distance
// must be half the endpoint-to-endpoint distance.
func TestGeoMidpointIsEquidistantFromBothEndpoints(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT
			GEO_DISTANCE(52.5200, 13.4050, GEO_LAT(GEO_MIDPOINT(52.5200, 13.4050, 48.1372, 11.5755)), GEO_LON(GEO_MIDPOINT(52.5200, 13.4050, 48.1372, 11.5755))) AS to_start,
			GEO_DISTANCE(48.1372, 11.5755, GEO_LAT(GEO_MIDPOINT(52.5200, 13.4050, 48.1372, 11.5755)), GEO_LON(GEO_MIDPOINT(52.5200, 13.4050, 48.1372, 11.5755))) AS to_end,
			GEO_DISTANCE(52.5200, 13.4050, 48.1372, 11.5755) AS total
	`))
	if err != nil {
		t.Fatalf("GEO_MIDPOINT equidistance: %v", err)
	}
	toStart := rs.Rows[0]["to_start"].(float64)
	toEnd := rs.Rows[0]["to_end"].(float64)
	total := rs.Rows[0]["total"].(float64)
	if math.Abs(toStart-toEnd) > 1 {
		t.Fatalf("midpoint is not equidistant: to_start=%v to_end=%v", toStart, toEnd)
	}
	if math.Abs(toStart-total/2) > 1 {
		t.Fatalf("midpoint distance = %v, want half of %v", toStart, total)
	}
}

// TestGeoDestinationRoundTripsWithBearingAndDistance projects a point east
// and north by exactly the distance GEO_DISTANCE reports to the equator/
// meridian test points, and checks the projection lands back on them --
// GEO_DESTINATION is the inverse of GEO_BEARING+GEO_DISTANCE.
func TestGeoDestinationRoundTripsWithBearingAndDistance(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_LON(GEO_DESTINATION(0, 0, 90, %v)) AS east_lon,
			GEO_LAT(GEO_DESTINATION(0, 0, 90, %v)) AS east_lat,
			GEO_LAT(GEO_DESTINATION(0, 0, 0, %v)) AS north_lat,
			GEO_LON(GEO_DESTINATION(0, 0, 0, %v)) AS north_lon
	`, haversineMeters(0, 0, 0, 10), haversineMeters(0, 0, 0, 10), haversineMeters(0, 0, 10, 0), haversineMeters(0, 0, 10, 0))))
	if err != nil {
		t.Fatalf("GEO_DESTINATION: %v", err)
	}
	expectFloat(t, rs.Rows[0]["east_lon"], 10, 1e-6, "destination east lon")
	expectFloat(t, rs.Rows[0]["east_lat"], 0, 1e-6, "destination east lat")
	expectFloat(t, rs.Rows[0]["north_lat"], 10, 1e-6, "destination north lat")
	expectFloat(t, rs.Rows[0]["north_lon"], 0, 1e-6, "destination north lon")
}

// TestGeoDestinationCoordinateOrderMatchesGeoDistance pins the 4-argument
// form's parameter order: (lat, lon, bearing, distance), matching
// GEO_DISTANCE(lat1, lon1, lat2, lon2) rather than GEO_POINT's (lon, lat).
// Due north (bearing 0) is the case where this is unambiguous regardless of
// starting latitude: it moves exactly along a meridian, so longitude must
// come back completely unchanged and latitude must increase by exactly
// distance/earthRadius, in radians. Using a non-equatorial, non-symmetric
// starting point (lat != lon) means a swapped argument order would be caught
// here, unlike the equator-anchored (0, 0, ...) cases above.
func TestGeoDestinationCoordinateOrderMatchesGeoDistance(t *testing.T) {
	db := storage.NewDB()
	const startLat, startLon = 30.0, 77.0
	const twoDegreesMeters = geoEarthRadiusMeters * 2 * math.Pi / 180
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_LAT(GEO_DESTINATION(%v, %v, 0, %v)) AS dest_lat,
			GEO_LON(GEO_DESTINATION(%v, %v, 0, %v)) AS dest_lon
	`, startLat, startLon, twoDegreesMeters, startLat, startLon, twoDegreesMeters)))
	if err != nil {
		t.Fatalf("GEO_DESTINATION coordinate order: %v", err)
	}
	expectFloat(t, rs.Rows[0]["dest_lat"], startLat+2, 1e-6, "destination latitude (due north, +2 degrees)")
	expectFloat(t, rs.Rows[0]["dest_lon"], startLon, 1e-6, "destination longitude (due north, unchanged)")
}

// TestGeoDestinationPointFormAndProjectAlias checks the (point, bearing,
// distance) calling form and the ST_PROJECT alias.
func TestGeoDestinationPointFormAndProjectAlias(t *testing.T) {
	db := storage.NewDB()
	dist := haversineMeters(0, 0, 0, 10)
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_LON(GEO_DESTINATION(GEO_POINT(0, 0), 90, %v)) AS point_form_lon,
			GEO_LON(ST_PROJECT(0, 0, 90, %v)) AS alias_lon
	`, dist, dist)))
	if err != nil {
		t.Fatalf("GEO_DESTINATION point form / ST_PROJECT: %v", err)
	}
	expectFloat(t, rs.Rows[0]["point_form_lon"], 10, 1e-6, "GEO_DESTINATION(point, ...)")
	expectFloat(t, rs.Rows[0]["alias_lon"], 10, 1e-6, "ST_PROJECT")
}

const geoTestPolygonWithHole = `{"type":"Polygon","coordinates":[` +
	`[[-2,-2],[-2,2],[2,2],[2,-2],[-2,-2]],` +
	`[[-1,-1],[-1,1],[1,1],[1,-1],[-1,-1]]` +
	`]}`

// TestGeoWithinPolygonRespectsHoles checks point-in-polygon against a square
// with a square hole: the center (inside the hole) must read as outside, a
// point between the hole and the outer boundary must read as inside, and a
// point outside the outer boundary entirely must also read as outside.
func TestGeoWithinPolygonRespectsHoles(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_WITHIN_POLYGON(GEO_POINT(0, 0), '%s') AS in_hole,
			GEO_WITHIN_POLYGON(GEO_POINT(1.5, 1.5), '%s') AS between_hole_and_edge,
			GEO_WITHIN_POLYGON(GEO_POINT(3, 3), '%s') AS outside_entirely
	`, geoTestPolygonWithHole, geoTestPolygonWithHole, geoTestPolygonWithHole)))
	if err != nil {
		t.Fatalf("GEO_WITHIN_POLYGON: %v", err)
	}
	if got := rs.Rows[0]["in_hole"]; got != false {
		t.Errorf("point inside the hole: within = %#v, want false", got)
	}
	if got := rs.Rows[0]["between_hole_and_edge"]; got != true {
		t.Errorf("point between hole and outer edge: within = %#v, want true", got)
	}
	if got := rs.Rows[0]["outside_entirely"]; got != false {
		t.Errorf("point outside the polygon entirely: within = %#v, want false", got)
	}
}

// TestGeoPolygonContainsIsWithinPolygonReversed checks ST_CONTAINS(polygon,
// point) computes the same predicate as GEO_WITHIN_POLYGON(point, polygon),
// with PostGIS's reversed argument order.
func TestGeoPolygonContainsIsWithinPolygonReversed(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			ST_CONTAINS('%s', GEO_POINT(1.5, 1.5)) AS contains_true,
			ST_CONTAINS('%s', GEO_POINT(0, 0)) AS contains_hole_false
	`, geoTestPolygonWithHole, geoTestPolygonWithHole)))
	if err != nil {
		t.Fatalf("ST_CONTAINS: %v", err)
	}
	if got := rs.Rows[0]["contains_true"]; got != true {
		t.Errorf("ST_CONTAINS true case = %#v, want true", got)
	}
	if got := rs.Rows[0]["contains_hole_false"]; got != false {
		t.Errorf("ST_CONTAINS hole case = %#v, want false", got)
	}
}

// TestGeoPolygonAreaMatchesHaversineApproximationForSmallSquare checks
// GEO_POLYGON_AREA's spherical formula against an independent estimate built
// from the same haversineMeters GEO_DISTANCE already relies on: for a square
// small enough that curvature is negligible, area ≈ side_lon_meters *
// side_lat_meters.
func TestGeoPolygonAreaMatchesHaversineApproximationForSmallSquare(t *testing.T) {
	db := storage.NewDB()
	const d = 0.01
	polygon := fmt.Sprintf(`{"type":"Polygon","coordinates":[[[0,0],[0,%v],[%v,%v],[%v,0],[0,0]]]}`, d, d, d, d)
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(
		`SELECT GEO_POLYGON_AREA('%s') AS area`, polygon)))
	if err != nil {
		t.Fatalf("GEO_POLYGON_AREA: %v", err)
	}
	sideLon := haversineMeters(0, 0, 0, d)
	sideLat := haversineMeters(0, 0, d, 0)
	want := sideLon * sideLat
	area, ok := rs.Rows[0]["area"].(float64)
	if !ok {
		t.Fatalf("area = %T, want float64", rs.Rows[0]["area"])
	}
	if relErr := math.Abs(area-want) / want; relErr > 0.01 {
		t.Fatalf("area = %v, want ~%v (haversine-side approximation), relative error %v", area, want, relErr)
	}
}

// TestGeoPolygonAreaSubtractsHoles checks the hole-punched polygon's area
// equals the outer square's area minus the inner square's area.
func TestGeoPolygonAreaSubtractsHoles(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(`
		SELECT
			GEO_POLYGON_AREA('%s') AS with_hole,
			GEO_POLYGON_AREA('{"type":"Polygon","coordinates":[[[-2,-2],[-2,2],[2,2],[2,-2],[-2,-2]]]}') AS outer_only,
			GEO_POLYGON_AREA('{"type":"Polygon","coordinates":[[[-1,-1],[-1,1],[1,1],[1,-1],[-1,-1]]]}') AS hole_only
	`, geoTestPolygonWithHole)))
	if err != nil {
		t.Fatalf("GEO_POLYGON_AREA hole subtraction: %v", err)
	}
	withHole := rs.Rows[0]["with_hole"].(float64)
	outerOnly := rs.Rows[0]["outer_only"].(float64)
	holeOnly := rs.Rows[0]["hole_only"].(float64)
	want := outerOnly - holeOnly
	if relErr := math.Abs(withHole-want) / want; relErr > 1e-9 {
		t.Fatalf("area with hole = %v, want outer(%v) - hole(%v) = %v", withHole, outerOnly, holeOnly, want)
	}
}

// TestGeoLengthSumsSegments checks GEO_LENGTH on a multi-vertex LineString
// equals the sum of GEO_DISTANCE over each consecutive pair of vertices.
func TestGeoLengthSumsSegments(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT GEO_LENGTH('{"type":"LineString","coordinates":[[0,0],[0,10],[5,10]]}') AS len
	`))
	if err != nil {
		t.Fatalf("GEO_LENGTH: %v", err)
	}
	want := haversineMeters(0, 0, 10, 0) + haversineMeters(10, 0, 10, 5)
	length, ok := rs.Rows[0]["len"].(float64)
	if !ok {
		t.Fatalf("len = %T, want float64", rs.Rows[0]["len"])
	}
	if math.Abs(length-want) > 1 {
		t.Fatalf("GEO_LENGTH = %v, want %v (sum of leg distances)", length, want)
	}
}

// TestGeoLengthAndAreaAndPolygonRejectWrongGeometryType checks that feeding a
// Point where a LineString/Polygon is expected (or vice versa) errors instead
// of silently misreading the geometry.
func TestGeoLengthAndAreaAndPolygonRejectWrongGeometryType(t *testing.T) {
	db := storage.NewDB()
	for _, sql := range []string{
		`SELECT GEO_LENGTH(GEO_POINT(0, 0))`,
		`SELECT GEO_POLYGON_AREA(GEO_POINT(0, 0))`,
		`SELECT GEO_WITHIN_POLYGON(GEO_POINT(0, 0), GEO_POINT(1, 1))`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err == nil {
			t.Errorf("%s: expected a geometry-type error, got none", sql)
		}
	}
}
