package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const geoEarthRadiusMeters = 6371000.0

type geoPoint struct {
	Lon float64
	Lat float64
	Z   *float64
}

func getGeoFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_POINT":          evalGeoPoint,
		"ST_MAKEPOINT":       evalGeoPoint,
		"ST_POINT":           evalGeoPoint,
		"GEO_LON":            evalGeoLon,
		"GEO_X":              evalGeoLon,
		"ST_X":               evalGeoLon,
		"GEO_LAT":            evalGeoLat,
		"GEO_Y":              evalGeoLat,
		"ST_Y":               evalGeoLat,
		"GEO_DISTANCE":       evalGeoDistance,
		"HAVERSINE":          evalGeoDistance,
		"ST_DISTANCE":        evalGeoDistance,
		"GEO_DWITHIN":        evalGeoDWithin,
		"ST_DWITHIN":         evalGeoDWithin,
		"GEO_WITHIN_BBOX":    evalGeoWithinBBox,
		"ST_WITHIN_BBOX":     evalGeoWithinBBox,
		"GEO_BEARING":        evalGeoBearing,
		"ST_AZIMUTH":         evalGeoBearing,
		"GEO_DESTINATION":    evalGeoDestination,
		"ST_PROJECT":         evalGeoDestination,
		"GEO_MIDPOINT":       evalGeoMidpoint,
		"ST_MIDPOINT":        evalGeoMidpoint,
		"GEO_WITHIN_POLYGON": evalGeoWithinPolygon,
		"ST_WITHIN":          evalGeoWithinPolygon,
		"ST_CONTAINS":        evalGeoPolygonContains,
		"GEO_POLYGON_AREA":   evalGeoPolygonArea,
		"ST_AREA":            evalGeoPolygonArea,
		"GEO_LENGTH":         evalGeoLength,
		"ST_LENGTH":          evalGeoLength,
	}
}

func evalGeoPoint(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 3); err != nil {
		return nil, err
	}
	lon, err := evalGeoFloatArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	lat, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	var z *float64
	if len(ex.Args) == 3 {
		v, err := evalGeoFloatArg(env, ex, row, 2)
		if err != nil {
			return nil, err
		}
		z = &v
	}
	return geoPointJSON(lon, lat, z)
}

// geoPointJSON encodes a GeoJSON Point, the wire format every geo function
// that returns a point (GEO_POINT itself, GEO_MIDPOINT, GEO_DESTINATION) uses
// so its result can round-trip straight back through GEO_LON/GEO_LAT/
// GEO_DISTANCE/etc. without a separate representation to support.
func geoPointJSON(lon, lat float64, z *float64) (any, error) {
	coords := []float64{lon, lat}
	if z != nil {
		coords = append(coords, *z)
	}
	body, err := json.Marshal(map[string]any{"type": "Point", "coordinates": coords})
	if err != nil {
		return nil, err
	}
	return string(body), nil
}

func evalGeoLon(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	p, err := evalGeoPointArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	return p.Lon, nil
}

func evalGeoLat(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	p, err := evalGeoPointArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	return p.Lat, nil
}

// evalGeoTwoPointsArgs resolves two coordinate pairs from either two GeoJSON
// Point arguments or four raw numbers (lat1, lon1, lat2, lon2) -- the dual
// calling convention GEO_DISTANCE established and GEO_BEARING/GEO_MIDPOINT
// reuse, so a caller can pass whichever form already has the data in it.
func evalGeoTwoPointsArgs(env ExecEnv, ex *FuncCall, row Row) (lat1, lon1, lat2, lon2 float64, err error) {
	switch len(ex.Args) {
	case 2:
		var a, b geoPoint
		if a, err = evalGeoPointArg(env, ex, row, 0); err != nil {
			return 0, 0, 0, 0, err
		}
		if b, err = evalGeoPointArg(env, ex, row, 1); err != nil {
			return 0, 0, 0, 0, err
		}
		return a.Lat, a.Lon, b.Lat, b.Lon, nil
	case 4:
		if lat1, err = evalGeoFloatArg(env, ex, row, 0); err != nil {
			return 0, 0, 0, 0, err
		}
		if lon1, err = evalGeoFloatArg(env, ex, row, 1); err != nil {
			return 0, 0, 0, 0, err
		}
		if lat2, err = evalGeoFloatArg(env, ex, row, 2); err != nil {
			return 0, 0, 0, 0, err
		}
		if lon2, err = evalGeoFloatArg(env, ex, row, 3); err != nil {
			return 0, 0, 0, 0, err
		}
		return lat1, lon1, lat2, lon2, nil
	default:
		return 0, 0, 0, 0, fmt.Errorf("%s expects 2 GeoJSON points or 4 coordinates: (lat1, lon1, lat2, lon2)", ex.Name)
	}
}

func evalGeoDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	lat1, lon1, lat2, lon2, err := evalGeoTwoPointsArgs(env, ex, row)
	if err != nil {
		return nil, err
	}
	return haversineMeters(lat1, lon1, lat2, lon2), nil
}

func evalGeoDWithin(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	switch len(ex.Args) {
	case 3:
		dist, err := evalGeoDistance(env, &FuncCall{Name: ex.Name, Args: ex.Args[:2]}, row)
		if err != nil {
			return nil, err
		}
		maxMeters, err := evalGeoFloatArg(env, ex, row, 2)
		if err != nil {
			return nil, err
		}
		return dist.(float64) <= maxMeters, nil
	case 5:
		dist, err := evalGeoDistance(env, &FuncCall{Name: ex.Name, Args: ex.Args[:4]}, row)
		if err != nil {
			return nil, err
		}
		maxMeters, err := evalGeoFloatArg(env, ex, row, 4)
		if err != nil {
			return nil, err
		}
		return dist.(float64) <= maxMeters, nil
	default:
		return nil, fmt.Errorf("%s expects (point, point, meters) or (lat1, lon1, lat2, lon2, meters)", ex.Name)
	}
}

func evalGeoWithinBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 5, 5); err != nil {
		return nil, err
	}
	p, err := evalGeoPointArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	minLon, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	minLat, err := evalGeoFloatArg(env, ex, row, 2)
	if err != nil {
		return nil, err
	}
	maxLon, err := evalGeoFloatArg(env, ex, row, 3)
	if err != nil {
		return nil, err
	}
	maxLat, err := evalGeoFloatArg(env, ex, row, 4)
	if err != nil {
		return nil, err
	}
	if minLon > maxLon {
		minLon, maxLon = maxLon, minLon
	}
	if minLat > maxLat {
		minLat, maxLat = maxLat, minLat
	}
	return p.Lon >= minLon && p.Lon <= maxLon && p.Lat >= minLat && p.Lat <= maxLat, nil
}

// evalGeoBearing returns the initial compass bearing (0-360°, clockwise from
// true north) of the great-circle path between two points -- the direction a
// routing/navigation UI would draw an arrow in at the start of the leg.
func evalGeoBearing(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	lat1, lon1, lat2, lon2, err := evalGeoTwoPointsArgs(env, ex, row)
	if err != nil {
		return nil, err
	}
	return initialBearingDegrees(lat1, lon1, lat2, lon2), nil
}

// evalGeoMidpoint returns the great-circle midpoint between two points as a
// GeoJSON Point. This is not the arithmetic average of the coordinates --
// that would cut across the chord, not the sphere -- except along the
// equator or a shared meridian, where the two happen to coincide.
func evalGeoMidpoint(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	lat1, lon1, lat2, lon2, err := evalGeoTwoPointsArgs(env, ex, row)
	if err != nil {
		return nil, err
	}
	midLat, midLon := midpointDegrees(lat1, lon1, lat2, lon2)
	return geoPointJSON(midLon, midLat, nil)
}

// evalGeoDestination projects a starting point along an initial bearing for a
// great-circle distance, returning the destination as a GeoJSON Point -- the
// inverse of GEO_BEARING/GEO_DISTANCE (which go from two points to a bearing
// or a distance). Accepts either a GeoJSON point plus bearing and distance
// (3 args) or raw lat/lon plus bearing and distance (4 args), the same
// point-vs-coordinates duality used throughout this file.
func evalGeoDestination(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	var lat, lon float64
	var bearingIdx, distIdx int
	switch len(ex.Args) {
	case 3:
		p, err := evalGeoPointArg(env, ex, row, 0)
		if err != nil {
			return nil, err
		}
		lat, lon = p.Lat, p.Lon
		bearingIdx, distIdx = 1, 2
	case 4:
		var err error
		if lat, err = evalGeoFloatArg(env, ex, row, 0); err != nil {
			return nil, err
		}
		if lon, err = evalGeoFloatArg(env, ex, row, 1); err != nil {
			return nil, err
		}
		bearingIdx, distIdx = 2, 3
	default:
		return nil, fmt.Errorf("%s expects (point, bearing_deg, distance_m) or (lat, lon, bearing_deg, distance_m)", ex.Name)
	}
	bearing, err := evalGeoFloatArg(env, ex, row, bearingIdx)
	if err != nil {
		return nil, err
	}
	distance, err := evalGeoFloatArg(env, ex, row, distIdx)
	if err != nil {
		return nil, err
	}
	destLat, destLon := destinationPoint(lat, lon, bearing, distance)
	return geoPointJSON(destLon, destLat, nil)
}

// evalGeoWithinPolygon reports whether a point lies within a GeoJSON Polygon
// (its exterior ring, minus any holes). Point-first argument order matches
// GEO_WITHIN_BBOX; ST_WITHIN mirrors PostGIS's ST_Within(geomA, geomB) =
// "A is within B", which for (point, polygon) is the same order.
func evalGeoWithinPolygon(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	p, err := evalGeoPointArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	poly, err := evalGeoPolygonArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return pointInPolygon(p, poly), nil
}

// evalGeoPolygonContains is ST_CONTAINS(polygon, point), PostGIS's
// ST_Contains(geomA, geomB) = "A contains B" order -- the reverse of
// GEO_WITHIN_POLYGON/ST_WITHIN's point-first order. Both compute the same
// point-in-polygon predicate.
func evalGeoPolygonContains(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	poly, err := evalGeoPolygonArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	p, err := evalGeoPointArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return pointInPolygon(p, poly), nil
}

// evalGeoPolygonArea returns a GeoJSON Polygon's area in square meters
// (exterior ring minus any holes), computed on the sphere rather than by
// projecting to a plane first -- see ringAreaMeters.
func evalGeoPolygonArea(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	poly, err := evalGeoPolygonArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	return polygonAreaMeters(poly), nil
}

// evalGeoLength returns a GeoJSON LineString's length in meters: the sum of
// the great-circle distance between each consecutive pair of vertices.
func evalGeoLength(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	ls, err := evalGeoLineStringArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	total := 0.0
	for i := 1; i < len(ls); i++ {
		total += haversineMeters(ls[i-1].Lat, ls[i-1].Lon, ls[i].Lat, ls[i].Lon)
	}
	return total, nil
}

func evalGeoFloatArg(env ExecEnv, ex *FuncCall, row Row, idx int) (float64, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return 0, err
	}
	f, err := geoFloat(v)
	if err != nil {
		return 0, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return f, nil
}

func evalGeoPointArg(env ExecEnv, ex *FuncCall, row Row, idx int) (geoPoint, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return geoPoint{}, err
	}
	p, err := geoPointFromValue(v)
	if err != nil {
		return geoPoint{}, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return p, nil
}

func geoPointFromValue(v any) (geoPoint, error) {
	switch x := v.(type) {
	case map[string]any:
		return geoPointFromMap(x)
	case json.RawMessage:
		return geoPointFromJSON(x)
	case []byte:
		return geoPointFromJSON(x)
	case string:
		return geoPointFromJSON([]byte(strings.TrimSpace(x)))
	default:
		return geoPoint{}, fmt.Errorf("expected GeoJSON Point, got %T", v)
	}
}

func geoPointFromJSON(body []byte) (geoPoint, error) {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return geoPoint{}, err
	}
	return geoPointFromMap(obj)
}

func geoPointFromMap(obj map[string]any) (geoPoint, error) {
	if typ, _ := obj["type"].(string); !strings.EqualFold(typ, "Point") {
		return geoPoint{}, fmt.Errorf("expected GeoJSON Point")
	}
	coords, ok := obj["coordinates"].([]any)
	if !ok {
		return geoPoint{}, fmt.Errorf("point coordinates must be an array")
	}
	if len(coords) < 2 {
		return geoPoint{}, fmt.Errorf("point coordinates need lon and lat")
	}
	lon, err := geoFloat(coords[0])
	if err != nil {
		return geoPoint{}, fmt.Errorf("lon: %w", err)
	}
	lat, err := geoFloat(coords[1])
	if err != nil {
		return geoPoint{}, fmt.Errorf("lat: %w", err)
	}
	p := geoPoint{Lon: lon, Lat: lat}
	if len(coords) > 2 {
		z, err := geoFloat(coords[2])
		if err == nil {
			p.Z = &z
		}
	}
	return p, nil
}

// ── Polygon and LineString geometry ──────────────────────────────────────
//
// GEO_POINT/GEO_LON/GEO_DISTANCE and friends only ever needed a single
// position; polygon containment, area and line length need whole rings and
// vertex sequences, so they get their own geometry types and parsers here
// rather than overloading geoPoint.

// geoRing is one linear ring of a GeoJSON Polygon. By the GeoJSON
// specification, the first ring in a geoPolygon is the exterior boundary and
// any further rings are holes cut out of it.
type geoRing []geoPoint

// geoPolygon is a GeoJSON Polygon: an exterior ring plus zero or more holes.
type geoPolygon struct {
	Rings []geoRing
}

// geoLineString is a GeoJSON LineString: an ordered sequence of vertices.
type geoLineString []geoPoint

func evalGeoPolygonArg(env ExecEnv, ex *FuncCall, row Row, idx int) (geoPolygon, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return geoPolygon{}, err
	}
	poly, err := geoPolygonFromValue(v)
	if err != nil {
		return geoPolygon{}, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return poly, nil
}

func evalGeoLineStringArg(env ExecEnv, ex *FuncCall, row Row, idx int) (geoLineString, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return nil, err
	}
	ls, err := geoLineStringFromValue(v)
	if err != nil {
		return nil, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return ls, nil
}

func geoPolygonFromValue(v any) (geoPolygon, error) {
	obj, err := geoObjectFromValue(v)
	if err != nil {
		return geoPolygon{}, err
	}
	if typ, _ := obj["type"].(string); !strings.EqualFold(typ, "Polygon") {
		return geoPolygon{}, fmt.Errorf("expected GeoJSON Polygon")
	}
	rawRings, ok := obj["coordinates"].([]any)
	if !ok || len(rawRings) == 0 {
		return geoPolygon{}, fmt.Errorf("polygon coordinates must be a non-empty array of rings")
	}
	poly := geoPolygon{Rings: make([]geoRing, 0, len(rawRings))}
	for i, rawRing := range rawRings {
		positions, ok := rawRing.([]any)
		if !ok || len(positions) < 4 {
			return geoPolygon{}, fmt.Errorf("polygon ring %d must have at least 4 positions (a closed ring)", i)
		}
		ring := make(geoRing, 0, len(positions))
		for j, rawPos := range positions {
			p, err := geoPositionFromValue(rawPos)
			if err != nil {
				return geoPolygon{}, fmt.Errorf("polygon ring %d position %d: %w", i, j, err)
			}
			ring = append(ring, p)
		}
		poly.Rings = append(poly.Rings, ring)
	}
	return poly, nil
}

func geoLineStringFromValue(v any) (geoLineString, error) {
	obj, err := geoObjectFromValue(v)
	if err != nil {
		return nil, err
	}
	if typ, _ := obj["type"].(string); !strings.EqualFold(typ, "LineString") {
		return nil, fmt.Errorf("expected GeoJSON LineString")
	}
	positions, ok := obj["coordinates"].([]any)
	if !ok || len(positions) < 2 {
		return nil, fmt.Errorf("linestring coordinates must have at least 2 positions")
	}
	ls := make(geoLineString, 0, len(positions))
	for i, rawPos := range positions {
		p, err := geoPositionFromValue(rawPos)
		if err != nil {
			return nil, fmt.Errorf("linestring position %d: %w", i, err)
		}
		ls = append(ls, p)
	}
	return ls, nil
}

// geoObjectFromValue decodes v -- a GeoJSON string, []byte, json.RawMessage,
// or an already-decoded map -- into its top-level JSON object, the shared
// first step geoPolygonFromValue/geoLineStringFromValue need before checking
// their own "type". Point parsing (geoPointFromValue) predates this helper
// and is left as its own equivalent dispatch to avoid touching working,
// tested code for the sake of DRYing out a four-line type switch.
func geoObjectFromValue(v any) (map[string]any, error) {
	switch x := v.(type) {
	case map[string]any:
		return x, nil
	case json.RawMessage:
		return geoObjectFromJSON(x)
	case []byte:
		return geoObjectFromJSON(x)
	case string:
		return geoObjectFromJSON([]byte(strings.TrimSpace(x)))
	default:
		return nil, fmt.Errorf("expected GeoJSON geometry, got %T", v)
	}
}

func geoObjectFromJSON(body []byte) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// geoPositionFromValue parses one GeoJSON position: [lon, lat] or
// [lon, lat, z].
func geoPositionFromValue(v any) (geoPoint, error) {
	coords, ok := v.([]any)
	if !ok || len(coords) < 2 {
		return geoPoint{}, fmt.Errorf("position must be an array of at least [lon, lat]")
	}
	lon, err := geoFloat(coords[0])
	if err != nil {
		return geoPoint{}, fmt.Errorf("lon: %w", err)
	}
	lat, err := geoFloat(coords[1])
	if err != nil {
		return geoPoint{}, fmt.Errorf("lat: %w", err)
	}
	p := geoPoint{Lon: lon, Lat: lat}
	if len(coords) > 2 {
		if z, err := geoFloat(coords[2]); err == nil {
			p.Z = &z
		}
	}
	return p, nil
}

// pointInRing reports whether p is inside ring using the even-odd (ray
// casting) rule: a ray from p toward +infinity longitude crosses the ring's
// edges an odd number of times if and only if p is inside. This is the same
// algorithm used by, e.g., turf.js's booleanPointInPolygon, applied directly
// in lon/lat space -- adequate for the ring sizes tinySQL polygons realistically
// hold; it does not correct for antimeridian-crossing rings.
func pointInRing(p geoPoint, ring geoRing) bool {
	n := len(ring)
	if n < 3 {
		return false
	}
	inside := false
	j := n - 1
	for i := 0; i < n; i++ {
		xi, yi := ring[i].Lon, ring[i].Lat
		xj, yj := ring[j].Lon, ring[j].Lat
		if (yi > p.Lat) != (yj > p.Lat) {
			xCross := (xj-xi)*(p.Lat-yi)/(yj-yi) + xi
			if p.Lon < xCross {
				inside = !inside
			}
		}
		j = i
	}
	return inside
}

// pointInPolygon reports whether p is inside poly's exterior ring and
// outside every hole, per the GeoJSON convention that Rings[0] is the
// exterior boundary and Rings[1:] are holes.
func pointInPolygon(p geoPoint, poly geoPolygon) bool {
	if len(poly.Rings) == 0 || !pointInRing(p, poly.Rings[0]) {
		return false
	}
	for _, hole := range poly.Rings[1:] {
		if pointInRing(p, hole) {
			return false
		}
	}
	return true
}

// ringAreaMeters is the unsigned spherical area enclosed by ring, using the
// same trapezoidal-integration formula as Google's Android Maps Utils
// SphericalUtil.computeArea: summing (lon2-lon1)*(2+sin(lat1)+sin(lat2)) over
// every edge gives twice the signed area on a unit sphere, which is then
// scaled by the Earth's radius squared. This is exact on a sphere for any
// polygon size, unlike projecting to a local tangent plane first and running
// the planar shoelace formula, which only stays accurate for small polygons.
func ringAreaMeters(ring geoRing) float64 {
	n := len(ring)
	if n < 3 {
		return 0
	}
	total := 0.0
	for i := 0; i < n; i++ {
		p1 := ring[i]
		p2 := ring[(i+1)%n]
		lon1 := p1.Lon * math.Pi / 180
		lon2 := p2.Lon * math.Pi / 180
		lat1 := p1.Lat * math.Pi / 180
		lat2 := p2.Lat * math.Pi / 180
		total += (lon2 - lon1) * (2 + math.Sin(lat1) + math.Sin(lat2))
	}
	return math.Abs(total * geoEarthRadiusMeters * geoEarthRadiusMeters / 2)
}

// polygonAreaMeters is the exterior ring's area minus every hole's area.
func polygonAreaMeters(poly geoPolygon) float64 {
	if len(poly.Rings) == 0 {
		return 0
	}
	area := ringAreaMeters(poly.Rings[0])
	for _, hole := range poly.Rings[1:] {
		area -= ringAreaMeters(hole)
	}
	if area < 0 {
		return 0
	}
	return area
}

func geoFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int8:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint8:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case json.Number:
		return x.Float64()
	case string:
		return strconv.ParseFloat(strings.TrimSpace(x), 64)
	default:
		return 0, fmt.Errorf("expected numeric, got %T", v)
	}
}

func haversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	sinLat := math.Sin(dLat / 2)
	sinLon := math.Sin(dLon / 2)
	a := sinLat*sinLat + math.Cos(lat1Rad)*math.Cos(lat2Rad)*sinLon*sinLon
	if a > 1 {
		a = 1
	}
	return 2 * geoEarthRadiusMeters * math.Asin(math.Sqrt(a))
}

// initialBearingDegrees returns the initial compass bearing (0-360°,
// clockwise from true north) of the great-circle path from (lat1, lon1) to
// (lat2, lon2). Reference: the standard great-circle bearing formula (see
// e.g. "Calculate distance, bearing and more between two Latitude/Longitude
// points", Chris Veness, movable-type.co.uk).
func initialBearingDegrees(lat1, lon1, lat2, lon2 float64) float64 {
	phi1 := lat1 * math.Pi / 180
	phi2 := lat2 * math.Pi / 180
	deltaLambda := (lon2 - lon1) * math.Pi / 180
	y := math.Sin(deltaLambda) * math.Cos(phi2)
	x := math.Cos(phi1)*math.Sin(phi2) - math.Sin(phi1)*math.Cos(phi2)*math.Cos(deltaLambda)
	theta := math.Atan2(y, x)
	return math.Mod(theta*180/math.Pi+360, 360)
}

// destinationPoint projects (lat, lon) along bearingDeg for distanceMeters
// over the great-circle sphere, returning the destination coordinates. This
// is the direct/forward geodesic problem; initialBearingDegrees and
// haversineMeters together solve the inverse problem.
func destinationPoint(lat, lon, bearingDeg, distanceMeters float64) (destLat, destLon float64) {
	delta := distanceMeters / geoEarthRadiusMeters
	theta := bearingDeg * math.Pi / 180
	phi1 := lat * math.Pi / 180
	lambda1 := lon * math.Pi / 180

	sinPhi2 := math.Sin(phi1)*math.Cos(delta) + math.Cos(phi1)*math.Sin(delta)*math.Cos(theta)
	phi2 := math.Asin(clampUnit(sinPhi2))
	y := math.Sin(theta) * math.Sin(delta) * math.Cos(phi1)
	x := math.Cos(delta) - math.Sin(phi1)*sinPhi2
	lambda2 := lambda1 + math.Atan2(y, x)

	destLat = phi2 * 180 / math.Pi
	destLon = math.Mod(lambda2*180/math.Pi+540, 360) - 180 // normalize to (-180, 180]
	return destLat, destLon
}

// midpointDegrees returns the great-circle midpoint between two coordinates.
func midpointDegrees(lat1, lon1, lat2, lon2 float64) (midLat, midLon float64) {
	phi1 := lat1 * math.Pi / 180
	lambda1 := lon1 * math.Pi / 180
	phi2 := lat2 * math.Pi / 180
	deltaLambda := (lon2 - lon1) * math.Pi / 180

	bx := math.Cos(phi2) * math.Cos(deltaLambda)
	by := math.Cos(phi2) * math.Sin(deltaLambda)

	phi3 := math.Atan2(math.Sin(phi1)+math.Sin(phi2), math.Sqrt((math.Cos(phi1)+bx)*(math.Cos(phi1)+bx)+by*by))
	lambda3 := lambda1 + math.Atan2(by, math.Cos(phi1)+bx)

	midLat = phi3 * 180 / math.Pi
	midLon = math.Mod(lambda3*180/math.Pi+540, 360) - 180
	return midLat, midLon
}

// clampUnit keeps a value that should mathematically already be in [-1, 1]
// (the sine of a latitude) from tripping math.Asin's NaN on floating-point
// overshoot right at the poles.
func clampUnit(v float64) float64 {
	if v < -1 {
		return -1
	}
	if v > 1 {
		return 1
	}
	return v
}
