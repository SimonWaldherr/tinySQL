package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
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
		"GEO_POINT":                 evalGeoPoint,
		"ST_MAKEPOINT":              evalGeoPoint,
		"ST_POINT":                  evalGeoPoint,
		"GEO_LON":                   evalGeoLon,
		"GEO_X":                     evalGeoLon,
		"ST_X":                      evalGeoLon,
		"GEO_LAT":                   evalGeoLat,
		"GEO_Y":                     evalGeoLat,
		"ST_Y":                      evalGeoLat,
		"GEO_DISTANCE":              evalGeoDistance,
		"HAVERSINE":                 evalGeoDistance,
		"ST_DISTANCE":               evalGeoDistance,
		"GEO_DWITHIN":               evalGeoDWithin,
		"ST_DWITHIN":                evalGeoDWithin,
		"GEO_WITHIN_BBOX":           evalGeoWithinBBox,
		"ST_WITHIN_BBOX":            evalGeoWithinBBox,
		"GEO_BEARING":               evalGeoBearing,
		"ST_AZIMUTH":                evalGeoBearing,
		"GEO_DESTINATION":           evalGeoDestination,
		"ST_PROJECT":                evalGeoDestination,
		"GEO_MIDPOINT":              evalGeoMidpoint,
		"ST_MIDPOINT":               evalGeoMidpoint,
		"GEO_WITHIN_POLYGON":        evalGeoWithinPolygon,
		"ST_WITHIN":                 evalGeoWithinPolygon,
		"ST_CONTAINS":               evalGeoPolygonContains,
		"GEO_POLYGON_AREA":          evalGeoPolygonArea,
		"ST_AREA":                   evalGeoPolygonArea,
		"GEO_LENGTH":                evalGeoLength,
		"ST_LENGTH":                 evalGeoLength,
		"GEO_BUFFER":                evalGeoBuffer,
		"ST_BUFFER":                 evalGeoBuffer,
		"GEO_CONVEX_HULL":           evalGeoConvexHull,
		"ST_CONVEXHULL":             evalGeoConvexHull,
		"GEO_ENVELOPE":              evalGeoEnvelope,
		"ST_ENVELOPE":               evalGeoEnvelope,
		"GEO_LINE_INTERPOLATE":      evalGeoLineInterpolate,
		"ST_LINE_INTERPOLATE_POINT": evalGeoLineInterpolate,
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
	return geoPointWithinBBox(p, minLon, minLat, maxLon, maxLat), nil
}

// geoPointWithinBBox is the shared bounding-box containment test behind
// GEO_WITHIN_BBOX/ST_WITHIN_BBOX. GEO_SEARCH's bbox-mode residual filter
// (spatial_index.go) calls this directly too, rather than re-deriving the
// same six lines, so both paths agree on bounds ordering and inclusivity by
// construction.
func geoPointWithinBBox(p geoPoint, minLon, minLat, maxLon, maxLat float64) bool {
	if minLon > maxLon {
		minLon, maxLon = maxLon, minLon
	}
	if minLat > maxLat {
		minLat, maxLat = maxLat, minLat
	}
	return p.Lon >= minLon && p.Lon <= maxLon && p.Lat >= minLat && p.Lat <= maxLat
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
// or MultiPolygon (any one of its parts; each part's exterior ring minus its
// own holes). Point-first argument order matches GEO_WITHIN_BBOX; ST_WITHIN
// mirrors PostGIS's ST_Within(geomA, geomB) = "A is within B", which for
// (point, polygon) is the same order.
func evalGeoWithinPolygon(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	p, err := evalGeoPointArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	mp, err := evalGeoPolygonArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return pointInMultiPolygon(p, mp), nil
}

// evalGeoPolygonContains is ST_CONTAINS(polygon, point), PostGIS's
// ST_Contains(geomA, geomB) = "A contains B" order -- the reverse of
// GEO_WITHIN_POLYGON/ST_WITHIN's point-first order. Both compute the same
// point-in-polygon(-or-multipolygon) predicate.
func evalGeoPolygonContains(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	mp, err := evalGeoPolygonArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	p, err := evalGeoPointArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return pointInMultiPolygon(p, mp), nil
}

// evalGeoPolygonArea returns a GeoJSON Polygon or MultiPolygon's area in
// square meters (each part's exterior ring minus its own holes, summed for a
// MultiPolygon), computed on the sphere rather than by projecting to a plane
// first -- see ringAreaMeters.
func evalGeoPolygonArea(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	mp, err := evalGeoPolygonArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	return multiPolygonAreaMeters(mp), nil
}

// evalGeoBuffer approximates a circular buffer of radiusMeters around a
// point as a regular polygon, using the same forward-geodesic projection
// GEO_DESTINATION uses at `segments` equally spaced bearings (default 32,
// which keeps the circle visually smooth without an excessive vertex count;
// callers needing more or less detail can pass a third argument from 8 to
// 256). This is the standard way to turn "within X meters of here" into a
// polygon usable with GEO_WITHIN_POLYGON/ST_INTERSECTS-style predicates, or
// simply to draw a service-area circle on a map.
func evalGeoBuffer(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 3); err != nil {
		return nil, err
	}
	p, err := evalGeoPointArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	radius, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(radius) || math.IsInf(radius, 0) || radius <= 0 {
		return nil, fmt.Errorf("%s radius must be a finite positive number of meters", ex.Name)
	}
	segments := 32
	if len(ex.Args) == 3 {
		n, err := evalGeoFloatArg(env, ex, row, 2)
		if err != nil {
			return nil, err
		}
		if n != math.Trunc(n) || n < 8 || n > 256 {
			return nil, fmt.Errorf("%s segments must be an integer from 8 to 256", ex.Name)
		}
		segments = int(n)
	}
	ring := make([]any, 0, segments+1)
	for i := 0; i < segments; i++ {
		bearing := float64(i) * 360 / float64(segments)
		destLat, destLon := destinationPoint(p.Lat, p.Lon, bearing, radius)
		ring = append(ring, []float64{destLon, destLat})
	}
	ring = append(ring, ring[0]) // GeoJSON rings must be explicitly closed
	body, err := json.Marshal(map[string]any{"type": "Polygon", "coordinates": []any{ring}})
	if err != nil {
		return nil, err
	}
	return string(body), nil
}

// evalGeoConvexHull returns the convex hull of every vertex in a Point,
// MultiPoint, LineString, MultiLineString, Polygon or MultiPolygon as a
// GeoJSON Polygon. The hull is computed in plain lon/lat space (Andrew's
// monotone chain) rather than on the sphere -- a planar approximation that
// is standard practice for this operation and entirely adequate at the
// regional extents tinySQL geometries realistically span; a rigorous
// spherical hull needs a different algorithm and is out of scope here.
func evalGeoConvexHull(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	points, err := collectAllPositions(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	hull, err := convexHull(points)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	ring := make([]any, 0, len(hull)+1)
	for _, p := range hull {
		ring = append(ring, []float64{p.Lon, p.Lat})
	}
	ring = append(ring, ring[0])
	body, err := json.Marshal(map[string]any{"type": "Polygon", "coordinates": []any{ring}})
	if err != nil {
		return nil, err
	}
	return string(body), nil
}

// evalGeoEnvelope returns a geometry's bounding box as a GeoJSON Polygon
// (rather than the [minLon, minLat, maxLon, maxLat] array GEO_BBOX/ST_BBOX
// return), for when the caller wants to draw it, intersect it, or feed it
// into GEO_WITHIN_POLYGON directly. It reuses evalGeoBBox (geo_editing.go)
// rather than re-walking the geometry a second way.
func evalGeoEnvelope(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	bboxResult, err := evalGeoBBox(env, ex, row)
	if err != nil {
		return nil, err
	}
	if bboxResult == nil {
		return nil, nil
	}
	bboxJSON, ok := bboxResult.(string)
	if !ok {
		return nil, fmt.Errorf("%s: unexpected bounding box result type %T", ex.Name, bboxResult)
	}
	var bbox []float64
	if err := json.Unmarshal([]byte(bboxJSON), &bbox); err != nil || len(bbox) != 4 {
		return nil, fmt.Errorf("%s: could not read bounding box: %w", ex.Name, err)
	}
	minLon, minLat, maxLon, maxLat := bbox[0], bbox[1], bbox[2], bbox[3]
	ring := []any{
		[]float64{minLon, minLat},
		[]float64{maxLon, minLat},
		[]float64{maxLon, maxLat},
		[]float64{minLon, maxLat},
		[]float64{minLon, minLat},
	}
	body, err := json.Marshal(map[string]any{"type": "Polygon", "coordinates": []any{ring}})
	if err != nil {
		return nil, err
	}
	return string(body), nil
}

// evalGeoLineInterpolate returns the point a given fraction (0 to 1) of the
// way along a LineString's total great-circle length -- fraction 0 is the
// first vertex, 1 the last, 0.5 the midpoint by distance (not by vertex
// count). Useful for placing a marker partway along a route, or sampling a
// track at regular intervals. Between the two vertices bracketing the
// target distance, the point is linearly interpolated in lon/lat space
// (the vertices themselves are already straight-line segments in the
// stored data, not great-circle arcs, so this matches how the line is
// actually drawn rather than introducing spherical curvature it doesn't have).
func evalGeoLineInterpolate(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	ls, err := evalGeoLineStringArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	fraction, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(fraction) || fraction < 0 || fraction > 1 {
		return nil, fmt.Errorf("%s fraction must be between 0 and 1", ex.Name)
	}
	if len(ls) == 1 {
		return geoPointJSON(ls[0].Lon, ls[0].Lat, nil)
	}

	segLengths := make([]float64, len(ls)-1)
	total := 0.0
	for i := 1; i < len(ls); i++ {
		d := haversineMeters(ls[i-1].Lat, ls[i-1].Lon, ls[i].Lat, ls[i].Lon)
		segLengths[i-1] = d
		total += d
	}
	if total == 0 {
		return geoPointJSON(ls[0].Lon, ls[0].Lat, nil)
	}

	target := fraction * total
	covered := 0.0
	for i, segLen := range segLengths {
		if covered+segLen >= target || i == len(segLengths)-1 {
			t := 0.0
			if segLen > 0 {
				t = clampUnitInterval((target - covered) / segLen)
			}
			lon := ls[i].Lon + (ls[i+1].Lon-ls[i].Lon)*t
			lat := ls[i].Lat + (ls[i+1].Lat-ls[i].Lat)*t
			return geoPointJSON(lon, lat, nil)
		}
		covered += segLen
	}
	last := ls[len(ls)-1] // unreachable: the loop always returns on its final segment
	return geoPointJSON(last.Lon, last.Lat, nil)
}

func clampUnitInterval(t float64) float64 {
	if t < 0 {
		return 0
	}
	if t > 1 {
		return 1
	}
	return t
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

// geoMultiPolygon is one or more polygons: a GeoJSON Polygon parses as a
// single-element geoMultiPolygon, and a MultiPolygon as all of its parts.
// GEO_WITHIN_POLYGON/ST_CONTAINS treat membership in any part as membership
// in the whole; GEO_POLYGON_AREA sums every part's area.
type geoMultiPolygon struct {
	Polygons []geoPolygon
}

// geoLineString is a GeoJSON LineString: an ordered sequence of vertices.
type geoLineString []geoPoint

func evalGeoPolygonArg(env ExecEnv, ex *FuncCall, row Row, idx int) (geoMultiPolygon, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return geoMultiPolygon{}, err
	}
	mp, err := geoMultiPolygonFromValue(v)
	if err != nil {
		return geoMultiPolygon{}, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return mp, nil
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

// geoMultiPolygonFromValue accepts either a GeoJSON Polygon (returned as a
// single-element geoMultiPolygon) or a MultiPolygon (every part parsed the
// same way polygonFromRingsValue parses a Polygon's own coordinates).
func geoMultiPolygonFromValue(v any) (geoMultiPolygon, error) {
	obj, err := geoObjectFromValue(v)
	if err != nil {
		return geoMultiPolygon{}, err
	}
	typ, _ := obj["type"].(string)
	switch {
	case strings.EqualFold(typ, "Polygon"):
		poly, err := polygonFromRingsValue(obj["coordinates"])
		if err != nil {
			return geoMultiPolygon{}, err
		}
		return geoMultiPolygon{Polygons: []geoPolygon{poly}}, nil
	case strings.EqualFold(typ, "MultiPolygon"):
		rawPolygons, ok := obj["coordinates"].([]any)
		if !ok || len(rawPolygons) == 0 {
			return geoMultiPolygon{}, fmt.Errorf("multipolygon coordinates must be a non-empty array of polygons")
		}
		mp := geoMultiPolygon{Polygons: make([]geoPolygon, 0, len(rawPolygons))}
		for i, rawPolygon := range rawPolygons {
			poly, err := polygonFromRingsValue(rawPolygon)
			if err != nil {
				return geoMultiPolygon{}, fmt.Errorf("polygon %d: %w", i, err)
			}
			mp.Polygons = append(mp.Polygons, poly)
		}
		return mp, nil
	default:
		return geoMultiPolygon{}, fmt.Errorf("expected GeoJSON Polygon or MultiPolygon")
	}
}

// polygonFromRingsValue parses one Polygon's own "coordinates" value: an
// array of rings, the first the exterior boundary and the rest holes.
func polygonFromRingsValue(v any) (geoPolygon, error) {
	rawRings, ok := v.([]any)
	if !ok || len(rawRings) == 0 {
		return geoPolygon{}, fmt.Errorf("polygon coordinates must be a non-empty array of rings")
	}
	poly := geoPolygon{Rings: make([]geoRing, 0, len(rawRings))}
	for i, rawRing := range rawRings {
		positions, ok := rawRing.([]any)
		if !ok || len(positions) < 4 {
			return geoPolygon{}, fmt.Errorf("ring %d must have at least 4 positions (a closed ring)", i)
		}
		ring := make(geoRing, 0, len(positions))
		for j, rawPos := range positions {
			p, err := geoPositionFromValue(rawPos)
			if err != nil {
				return geoPolygon{}, fmt.Errorf("ring %d position %d: %w", i, j, err)
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

// canonicalGeoJSON decodes v as a GeoJSON Geometry object (Point, MultiPoint,
// LineString, MultiLineString, Polygon, MultiPolygon, or GeometryCollection --
// deliberately not Feature or FeatureCollection, which bundle non-geometry
// data that has no home in a single GEOMETRY column; extract .geometry first
// if you have one of those), validates its structural shape, and re-marshals
// it with encoding/json to get stable, canonical key ordering (json.Marshal
// sorts map keys). This is what both GEOMETRY column coercion
// (coerceToGeometry, coerce.go) and CAST(x AS GEOMETRY) (castValue,
// builtin_string.go) use, so a value entering the database through either
// path ends up as identical, predictable text.
func canonicalGeoJSON(v any) (string, error) {
	obj, err := geoObjectFromValue(v)
	if err != nil {
		return "", err
	}
	if err := validateGeometryShape(obj); err != nil {
		return "", err
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("encode geometry: %w", err)
	}
	return string(body), nil
}

// validateGeometryShape checks that object is a structurally well-formed
// GeoJSON Geometry: a legal "type" plus coordinates (or, for
// GeometryCollection, child geometries) whose nesting depth is consistent
// with that type. It is deliberately not stricter than the rest of this
// file's parsers (geoPointFromValue, geoMultiPolygonFromValue,
// geoLineStringFromValue, collectAllPositions all already accept exactly
// this shape) -- and it is deliberately more lenient than GEO_IS_VALID in
// one respect: it does not require closed rings or deduplicated vertices,
// since polygonFromRingsValue/pointInRing already tolerate both. Use
// GEO_IS_VALID/GEO_CLEAN separately to check those stronger, optional
// invariants.
func validateGeometryShape(object map[string]any) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "point":
		_, err := geoPositionFromValue(object["coordinates"])
		return err
	case "multipoint":
		_, err := positionsFromArray(object["coordinates"])
		return err
	case "linestring":
		positions, err := positionsFromArray(object["coordinates"])
		if err != nil {
			return err
		}
		if len(positions) < 2 {
			return fmt.Errorf("LineString needs at least 2 positions")
		}
		return nil
	case "multilinestring", "polygon":
		_, err := positionsFromNestedArray(object["coordinates"], 1)
		return err
	case "multipolygon":
		_, err := positionsFromNestedArray(object["coordinates"], 2)
		return err
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, g := range geometries {
			child, ok := g.(map[string]any)
			if !ok {
				return fmt.Errorf("geometry %d: expected a GeoJSON geometry object", i)
			}
			if err := validateGeometryShape(child); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
		}
		return nil
	case "feature", "featurecollection":
		return fmt.Errorf("GEOMETRY columns hold a Geometry, not a %s; extract .geometry first", typ)
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
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

// pointInMultiPolygon reports whether p is inside any one of mp's parts.
func pointInMultiPolygon(p geoPoint, mp geoMultiPolygon) bool {
	for _, poly := range mp.Polygons {
		if pointInPolygon(p, poly) {
			return true
		}
	}
	return false
}

// multiPolygonAreaMeters sums every part's area (a GeoJSON MultiPolygon's
// parts are not expected to overlap, so a plain sum is the standard,
// PostGIS-matching definition of its total area).
func multiPolygonAreaMeters(mp geoMultiPolygon) float64 {
	total := 0.0
	for _, poly := range mp.Polygons {
		total += polygonAreaMeters(poly)
	}
	return total
}

// ── Vertex collection and convex hull ────────────────────────────────────

// collectAllPositions extracts every coordinate position from any GeoJSON
// geometry (Point, MultiPoint, LineString, MultiLineString, Polygon,
// MultiPolygon). GEO_CONVEX_HULL is the only caller: it needs the vertex
// set, not which lines or rings connect them.
func collectAllPositions(v any) ([]geoPoint, error) {
	obj, err := geoObjectFromValue(v)
	if err != nil {
		return nil, err
	}
	typ, _ := obj["type"].(string)
	switch strings.ToLower(typ) {
	case "point":
		p, err := geoPositionFromValue(obj["coordinates"])
		if err != nil {
			return nil, err
		}
		return []geoPoint{p}, nil
	case "multipoint", "linestring":
		return positionsFromArray(obj["coordinates"])
	case "multilinestring", "polygon":
		return positionsFromNestedArray(obj["coordinates"], 1)
	case "multipolygon":
		return positionsFromNestedArray(obj["coordinates"], 2)
	default:
		return nil, fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func positionsFromArray(v any) ([]geoPoint, error) {
	raw, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("coordinates must be an array")
	}
	out := make([]geoPoint, 0, len(raw))
	for i, item := range raw {
		p, err := geoPositionFromValue(item)
		if err != nil {
			return nil, fmt.Errorf("position %d: %w", i, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// positionsFromNestedArray flattens `depth` levels of grouping (rings within
// a polygon; polygons, each with its own rings, within a multipolygon) down
// to one flat point list.
func positionsFromNestedArray(v any, depth int) ([]geoPoint, error) {
	raw, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("coordinates must be an array")
	}
	var out []geoPoint
	for i, item := range raw {
		var points []geoPoint
		var err error
		if depth == 1 {
			points, err = positionsFromArray(item)
		} else {
			points, err = positionsFromNestedArray(item, depth-1)
		}
		if err != nil {
			return nil, fmt.Errorf("group %d: %w", i, err)
		}
		out = append(out, points...)
	}
	return out, nil
}

// convexHull computes the convex hull of points via Andrew's monotone
// chain: sort by (lon, lat), then build the lower and upper hull chains,
// discarding a point whenever the last three make a non-left turn (cross
// product <= 0 also drops collinear points, keeping the hull's minimal
// vertex set). O(n log n) via the sort; the scan itself is linear.
func convexHull(points []geoPoint) ([]geoPoint, error) {
	uniq := dedupePoints(points)
	if len(uniq) < 3 {
		return nil, fmt.Errorf("need at least 3 distinct points for a hull, got %d", len(uniq))
	}
	sort.Slice(uniq, func(i, j int) bool {
		if uniq[i].Lon != uniq[j].Lon {
			return uniq[i].Lon < uniq[j].Lon
		}
		return uniq[i].Lat < uniq[j].Lat
	})

	cross := func(o, a, b geoPoint) float64 {
		return (a.Lon-o.Lon)*(b.Lat-o.Lat) - (a.Lat-o.Lat)*(b.Lon-o.Lon)
	}

	lower := make([]geoPoint, 0, len(uniq))
	for _, p := range uniq {
		for len(lower) >= 2 && cross(lower[len(lower)-2], lower[len(lower)-1], p) <= 0 {
			lower = lower[:len(lower)-1]
		}
		lower = append(lower, p)
	}
	upper := make([]geoPoint, 0, len(uniq))
	for i := len(uniq) - 1; i >= 0; i-- {
		p := uniq[i]
		for len(upper) >= 2 && cross(upper[len(upper)-2], upper[len(upper)-1], p) <= 0 {
			upper = upper[:len(upper)-1]
		}
		upper = append(upper, p)
	}

	hull := append(lower[:len(lower)-1], upper[:len(upper)-1]...)
	if len(hull) < 3 {
		return nil, fmt.Errorf("points are collinear; no polygon hull exists")
	}
	return hull, nil
}

func dedupePoints(points []geoPoint) []geoPoint {
	seen := make(map[[2]float64]bool, len(points))
	out := make([]geoPoint, 0, len(points))
	for _, p := range points {
		key := [2]float64{p.Lon, p.Lat}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, p)
	}
	return out
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
