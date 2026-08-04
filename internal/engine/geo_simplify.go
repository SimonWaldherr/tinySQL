package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// getGeoSimplifyFunctions returns mapshaper-inspired geometry simplification
// functions. Douglas-Peucker tolerance is measured in source coordinate units;
// Visvalingam tolerance is measured in squared source units. For geographic
// longitude/latitude data, callers should choose a tolerance appropriate to
// the dataset rather than assuming it represents meters.
func getGeoSimplifyFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_SIMPLIFY": evalGeoSimplify,
		"ST_SIMPLIFY":  evalGeoSimplify,
	}
}

func evalGeoSimplify(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 3); err != nil {
		return nil, err
	}

	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	tolerance, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(tolerance) || math.IsInf(tolerance, 0) || tolerance < 0 {
		return nil, fmt.Errorf("%s tolerance must be a finite non-negative number", ex.Name)
	}

	method := "dp"
	if len(ex.Args) == 3 {
		methodValue, err := evalExpr(env, ex.Args[2], row)
		if err != nil {
			return nil, err
		}
		if methodValue == nil {
			return nil, fmt.Errorf("%s method must not be NULL", ex.Name)
		}
		method = normalizeGeoSimplifyMethod(fmt.Sprint(methodValue))
	}
	if method != "dp" && method != "visvalingam-effective" && method != "visvalingam-weighted" {
		return nil, fmt.Errorf("%s: unknown simplification method %q (use dp, visvalingam-effective, or visvalingam-weighted)", ex.Name, method)
	}

	object, err := simplifyGeoJSONValue(value, tolerance, method)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	result, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("%s: encode simplified geometry: %w", ex.Name, err)
	}
	return string(result), nil
}

func normalizeGeoSimplifyMethod(method string) string {
	method = strings.ToLower(strings.TrimSpace(method))
	method = strings.ReplaceAll(method, "_", "-")
	method = strings.ReplaceAll(method, " ", "-")
	switch method {
	case "douglas-peucker", "douglaspeucker", "dp":
		return "dp"
	case "visvalingam", "visvalingam-effective", "effective-area", "effective":
		return "visvalingam-effective"
	case "visvalingam-weighted", "weighted-area", "weighted":
		return "visvalingam-weighted"
	default:
		return method
	}
}

// simplifyGeoJSONValue simplifies the geometry-bearing parts of a GeoJSON
// object while retaining feature properties, ids, and other top-level fields.
func simplifyGeoJSONValue(value any, tolerance float64, method string) (map[string]any, error) {
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, err
	}
	if err := simplifyGeoJSONObject(object, tolerance, method); err != nil {
		return nil, err
	}
	return object, nil
}

func geoSimplifyObject(value any) (map[string]any, error) {
	// The map[string]any case is deep-cloned directly instead of round-tripped
	// through json.Marshal+Unmarshal: callers throughout the geo package (geo
	// editing/quality/simplify/aggregate, spatial index building) pass in
	// values that may alias a table cell or an already-decoded parent object,
	// and every caller mutates the returned map in place — so this still must
	// return a fully independent clone, just built directly instead of paying
	// for a JSON text encode+decode round trip to get one.
	if x, ok := value.(map[string]any); ok {
		return geoDeepCloneObject(x), nil
	}

	var data []byte
	switch x := value.(type) {
	case json.RawMessage:
		data = x
	case []byte:
		data = x
	case string:
		data = []byte(strings.TrimSpace(x))
	default:
		return nil, fmt.Errorf("expected GeoJSON geometry, got %T", value)
	}

	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("decode GeoJSON: %w", err)
	}
	if object == nil {
		return nil, fmt.Errorf("GeoJSON value must be an object")
	}
	return object, nil
}

// geoDeepCloneObject deep-clones a map[string]any exactly as a
// json.Marshal+Unmarshal round trip through the same map would, without
// paying for the text encode/decode: every nested map/slice is rebuilt
// (never shared with the input), and any numeric type a JSON round trip
// would normalize to float64 (int/int32/int64/json.Number) is normalized the
// same way, so callers see identical value types whether the object reached
// them via a real JSON parse or via this clone.
func geoDeepCloneObject(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = geoDeepCloneValue(v)
	}
	return out
}

func geoDeepCloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return geoDeepCloneObject(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = geoDeepCloneValue(e)
		}
		return out
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return t
		}
		return f
	default:
		// string, float64, bool, nil, and anything else already matches what
		// a JSON round trip would produce, and is copied by value already.
		return v
	}
}

func simplifyGeoJSONObject(object map[string]any, tolerance float64, method string) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "feature":
		geometry := object["geometry"]
		if geometry == nil {
			return nil
		}
		geometryObject, err := geoSimplifyObject(geometry)
		if err != nil {
			return fmt.Errorf("feature geometry: %w", err)
		}
		if err := simplifyGeoJSONObject(geometryObject, tolerance, method); err != nil {
			return err
		}
		object["geometry"] = geometryObject
		return nil
	case "featurecollection":
		features, ok := object["features"].([]any)
		if !ok {
			return fmt.Errorf("FeatureCollection features must be an array")
		}
		for i, feature := range features {
			featureObject, err := geoSimplifyObject(feature)
			if err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			if err := simplifyGeoJSONObject(featureObject, tolerance, method); err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			features[i] = featureObject
		}
		object["features"] = features
		return nil
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, geometry := range geometries {
			geometryObject, err := geoSimplifyObject(geometry)
			if err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			if err := simplifyGeoJSONObject(geometryObject, tolerance, method); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			geometries[i] = geometryObject
		}
		object["geometries"] = geometries
		return nil
	case "point", "multipoint":
		return nil
	case "linestring":
		coordinates, err := simplifyGeoCoordinateSequence(object["coordinates"], false, tolerance, method)
		if err != nil {
			return err
		}
		object["coordinates"] = coordinates
		return nil
	case "multilinestring":
		lines, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return fmt.Errorf("MultiLineString coordinates: %w", err)
		}
		for i, line := range lines {
			simplified, err := simplifyGeoCoordinateSequence(line, false, tolerance, method)
			if err != nil {
				return fmt.Errorf("MultiLineString line %d: %w", i, err)
			}
			lines[i] = simplified
		}
		object["coordinates"] = lines
		return nil
	case "polygon":
		rings, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return fmt.Errorf("Polygon coordinates: %w", err)
		}
		for i, ring := range rings {
			simplified, err := simplifyGeoCoordinateSequence(ring, true, tolerance, method)
			if err != nil {
				return fmt.Errorf("Polygon ring %d: %w", i, err)
			}
			rings[i] = simplified
		}
		object["coordinates"] = rings
		return nil
	case "multipolygon":
		polygons, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return fmt.Errorf("MultiPolygon coordinates: %w", err)
		}
		for i, polygon := range polygons {
			rings, err := geoCoordinateGroups(polygon)
			if err != nil {
				return fmt.Errorf("MultiPolygon polygon %d: %w", i, err)
			}
			for j, ring := range rings {
				simplified, err := simplifyGeoCoordinateSequence(ring, true, tolerance, method)
				if err != nil {
					return fmt.Errorf("MultiPolygon polygon %d ring %d: %w", i, j, err)
				}
				rings[j] = simplified
			}
			polygons[i] = rings
		}
		object["coordinates"] = polygons
		return nil
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func geoCoordinateGroups(value any) ([]any, error) {
	groups, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected an array")
	}
	return groups, nil
}

type simplifyCoordinate []float64

func simplifyGeoCoordinateSequence(value any, closed bool, tolerance float64, method string) ([]any, error) {
	raw, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected an array of positions")
	}
	points := make([]simplifyCoordinate, len(raw))
	for i, position := range raw {
		parsed, err := simplifyGeoPosition(position)
		if err != nil {
			return nil, fmt.Errorf("position %d: %w", i, err)
		}
		points[i] = parsed
	}

	if closed {
		if len(points) < 4 {
			return nil, fmt.Errorf("closed ring must have at least 4 positions")
		}
		if sameSimplifyCoordinate(points[0], points[len(points)-1]) {
			points = points[:len(points)-1]
		}
		if len(points) < 3 {
			return nil, fmt.Errorf("closed ring must have at least 3 distinct positions")
		}
	}

	var simplified []simplifyCoordinate
	if tolerance == 0 || len(points) <= 2 {
		simplified = append([]simplifyCoordinate(nil), points...)
	} else {
		switch method {
		case "dp":
			simplified = simplifyDouglasPeucker(points, tolerance)
		case "visvalingam-effective":
			simplified = simplifyVisvalingam(points, tolerance, false)
		case "visvalingam-weighted":
			simplified = simplifyVisvalingam(points, tolerance, true)
		default:
			return nil, fmt.Errorf("unknown simplification method %q (use dp, visvalingam-effective, or visvalingam-weighted)", method)
		}
	}

	if closed {
		if len(simplified) < 3 {
			simplified = points
		}
		if !sameSimplifyCoordinate(simplified[0], simplified[len(simplified)-1]) {
			simplified = append(simplified, simplified[0])
		} else if len(simplified) == 3 {
			simplified = append(simplified, simplified[0])
		}
	}

	result := make([]any, len(simplified))
	for i, point := range simplified {
		position := make([]any, len(point))
		for j, value := range point {
			position[j] = value
		}
		result[i] = position
	}
	return result, nil
}

func simplifyGeoPosition(value any) (simplifyCoordinate, error) {
	positions, ok := value.([]any)
	if !ok || len(positions) < 2 {
		return nil, fmt.Errorf("position must contain at least longitude and latitude")
	}
	point := make(simplifyCoordinate, len(positions))
	for i, value := range positions {
		coordinate, err := geoFloat(value)
		if err != nil || math.IsNaN(coordinate) || math.IsInf(coordinate, 0) {
			if err == nil {
				err = fmt.Errorf("must be finite")
			}
			return nil, fmt.Errorf("coordinate %d: %w", i, err)
		}
		point[i] = coordinate
	}
	return point, nil
}

func sameSimplifyCoordinate(a, b simplifyCoordinate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func simplifyDouglasPeucker(points []simplifyCoordinate, tolerance float64) []simplifyCoordinate {
	if len(points) <= 2 {
		return append([]simplifyCoordinate(nil), points...)
	}
	keep := make([]bool, len(points))
	keep[0], keep[len(points)-1] = true, true
	limit := tolerance * tolerance
	var visit func(int, int)
	visit = func(start, end int) {
		if end <= start+1 {
			return
		}
		maxDistance := -1.0
		maxIndex := -1
		for i := start + 1; i < end; i++ {
			distance := simplifyPointSegmentDistanceSquared(points[i], points[start], points[end])
			if distance > maxDistance {
				maxDistance, maxIndex = distance, i
			}
		}
		if maxDistance > limit {
			keep[maxIndex] = true
			visit(start, maxIndex)
			visit(maxIndex, end)
		}
	}
	visit(0, len(points)-1)

	result := make([]simplifyCoordinate, 0, len(points))
	for i, point := range points {
		if keep[i] {
			result = append(result, point)
		}
	}
	return result
}

func simplifyPointSegmentDistanceSquared(point, start, end simplifyCoordinate) float64 {
	dx := end[0] - start[0]
	dy := end[1] - start[1]
	if dx == 0 && dy == 0 {
		dx, dy = point[0]-start[0], point[1]-start[1]
		return dx*dx + dy*dy
	}
	t := ((point[0]-start[0])*dx + (point[1]-start[1])*dy) / (dx*dx + dy*dy)
	if t < 0 {
		dx, dy = point[0]-start[0], point[1]-start[1]
	} else if t > 1 {
		dx, dy = point[0]-end[0], point[1]-end[1]
	} else {
		projectionX := start[0] + t*(end[0]-start[0])
		projectionY := start[1] + t*(end[1]-start[1])
		dx, dy = point[0]-projectionX, point[1]-projectionY
	}
	return dx*dx + dy*dy
}

// simplifyVisvalingam repeatedly removes the least-important interior
// vertex. Weighted-area simplification multiplies triangle area by the sine
// of the vertex angle, giving sharper corners a lower priority and producing
// smoother paths.
func simplifyVisvalingam(points []simplifyCoordinate, tolerance float64, weighted bool) []simplifyCoordinate {
	if len(points) <= 2 {
		return append([]simplifyCoordinate(nil), points...)
	}
	points = append([]simplifyCoordinate(nil), points...)
	for len(points) > 2 {
		minIndex := -1
		minArea := math.Inf(1)
		for i := 1; i < len(points)-1; i++ {
			area := simplifyTriangleArea(points[i-1], points[i], points[i+1])
			if weighted {
				area *= simplifyAngleSine(points[i-1], points[i], points[i+1])
			}
			if area < minArea {
				minArea, minIndex = area, i
			}
		}
		if minIndex < 0 || minArea > tolerance {
			break
		}
		points = append(points[:minIndex], points[minIndex+1:]...)
	}
	return points
}

func simplifyTriangleArea(a, b, c simplifyCoordinate) float64 {
	return math.Abs((a[0]*(b[1]-c[1]) + b[0]*(c[1]-a[1]) + c[0]*(a[1]-b[1])) / 2)
}

func simplifyAngleSine(a, b, c simplifyCoordinate) float64 {
	baX, baY := a[0]-b[0], a[1]-b[1]
	bcX, bcY := c[0]-b[0], c[1]-b[1]
	baLength := math.Hypot(baX, baY)
	bcLength := math.Hypot(bcX, bcY)
	if baLength == 0 || bcLength == 0 {
		return 0
	}
	return math.Abs(baX*bcY-baY*bcX) / (baLength * bcLength)
}
