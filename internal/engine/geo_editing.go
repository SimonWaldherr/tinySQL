package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// getGeoEditingFunctions contains geometry inspection and editing operations
// that correspond to common mapshaper workflows while remaining natural as
// scalar SQL functions.
func getGeoEditingFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_BBOX":        evalGeoBBox,
		"ST_BBOX":         evalGeoBBox,
		"GEO_CENTROID":    evalGeoCentroid,
		"ST_CENTROID":     evalGeoCentroid,
		"GEO_AFFINE":      evalGeoAffine,
		"ST_AFFINE":       evalGeoAffine,
		"GEO_SMOOTH":      evalGeoSmooth,
		"ST_SMOOTH":       evalGeoSmooth,
		"GEO_DROP_HOLES":  evalGeoDropHoles,
		"ST_REMOVE_HOLES": evalGeoDropHoles,
		"GEO_CLEAN":       evalGeoClean,
		"ST_CLEAN":        evalGeoClean,
		"GEO_SNAP":        evalGeoSnap,
		"ST_SNAPTOGRID":   evalGeoSnap,
		"GEO_IS_VALID":    evalGeoIsValid,
		"ST_ISVALID":      evalGeoIsValid,
	}
}

type geoEditPoint struct {
	X float64
	Y float64
}

type geoEditBBox struct {
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
	Set  bool
}

func (b *geoEditBBox) add(p geoEditPoint) {
	if !b.Set {
		b.MinX, b.MinY, b.MaxX, b.MaxY, b.Set = p.X, p.Y, p.X, p.Y, true
		return
	}
	b.MinX = math.Min(b.MinX, p.X)
	b.MinY = math.Min(b.MinY, p.Y)
	b.MaxX = math.Max(b.MaxX, p.X)
	b.MaxY = math.Max(b.MaxY, p.Y)
}

func evalGeoBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
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
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	bbox := geoEditBBox{}
	if err := collectGeoBBox(object, &bbox); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if !bbox.Set {
		return nil, fmt.Errorf("%s: geometry has no coordinates", ex.Name)
	}
	result, err := json.Marshal([]float64{bbox.MinX, bbox.MinY, bbox.MaxX, bbox.MaxY})
	if err != nil {
		return nil, fmt.Errorf("%s: encode bounding box: %w", ex.Name, err)
	}
	return string(result), nil
}

func collectGeoBBox(object map[string]any, bbox *geoEditBBox) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "feature":
		geometry := object["geometry"]
		if geometry == nil {
			return nil
		}
		child, err := geoSimplifyObject(geometry)
		if err != nil {
			return err
		}
		return collectGeoBBox(child, bbox)
	case "featurecollection":
		features, ok := object["features"].([]any)
		if !ok {
			return fmt.Errorf("FeatureCollection features must be an array")
		}
		for i, feature := range features {
			child, err := geoSimplifyObject(feature)
			if err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			if err := collectGeoBBox(child, bbox); err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
		}
		return nil
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, geometry := range geometries {
			child, err := geoSimplifyObject(geometry)
			if err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			if err := collectGeoBBox(child, bbox); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
		}
		return nil
	case "point", "multipoint", "linestring", "multilinestring", "polygon", "multipolygon":
		return walkGeoCoordinateValue(object["coordinates"], func(position simplifyCoordinate) {
			bbox.add(geoEditPoint{X: position[0], Y: position[1]})
		})
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func walkGeoCoordinateValue(value any, visit func(simplifyCoordinate)) error {
	positions, ok := value.([]any)
	if !ok {
		return fmt.Errorf("coordinates must be an array")
	}
	if len(positions) >= 2 {
		if position, err := simplifyGeoPosition(positions); err == nil {
			visit(position)
			return nil
		}
	}
	for i, child := range positions {
		if err := walkGeoCoordinateValue(child, visit); err != nil {
			return fmt.Errorf("coordinate group %d: %w", i, err)
		}
	}
	return nil
}

type geoCentroidAccumulator struct {
	X        float64
	Y        float64
	Weight   float64
	Fallback []geoEditPoint
}

func (a *geoCentroidAccumulator) add(point geoEditPoint, weight float64) {
	if weight > 0 && !math.IsNaN(weight) && !math.IsInf(weight, 0) {
		a.X += point.X * weight
		a.Y += point.Y * weight
		a.Weight += weight
	}
	a.Fallback = append(a.Fallback, point)
}

func evalGeoCentroid(env ExecEnv, ex *FuncCall, row Row) (any, error) {
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
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	acc := geoCentroidAccumulator{}
	if err := collectGeoCentroid(object, &acc); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if acc.Weight == 0 {
		if len(acc.Fallback) == 0 {
			return nil, fmt.Errorf("%s: geometry has no coordinates", ex.Name)
		}
		for _, point := range acc.Fallback {
			acc.X += point.X
			acc.Y += point.Y
		}
		acc.Weight = float64(len(acc.Fallback))
	}
	return geoPointJSON(acc.X/acc.Weight, acc.Y/acc.Weight, nil)
}

func collectGeoCentroid(object map[string]any, acc *geoCentroidAccumulator) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "feature":
		if object["geometry"] == nil {
			return nil
		}
		child, err := geoSimplifyObject(object["geometry"])
		if err != nil {
			return err
		}
		return collectGeoCentroid(child, acc)
	case "featurecollection":
		features, ok := object["features"].([]any)
		if !ok {
			return fmt.Errorf("FeatureCollection features must be an array")
		}
		for i, feature := range features {
			child, err := geoSimplifyObject(feature)
			if err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			if err := collectGeoCentroid(child, acc); err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
		}
		return nil
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, geometry := range geometries {
			child, err := geoSimplifyObject(geometry)
			if err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			if err := collectGeoCentroid(child, acc); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
		}
		return nil
	case "point":
		return addGeoCentroidPositions(object["coordinates"], acc, 1)
	case "multipoint":
		return addGeoCentroidPositions(object["coordinates"], acc, 1)
	case "linestring":
		return addGeoCentroidLine(object["coordinates"], acc, false)
	case "multilinestring":
		lines, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for _, line := range lines {
			if err := addGeoCentroidLine(line, acc, false); err != nil {
				return err
			}
		}
		return nil
	case "polygon":
		return addGeoCentroidPolygon(object["coordinates"], acc)
	case "multipolygon":
		polygons, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for _, polygon := range polygons {
			if err := addGeoCentroidPolygon(polygon, acc); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func addGeoCentroidPositions(value any, acc *geoCentroidAccumulator, weight float64) error {
	return walkGeoCoordinateValue(value, func(position simplifyCoordinate) {
		acc.add(geoEditPoint{X: position[0], Y: position[1]}, weight)
	})
}

func addGeoCentroidLine(value any, acc *geoCentroidAccumulator, closed bool) error {
	positions, err := geoEditPositions(value)
	if err != nil {
		return err
	}
	if closed && len(positions) > 1 && sameSimplifyCoordinate(positions[0], positions[len(positions)-1]) {
		positions = positions[:len(positions)-1]
	}
	for i := 1; i < len(positions); i++ {
		a, b := positions[i-1], positions[i]
		length := math.Hypot(b[0]-a[0], b[1]-a[1])
		acc.add(geoEditPoint{X: (a[0] + b[0]) / 2, Y: (a[1] + b[1]) / 2}, length)
	}
	for _, position := range positions {
		acc.Fallback = append(acc.Fallback, geoEditPoint{X: position[0], Y: position[1]})
	}
	return nil
}

func addGeoCentroidPolygon(value any, acc *geoCentroidAccumulator) error {
	rings, err := geoCoordinateGroups(value)
	if err != nil {
		return err
	}
	if len(rings) == 0 {
		return nil
	}
	outer, err := geoEditPositions(rings[0])
	if err != nil {
		return err
	}
	outerArea, outerCentroid := geoEditRingCentroid(outer)
	for _, position := range outer {
		acc.Fallback = append(acc.Fallback, geoEditPoint{X: position[0], Y: position[1]})
	}
	netArea := outerArea
	weightedX := outerCentroid.X * outerArea
	weightedY := outerCentroid.Y * outerArea
	for _, rawHole := range rings[1:] {
		hole, err := geoEditPositions(rawHole)
		if err != nil {
			return err
		}
		area, centroid := geoEditRingCentroid(hole)
		netArea -= area
		weightedX -= centroid.X * area
		weightedY -= centroid.Y * area
		for _, position := range hole {
			acc.Fallback = append(acc.Fallback, geoEditPoint{X: position[0], Y: position[1]})
		}
	}
	if netArea > 1e-15 {
		acc.X += weightedX
		acc.Y += weightedY
		acc.Weight += netArea
	}
	return nil
}

func geoEditPositions(value any) ([]simplifyCoordinate, error) {
	raw, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("coordinates must be an array")
	}
	positions := make([]simplifyCoordinate, len(raw))
	for i, item := range raw {
		position, err := simplifyGeoPosition(item)
		if err != nil {
			return nil, fmt.Errorf("position %d: %w", i, err)
		}
		positions[i] = position
	}
	return positions, nil
}

func geoEditRingCentroid(ring []simplifyCoordinate) (float64, geoEditPoint) {
	if len(ring) < 3 {
		return 0, geoEditPoint{}
	}
	areaTwice := 0.0
	cx, cy := 0.0, 0.0
	for i := 0; i < len(ring); i++ {
		a, b := ring[i], ring[(i+1)%len(ring)]
		cross := a[0]*b[1] - b[0]*a[1]
		areaTwice += cross
		cx += (a[0] + b[0]) * cross
		cy += (a[1] + b[1]) * cross
	}
	if math.Abs(areaTwice) < 1e-15 {
		return 0, geoEditPoint{}
	}
	area := math.Abs(areaTwice / 2)
	return area, geoEditPoint{X: cx / (3 * areaTwice), Y: cy / (3 * areaTwice)}
}

type geoAffineTransform func(x, y float64) (float64, float64)

func evalGeoAffine(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 5 && len(ex.Args) != 7 {
		return nil, fmt.Errorf("%s requires (geometry, shift_x, shift_y, scale, rotate_deg[, anchor_x, anchor_y])", ex.Name)
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	args := make([]float64, len(ex.Args)-1)
	for i := range args {
		args[i], err = evalGeoFloatArg(env, ex, row, i+1)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(args[i]) || math.IsInf(args[i], 0) {
			return nil, fmt.Errorf("%s argument %d must be finite", ex.Name, i+2)
		}
	}
	shiftX, shiftY, scale, rotate := args[0], args[1], args[2], args[3]
	if scale == 0 {
		return nil, fmt.Errorf("%s scale must not be zero", ex.Name)
	}
	anchorX, anchorY := 0.0, 0.0
	if len(args) == 4 {
		object, err := geoSimplifyObject(value)
		if err != nil {
			return nil, err
		}
		bbox := geoEditBBox{}
		if err := collectGeoBBox(object, &bbox); err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		if bbox.Set {
			anchorX, anchorY = (bbox.MinX+bbox.MaxX)/2, (bbox.MinY+bbox.MaxY)/2
		}
	} else {
		anchorX, anchorY = args[4], args[5]
	}
	rad := rotate * math.Pi / 180
	cos, sin := math.Cos(rad)*scale, math.Sin(rad)*scale
	transform := func(x, y float64) (float64, float64) {
		dx, dy := x-anchorX, y-anchorY
		return anchorX + dx*cos - dy*sin + shiftX, anchorY + dx*sin + dy*cos + shiftY
	}
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, err
	}
	if err := transformGeoJSONObject(object, transform); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	result, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(result), nil
}

func transformGeoJSONObject(object map[string]any, transform geoAffineTransform) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "feature":
		if object["geometry"] == nil {
			return nil
		}
		child, err := geoSimplifyObject(object["geometry"])
		if err != nil {
			return err
		}
		if err := transformGeoJSONObject(child, transform); err != nil {
			return err
		}
		object["geometry"] = child
		return nil
	case "featurecollection":
		features, ok := object["features"].([]any)
		if !ok {
			return fmt.Errorf("FeatureCollection features must be an array")
		}
		for i, feature := range features {
			child, err := geoSimplifyObject(feature)
			if err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			if err := transformGeoJSONObject(child, transform); err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			features[i] = child
		}
		object["features"] = features
		return nil
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, geometry := range geometries {
			child, err := geoSimplifyObject(geometry)
			if err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			if err := transformGeoJSONObject(child, transform); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			geometries[i] = child
		}
		object["geometries"] = geometries
		return nil
	case "point", "multipoint", "linestring", "multilinestring", "polygon", "multipolygon":
		coordinates, err := transformGeoCoordinateValue(object["coordinates"], transform)
		if err != nil {
			return err
		}
		object["coordinates"] = coordinates
		return nil
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func transformGeoCoordinateValue(value any, transform geoAffineTransform) (any, error) {
	positions, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("coordinates must be an array")
	}
	if len(positions) >= 2 {
		if position, err := simplifyGeoPosition(positions); err == nil {
			x, y := transform(position[0], position[1])
			position[0], position[1] = x, y
			result := make([]any, len(position))
			for i, value := range position {
				result[i] = value
			}
			return result, nil
		}
	}
	result := make([]any, len(positions))
	for i, child := range positions {
		transformed, err := transformGeoCoordinateValue(child, transform)
		if err != nil {
			return nil, fmt.Errorf("coordinate group %d: %w", i, err)
		}
		result[i] = transformed
	}
	return result, nil
}

func evalGeoSmooth(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	iterations, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	if iterations < 0 || iterations > 8 || iterations != math.Trunc(iterations) {
		return nil, fmt.Errorf("%s iterations must be an integer from 0 to 8", ex.Name)
	}
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(iterations); i++ {
		if err := smoothGeoJSONObject(object); err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
	}
	result, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(result), nil
}

func smoothGeoJSONObject(object map[string]any) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "feature":
		if object["geometry"] == nil {
			return nil
		}
		child, err := geoSimplifyObject(object["geometry"])
		if err != nil {
			return err
		}
		if err := smoothGeoJSONObject(child); err != nil {
			return err
		}
		object["geometry"] = child
		return nil
	case "featurecollection":
		features, ok := object["features"].([]any)
		if !ok {
			return fmt.Errorf("FeatureCollection features must be an array")
		}
		for i, feature := range features {
			child, err := geoSimplifyObject(feature)
			if err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			if err := smoothGeoJSONObject(child); err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			features[i] = child
		}
		object["features"] = features
		return nil
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, geometry := range geometries {
			child, err := geoSimplifyObject(geometry)
			if err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			if err := smoothGeoJSONObject(child); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			geometries[i] = child
		}
		object["geometries"] = geometries
		return nil
	case "point", "multipoint":
		return nil
	case "linestring":
		coordinates, err := smoothGeoCoordinateSequence(object["coordinates"], false)
		if err != nil {
			return err
		}
		object["coordinates"] = coordinates
		return nil
	case "multilinestring":
		lines, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for i, line := range lines {
			lines[i], err = smoothGeoCoordinateSequence(line, false)
			if err != nil {
				return fmt.Errorf("line %d: %w", i, err)
			}
		}
		object["coordinates"] = lines
		return nil
	case "polygon":
		rings, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for i, ring := range rings {
			rings[i], err = smoothGeoCoordinateSequence(ring, true)
			if err != nil {
				return fmt.Errorf("ring %d: %w", i, err)
			}
		}
		object["coordinates"] = rings
		return nil
	case "multipolygon":
		polygons, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for i, polygon := range polygons {
			rings, err := geoCoordinateGroups(polygon)
			if err != nil {
				return fmt.Errorf("polygon %d: %w", i, err)
			}
			for j, ring := range rings {
				rings[j], err = smoothGeoCoordinateSequence(ring, true)
				if err != nil {
					return fmt.Errorf("polygon %d ring %d: %w", i, j, err)
				}
			}
			polygons[i] = rings
		}
		object["coordinates"] = polygons
		return nil
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func smoothGeoCoordinateSequence(value any, closed bool) ([]any, error) {
	positions, err := geoEditPositions(value)
	if err != nil {
		return nil, err
	}
	if closed {
		if len(positions) < 4 {
			return nil, fmt.Errorf("closed ring must have at least 4 positions")
		}
		if sameSimplifyCoordinate(positions[0], positions[len(positions)-1]) {
			positions = positions[:len(positions)-1]
		}
	}
	if len(positions) < 2 {
		return nil, fmt.Errorf("line must have at least 2 positions")
	}
	result := make([]simplifyCoordinate, 0, len(positions)*2)
	if closed {
		for i, point := range positions {
			next := positions[(i+1)%len(positions)]
			result = append(result, geoInterpolatePosition(point, next, 0.25), geoInterpolatePosition(point, next, 0.75))
		}
		result = append(result, result[0])
	} else {
		result = append(result, positions[0])
		for i := 0; i < len(positions)-1; i++ {
			result = append(result, geoInterpolatePosition(positions[i], positions[i+1], 0.25), geoInterpolatePosition(positions[i], positions[i+1], 0.75))
		}
		result = append(result, positions[len(positions)-1])
	}
	output := make([]any, len(result))
	for i, point := range result {
		position := make([]any, len(point))
		for j, value := range point {
			position[j] = value
		}
		output[i] = position
	}
	return output, nil
}

func geoInterpolatePosition(a, b simplifyCoordinate, t float64) simplifyCoordinate {
	point := make(simplifyCoordinate, len(a))
	for i := range a {
		point[i] = a[i] + (b[i]-a[i])*t
	}
	return point
}

func evalGeoDropHoles(env ExecEnv, ex *FuncCall, row Row) (any, error) {
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
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, err
	}
	if err := dropGeoHoles(object); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	result, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(result), nil
}

func dropGeoHoles(object map[string]any) error {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "feature":
		if object["geometry"] == nil {
			return nil
		}
		child, err := geoSimplifyObject(object["geometry"])
		if err != nil {
			return err
		}
		if err := dropGeoHoles(child); err != nil {
			return err
		}
		object["geometry"] = child
		return nil
	case "featurecollection":
		features, ok := object["features"].([]any)
		if !ok {
			return fmt.Errorf("FeatureCollection features must be an array")
		}
		for i, feature := range features {
			child, err := geoSimplifyObject(feature)
			if err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			if err := dropGeoHoles(child); err != nil {
				return fmt.Errorf("feature %d: %w", i, err)
			}
			features[i] = child
		}
		object["features"] = features
		return nil
	case "geometrycollection":
		geometries, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, geometry := range geometries {
			child, err := geoSimplifyObject(geometry)
			if err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			if err := dropGeoHoles(child); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
			geometries[i] = child
		}
		object["geometries"] = geometries
		return nil
	case "polygon":
		rings, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		if len(rings) > 1 {
			object["coordinates"] = []any{rings[0]}
		}
		return nil
	case "multipolygon":
		polygons, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for i, polygon := range polygons {
			rings, err := geoCoordinateGroups(polygon)
			if err != nil {
				return fmt.Errorf("polygon %d: %w", i, err)
			}
			if len(rings) > 1 {
				polygons[i] = []any{rings[0]}
			}
		}
		object["coordinates"] = polygons
		return nil
	case "point", "multipoint", "linestring", "multilinestring":
		return nil
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}
