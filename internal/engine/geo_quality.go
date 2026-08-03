package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// GEO_CLEAN removes repeated consecutive vertices and normalizes polygon ring
// closure. GEO_SNAP rounds coordinates to a source-coordinate grid and then
// runs the same cleanup. They deliberately reject geometries that collapse
// below GeoJSON's minimum vertex counts instead of returning invalid output.
func evalGeoClean(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil || value == nil {
		return value, err
	}
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if err := cleanGeoJSONObject(object); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	result, err := json.Marshal(object)
	return string(result), err
}

func evalGeoSnap(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil || value == nil {
		return value, err
	}
	grid, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	if grid <= 0 || math.IsNaN(grid) || math.IsInf(grid, 0) {
		return nil, fmt.Errorf("%s grid size must be a positive finite number", ex.Name)
	}
	object, err := geoSimplifyObject(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if err := transformGeoJSONObject(object, func(x, y float64) (float64, float64) {
		return math.Round(x/grid) * grid, math.Round(y/grid) * grid
	}); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if err := cleanGeoJSONObject(object); err != nil {
		return nil, fmt.Errorf("%s: snapped geometry is invalid: %w", ex.Name, err)
	}
	result, err := json.Marshal(object)
	return string(result), err
}

func evalGeoIsValid(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil || value == nil {
		return value != nil, err
	}
	object, err := geoSimplifyObject(value)
	if err != nil {
		return false, nil
	}
	return validateGeoJSONObject(object) == nil, nil
}

func cleanGeoJSONObject(object map[string]any) error {
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
		if err := cleanGeoJSONObject(child); err != nil {
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
			if err := cleanGeoJSONObject(child); err != nil {
				return err
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
			if err := cleanGeoJSONObject(child); err != nil {
				return err
			}
			geometries[i] = child
		}
		object["geometries"] = geometries
		return nil
	case "point":
		_, err := simplifyGeoPosition(object["coordinates"])
		return err
	case "multipoint", "linestring":
		coordinates, err := cleanGeoSequence(object["coordinates"], false)
		if err != nil {
			return err
		}
		if strings.EqualFold(typ, "linestring") && len(coordinates) < 2 {
			return fmt.Errorf("LineString needs at least 2 positions")
		}
		object["coordinates"] = coordinates
		return nil
	case "multilinestring":
		lines, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		for i := range lines {
			lines[i], err = cleanGeoSequence(lines[i], false)
			if err != nil {
				return err
			}
			if len(lines[i].([]any)) < 2 {
				return fmt.Errorf("line %d needs at least 2 positions", i)
			}
		}
		object["coordinates"] = lines
		return nil
	case "polygon":
		rings, err := geoCoordinateGroups(object["coordinates"])
		if err != nil {
			return err
		}
		if len(rings) == 0 {
			return fmt.Errorf("Polygon needs an exterior ring")
		}
		for i := range rings {
			rings[i], err = cleanGeoSequence(rings[i], true)
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
			child := map[string]any{"type": "Polygon", "coordinates": polygon}
			if err := cleanGeoJSONObject(child); err != nil {
				return fmt.Errorf("polygon %d: %w", i, err)
			}
			polygons[i] = child["coordinates"]
		}
		object["coordinates"] = polygons
		return nil
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func cleanGeoSequence(value any, closed bool) ([]any, error) {
	positions, err := geoEditPositions(value)
	if err != nil {
		return nil, err
	}
	result := make([]simplifyCoordinate, 0, len(positions))
	for _, position := range positions {
		if len(result) == 0 || !sameSimplifyCoordinate(result[len(result)-1], position) {
			result = append(result, position)
		}
	}
	if closed {
		if len(result) > 1 && sameSimplifyCoordinate(result[0], result[len(result)-1]) {
			result = result[:len(result)-1]
		}
		if len(result) < 3 {
			return nil, fmt.Errorf("closed ring needs at least 3 distinct positions")
		}
		result = append(result, result[0])
	}
	out := make([]any, len(result))
	for i, position := range result {
		out[i] = []float64(position)
	}
	return out, nil
}

func validateGeoJSONObject(object map[string]any) error {
	copy := make(map[string]any, len(object))
	for key, value := range object {
		copy[key] = value
	}
	return cleanGeoJSONObject(copy)
}
