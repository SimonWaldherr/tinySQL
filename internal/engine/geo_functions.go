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
		"GEO_POINT":       evalGeoPoint,
		"ST_MAKEPOINT":    evalGeoPoint,
		"ST_POINT":        evalGeoPoint,
		"GEO_LON":         evalGeoLon,
		"GEO_X":           evalGeoLon,
		"ST_X":            evalGeoLon,
		"GEO_LAT":         evalGeoLat,
		"GEO_Y":           evalGeoLat,
		"ST_Y":            evalGeoLat,
		"GEO_DISTANCE":    evalGeoDistance,
		"HAVERSINE":       evalGeoDistance,
		"ST_DISTANCE":     evalGeoDistance,
		"GEO_DWITHIN":     evalGeoDWithin,
		"ST_DWITHIN":      evalGeoDWithin,
		"GEO_WITHIN_BBOX": evalGeoWithinBBox,
		"ST_WITHIN_BBOX":  evalGeoWithinBBox,
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
	coords := []float64{lon, lat}
	if len(ex.Args) == 3 {
		z, err := evalGeoFloatArg(env, ex, row, 2)
		if err != nil {
			return nil, err
		}
		coords = append(coords, z)
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

func evalGeoDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	switch len(ex.Args) {
	case 2:
		a, err := evalGeoPointArg(env, ex, row, 0)
		if err != nil {
			return nil, err
		}
		b, err := evalGeoPointArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
		return haversineMeters(a.Lat, a.Lon, b.Lat, b.Lon), nil
	case 4:
		lat1, err := evalGeoFloatArg(env, ex, row, 0)
		if err != nil {
			return nil, err
		}
		lon1, err := evalGeoFloatArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
		lat2, err := evalGeoFloatArg(env, ex, row, 2)
		if err != nil {
			return nil, err
		}
		lon2, err := evalGeoFloatArg(env, ex, row, 3)
		if err != nil {
			return nil, err
		}
		return haversineMeters(lat1, lon1, lat2, lon2), nil
	default:
		return nil, fmt.Errorf("%s expects 2 GeoJSON points or 4 coordinates: (lat1, lon1, lat2, lon2)", ex.Name)
	}
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
