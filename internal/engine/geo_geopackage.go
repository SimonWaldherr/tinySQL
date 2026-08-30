// OGC GeoPackageBinary scalar helpers. These functions operate on a BLOB and
// therefore work independently of where it came from: a GeoPackage file, an
// HTTP API, a message queue, or an ordinary tinySQL column.
package engine

import (
	"encoding/json"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/gpkg"
)

func getGeoPackageFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GPKG_SRID":       evalGPKGSRID,
		"GPKG_IS_EMPTY":   evalGPKGIsEmpty,
		"GPKG_BBOX":       evalGPKGBBox,
		"GPKG_HEADER":     evalGPKGHeader,
		"GPKG_AS_WKB":     evalGPKGAsWKB,
		"GEO_FROM_GPKG":   evalGeoFromGPKG,
		"ST_GEOMFROMGPKG": evalGeoFromGPKG,
	}
}

func evalGPKGGeometryArg(env ExecEnv, ex *FuncCall, row Row) (gpkg.Geometry, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return gpkg.Geometry{}, err
	}
	body, err := evalGeoBlobArg(env, ex, row, 0)
	if err != nil {
		return gpkg.Geometry{}, err
	}
	geometry, err := gpkg.ParseGeometry(body)
	if err != nil {
		return gpkg.Geometry{}, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return geometry, nil
}

func evalGPKGSRID(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	geometry, err := evalGPKGGeometryArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	return int64(geometry.SRID), nil
}

func evalGPKGIsEmpty(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	geometry, err := evalGPKGGeometryArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	return geometry.Empty, nil
}

func evalGPKGBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	geometry, err := evalGPKGGeometryArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	if len(geometry.BBox) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(geometry.BBox)
	if err != nil {
		return nil, fmt.Errorf("%s: encode bbox: %w", ex.Name, err)
	}
	return string(body), nil
}

func evalGPKGHeader(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	geometry, err := evalGPKGGeometryArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"version":     geometry.Version,
		"srid":        geometry.SRID,
		"empty":       geometry.Empty,
		"extended":    geometry.Extended,
		"header_size": geometry.HeaderLen,
	}
	if len(geometry.BBox) > 0 {
		out["bbox"] = geometry.BBox
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("%s: encode header: %w", ex.Name, err)
	}
	return string(body), nil
}

func evalGPKGAsWKB(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	geometry, err := evalGPKGGeometryArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), geometry.WKB...), nil
}

func evalGeoFromGPKG(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	geometry, err := evalGPKGGeometryArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	if err := geoRequireWGS84SRID(ex.Name, int64(geometry.SRID)); err != nil {
		return nil, err
	}
	if geometry.Empty {
		return nil, nil
	}
	object, err := DecodeWKBGeometry(geometry.WKB)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	body, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}
