// ST_TRANSFORM reprojects a geometry between WGS84 (SRID 4326, the lon/lat
// degrees every other geo function in this engine assumes) and Web Mercator
// (SRID 3857, the projection every XYZ/Slippy-map tile server -- and this
// engine's own TILE_* functions, see tile_functions.go -- uses). These two
// are supported because they are the pair that actually shows up in
// practice for a tile-serving engine: a table of WGS84 geometry needs to
// become Mercator meters to compute a tile bounding box or measure a
// screen-space quantity, and a value pulled from a Mercator-native source
// (e.g. an MVT tile, or a system that stores 3857 natively) needs to become
// WGS84 to interoperate with everything else here.
//
// A general reprojection engine (arbitrary EPSG codes, datum shifts, PROJ
// pipelines) is out of scope: that needs a large parameter database this
// project has no appetite for vendoring (see the project's own
// minimal-dependencies stance), and every other geo function already
// assumes -- and only makes sense for -- WGS84 degrees. ST_TRANSFORM's job
// here is narrowly to get a geometry to or from the one other SRID this
// codebase's tile support cares about, not to become a general GIS
// reprojection layer.
package engine

import (
	"encoding/json"
	"fmt"
	"math"
)

func getGeoTransformFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"ST_TRANSFORM":  evalGeoTransform,
		"GEO_TRANSFORM": evalGeoTransform,
	}
}

// geoWebMercatorSRID is the only other SRID ST_TRANSFORM understands.
const geoWebMercatorSRID = 3857

// geoWebMercatorMaxLat is the latitude at which Web Mercator's Y coordinate
// diverges to infinity; the standard projection simply clips to this bound
// (the same +/-85.05112878 degrees every Slippy-map tile scheme clips to)
// rather than erroring, since real-world data that grazes the pole is
// common (e.g. a bounding box literally at +90) and clipping is what every
// other implementation of this projection does too.
const geoWebMercatorMaxLat = 85.05112877980659

func lonLatToWebMercator(lon, lat float64) (x, y float64) {
	if lat > geoWebMercatorMaxLat {
		lat = geoWebMercatorMaxLat
	}
	if lat < -geoWebMercatorMaxLat {
		lat = -geoWebMercatorMaxLat
	}
	const r = geoEarthRadiusMeters
	x = lon * math.Pi / 180 * r
	y = math.Log(math.Tan(math.Pi/4+lat*math.Pi/360)) * r
	return x, y
}

func webMercatorToLonLat(x, y float64) (lon, lat float64) {
	const r = geoEarthRadiusMeters
	lon = x / r * 180 / math.Pi
	lat = (2*math.Atan(math.Exp(y/r)) - math.Pi/2) * 180 / math.Pi
	return lon, lat
}

func evalGeoTransform(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	obj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	targetF, err := evalGeoFloatArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	if targetF != math.Trunc(targetF) {
		return nil, fmt.Errorf("%s: target SRID must be an integer", ex.Name)
	}
	target := int64(targetF)

	var project func(lon, lat float64) (float64, float64)
	switch target {
	case geoWebMercatorSRID:
		project = lonLatToWebMercator
	case geoWGS84SRID, 0:
		project = webMercatorToLonLat
	default:
		return nil, fmt.Errorf("%s: unsupported target SRID %d; only 4326 and 3857 are supported", ex.Name, target)
	}

	out := geoDeepCloneObject(obj)
	if err := transformGeoJSONPositionsInPlace(out, project); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}

// transformGeoJSONPositionsInPlace rewrites every position's x/y ordinates
// via project, keeping any Z ordinate untouched (Web Mercator's Z axis is
// the same meters-if-projected-consistently convention many tools use, but
// since neither direction here changes vertical datum, passing Z through
// unmodified is the only defensible choice).
func transformGeoJSONPositionsInPlace(object map[string]any, project func(x, y float64) (float64, float64)) error {
	return walkGeoJSONPositions(object, func(pos []any) error {
		if len(pos) < 2 {
			return fmt.Errorf("position must have at least 2 ordinates")
		}
		x, err := geoFloat(pos[0])
		if err != nil {
			return err
		}
		y, err := geoFloat(pos[1])
		if err != nil {
			return err
		}
		nx, ny := project(x, y)
		pos[0], pos[1] = nx, ny
		return nil
	})
}
