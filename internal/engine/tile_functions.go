// Web Mercator tile addressing, for working with MBTiles tilesets in SQL.
//
// Two coordinate conventions meet in a tileset, and confusing them is the
// classic MBTiles bug:
//
//   - XYZ ("slippy map", what web clients and /{z}/{x}/{y}.png URLs use) counts
//     rows from the *top*: row 0 is the northernmost.
//   - TMS (what the MBTiles specification stores in tiles.tile_row) counts rows
//     from the *bottom*: row 0 is the southernmost.
//
// They differ by tile_row = 2^zoom - 1 - y. A tileset served with the flip
// missing looks plausible — right tiles, wrong places, mirrored vertically —
// which is why every function here states which convention it speaks, and why
// TILE_FLIP_Y exists as the one explicit conversion.
//
// Functions taking or returning a y value use XYZ, because that is what callers
// have when serving a map request. Only TILE_FLIP_Y and the MBTiles helpers
// mention TMS.
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// tileMaxZoom bounds zoom so 2^zoom stays exactly representable and tile
// indices stay well inside int range. Zoom 30 is already a ~3cm/pixel scale.
const tileMaxZoom = 30

// tileMaxLat is the Web Mercator latitude limit (85.0511287798...). The
// projection diverges at the poles, so latitudes are clamped rather than
// producing an out-of-range tile.
const tileMaxLat = 85.05112877980659

func getTileFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"TILE_X":                 evalTileX,
		"TILE_Y":                 evalTileY,
		"TILE_ZXY":               evalTileZXY,
		"TILE_FLIP_Y":            evalTileFlipY,
		"TILE_ROW_TMS":           evalTileFlipY, // alias: XYZ y -> MBTiles tile_row
		"TILE_LON":               evalTileLon,
		"TILE_LAT":               evalTileLat,
		"TILE_BBOX":              evalTileBBox,
		"TILE_BBOX_3857":         evalTileBBox3857,
		"TILE_RESOLUTION":        evalTileResolution,
		"WMTS_RESOLUTION":        evalTileResolution,
		"WMTS_SCALE_DENOMINATOR": evalWMTSScaleDenominator,
		"TILE_SCALE_DENOMINATOR": evalWMTSScaleDenominator,
		"WMS_BBOX":               evalWMSBBox,
		"TILE_MATRIX_BBOX":       evalTileMatrixBBox,
		"WMTS_TILE_BBOX":         evalTileMatrixBBox,
		"TILE_MATRIX_POSITION":   evalTileMatrixPosition,
		"TILE_QUADKEY":           evalTileQuadkey,
		"TILE_FROM_QUADKEY":      evalTileFromQuadkey,
		"TILE_PARENT":            evalTileParent,
		"TILE_COUNT":             evalTileCount,
		"TILE_CONTAINS":          evalTileContains,
	}
}

const (
	// WMTS WebMercatorQuad uses the EPSG:3857 sphere radius and the OGC
	// standard pixel size of 0.28 mm for ScaleDenominator.
	tileWebMercatorRadiusMeters = 6378137.0
	wmtsStandardPixelMeters     = 0.00028
	tileDefaultPixelSize        = 256
)

func tilePixelSizeArg(env ExecEnv, ex *FuncCall, row Row, idx int) (int, error) {
	value, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return 0, err
	}
	size, err := toInt(value)
	if err != nil || size <= 0 || size > 16384 {
		if err == nil {
			err = fmt.Errorf("must be in range 1..16384")
		}
		return 0, fmt.Errorf("%s tile_size: %w", ex.Name, err)
	}
	return size, nil
}

// tileZoomArg reads a zoom level and validates its range.
func tileZoomArg(env ExecEnv, ex *FuncCall, row Row, idx int) (int, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return 0, err
	}
	z, err := toInt(v)
	if err != nil {
		return 0, fmt.Errorf("%s zoom: %w", ex.Name, err)
	}
	if z < 0 || z > tileMaxZoom {
		return 0, fmt.Errorf("%s: zoom %d out of range 0..%d", ex.Name, z, tileMaxZoom)
	}
	return z, nil
}

// tileIndexArg reads a tile column or row index and validates it against zoom.
func tileIndexArg(env ExecEnv, ex *FuncCall, row Row, idx, zoom int, what string) (int, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return 0, err
	}
	n, err := toInt(v)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", ex.Name, what, err)
	}
	limit := 1 << uint(zoom)
	if n < 0 || n >= limit {
		return 0, fmt.Errorf("%s: %s %d out of range 0..%d at zoom %d", ex.Name, what, n, limit-1, zoom)
	}
	return n, nil
}

// tileClampLat clamps a latitude to the Web Mercator valid range.
func tileClampLat(lat float64) float64 {
	if lat > tileMaxLat {
		return tileMaxLat
	}
	if lat < -tileMaxLat {
		return -tileMaxLat
	}
	return lat
}

// tileColumnFor returns the tile column containing lon at zoom.
func tileColumnFor(lon float64, zoom int) int {
	n := math.Ldexp(1, zoom) // 2^zoom
	// Longitude wraps, so normalize into [-180, 180) rather than clamping: a
	// dateline-crossing bounding box otherwise collapses onto the last column.
	lon = math.Mod(lon+180, 360)
	if lon < 0 {
		lon += 360
	}
	x := int(math.Floor(lon / 360 * n))
	if x >= int(n) {
		x = int(n) - 1
	}
	return x
}

// tileRowFor returns the XYZ tile row containing lat at zoom.
func tileRowFor(lat float64, zoom int) int {
	n := math.Ldexp(1, zoom)
	rad := tileClampLat(lat) * math.Pi / 180
	// The standard Web Mercator inverse: y grows southward from 0.
	frac := (1 - math.Log(math.Tan(rad)+1/math.Cos(rad))/math.Pi) / 2
	y := int(math.Floor(frac * n))
	if y < 0 {
		y = 0
	}
	if y >= int(n) {
		y = int(n) - 1
	}
	return y
}

// tileWestLon returns the longitude of a tile column's western edge.
func tileWestLon(x, zoom int) float64 {
	return float64(x)/math.Ldexp(1, zoom)*360 - 180
}

// tileNorthLat returns the latitude of an XYZ tile row's northern edge.
func tileNorthLat(y, zoom int) float64 {
	n := math.Pi * (1 - 2*float64(y)/math.Ldexp(1, zoom))
	return 180 / math.Pi * math.Atan(math.Sinh(n))
}

// TILE_X(lon, zoom) returns the tile column containing lon.
func evalTileX(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	lon, err := evalGeoFloatArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return tileColumnFor(lon, zoom), nil
}

// TILE_Y(lat, zoom) returns the XYZ tile row containing lat (0 = northernmost).
func evalTileY(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	lat, err := evalGeoFloatArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return tileRowFor(lat, zoom), nil
}

// TILE_ZXY(lon, lat, zoom) returns the covering tile as JSON, including the
// MBTiles TMS row so a caller can go straight to a tiles-table lookup:
//
//	{"z":14,"x":8529,"y":5975,"tile_row":10408}
func evalTileZXY(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 3, 3); err != nil {
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
	zoom, err := tileZoomArg(env, ex, row, 2)
	if err != nil {
		return nil, err
	}
	x := tileColumnFor(lon, zoom)
	y := tileRowFor(lat, zoom)
	out, err := json.Marshal(map[string]any{
		"z": zoom, "x": x, "y": y, "tile_row": (1 << uint(zoom)) - 1 - y,
	})
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// TILE_FLIP_Y(y, zoom) converts between the XYZ row a web client requests and
// the TMS tile_row the MBTiles specification stores. The mapping is its own
// inverse, so one function serves both directions.
func evalTileFlipY(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 0, zoom, "y")
	if err != nil {
		return nil, err
	}
	return (1 << uint(zoom)) - 1 - y, nil
}

// TILE_LON(x, zoom) returns the longitude of the tile's western edge.
func evalTileLon(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	x, err := tileIndexArg(env, ex, row, 0, zoom, "x")
	if err != nil {
		return nil, err
	}
	return tileWestLon(x, zoom), nil
}

// TILE_LAT(y, zoom) returns the latitude of the XYZ tile row's northern edge.
func evalTileLat(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 0, zoom, "y")
	if err != nil {
		return nil, err
	}
	return tileNorthLat(y, zoom), nil
}

// TILE_BBOX(zoom, x, y) returns the tile's geographic bounds as a JSON array in
// the [west, south, east, north] order MBTiles metadata and TileJSON use.
func evalTileBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 3, 3); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	x, err := tileIndexArg(env, ex, row, 1, zoom, "x")
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 2, zoom, "y")
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal([]float64{
		tileWestLon(x, zoom),
		tileNorthLat(y+1, zoom), // south edge is the next row's north edge
		tileWestLon(x+1, zoom),
		tileNorthLat(y, zoom),
	})
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// TILE_BBOX_3857(zoom,x,y) returns the XYZ tile extent in projected Web
// Mercator meters. WMS/WMTS clients can use it directly as an EPSG:3857 BBOX
// without a per-corner ST_TRANSFORM round trip.
func evalTileBBox3857(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 3, 3); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	x, err := tileIndexArg(env, ex, row, 1, zoom, "x")
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 2, zoom, "y")
	if err != nil {
		return nil, err
	}
	extent := math.Pi * tileWebMercatorRadiusMeters
	span := (2 * extent) / math.Ldexp(1, zoom)
	minX := -extent + float64(x)*span
	maxY := extent - float64(y)*span
	out, err := json.Marshal([]float64{minX, maxY - span, minX + span, maxY})
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// TILE_RESOLUTION(zoom [,tile_size]) returns projected meters per pixel for
// the standard WebMercatorQuad matrix. It applies equally to raster DTK/DOP
// tiles and vector tiles; 256 pixels is used when tile_size is omitted.
func evalTileResolution(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 2); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	tileSize := tileDefaultPixelSize
	if len(ex.Args) == 2 {
		tileSize, err = tilePixelSizeArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
	}
	worldMeters := 2 * math.Pi * tileWebMercatorRadiusMeters
	return worldMeters / (float64(tileSize) * math.Ldexp(1, zoom)), nil
}

// WMTS_SCALE_DENOMINATOR(zoom [,tile_size]) returns the OGC WMTS
// ScaleDenominator, defined using the standard 0.28 mm pixel size.
func evalWMTSScaleDenominator(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	resolution, err := evalTileResolution(env, ex, row)
	if err != nil {
		return nil, err
	}
	return resolution.(float64) / wmtsStandardPixelMeters, nil
}

// WMS_BBOX(minX,minY,maxX,maxY,crs [,version [,axis_order]]) formats a WMS
// BBOX parameter. Inputs always use GIS x/y order. WMS 1.3 uses the declared
// CRS axis order; the built-in CRS registry covers common OGC, INSPIRE, AdV,
// ETRS89 and DHDN identifiers. An explicit axis_order (xy or yx, with common
// descriptive spellings accepted) handles every other CRS without a service-
// specific code path. WMS 1.1 retains x/y. The default version is 1.3.0.
func evalWMSBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 5, 7); err != nil {
		return nil, err
	}
	coords := [4]float64{}
	for i := range coords {
		value, err := evalGeoFloatArg(env, ex, row, i)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("%s: bbox coordinates must be finite", ex.Name)
		}
		coords[i] = value
	}
	crsValue, err := evalExpr(env, ex.Args[4], row)
	if err != nil {
		return nil, err
	}
	crs, ok := crsValue.(string)
	if !ok || strings.TrimSpace(crs) == "" {
		return nil, fmt.Errorf("%s: crs must be a non-empty string", ex.Name)
	}
	version := "1.3.0"
	if len(ex.Args) >= 6 {
		versionValue, err := evalExpr(env, ex.Args[5], row)
		if err != nil {
			return nil, err
		}
		version, ok = versionValue.(string)
		if !ok || strings.TrimSpace(version) == "" {
			return nil, fmt.Errorf("%s: version must be a non-empty string", ex.Name)
		}
	}
	swap := false
	if len(ex.Args) == 7 {
		axisValue, err := evalExpr(env, ex.Args[6], row)
		if err != nil {
			return nil, err
		}
		axis, ok := axisValue.(string)
		if !ok || strings.TrimSpace(axis) == "" {
			return nil, fmt.Errorf("%s: axis_order must be a non-empty string", ex.Name)
		}
		swap, err = parseExplicitAxisOrder(axis)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
	} else if id, err := parseCRSIdentifier(crs); err == nil {
		swap, _ = crsNeedsYX(id)
	}
	if strings.HasPrefix(strings.TrimSpace(version), "1.3") && swap {
		coords = [4]float64{coords[1], coords[0], coords[3], coords[2]}
	}
	return fmt.Sprintf("%.15g,%.15g,%.15g,%.15g", coords[0], coords[1], coords[2], coords[3]), nil
}

// TILE_MATRIX_BBOX(origin_x,origin_y,cell_size,tile_width,tile_height,
// tile_col,tile_row [,corner]) returns a tile extent for an arbitrary OGC
// TileMatrix. corner is topLeft (the default) or bottomLeft. Coordinates and
// cell_size stay in the TileMatrixSet CRS, so this supports national and
// European projected grids as well as WebMercatorQuad.
func evalTileMatrixBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 7, 8); err != nil {
		return nil, err
	}
	values := [5]float64{}
	for i := range values {
		v, err := evalGeoFloatArg(env, ex, row, i)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%s: matrix parameters must be finite", ex.Name)
		}
		values[i] = v
	}
	if values[2] <= 0 || values[3] <= 0 || values[4] <= 0 {
		return nil, fmt.Errorf("%s: cell_size, tile_width, and tile_height must be positive", ex.Name)
	}
	col, err := tileNonNegativeIndexArg(env, ex, row, 5, "tile_col")
	if err != nil {
		return nil, err
	}
	rowIndex, err := tileNonNegativeIndexArg(env, ex, row, 6, "tile_row")
	if err != nil {
		return nil, err
	}
	corner, err := tileMatrixCornerArg(env, ex, row, 7)
	if err != nil {
		return nil, err
	}
	originX, originY, cellSize := values[0], values[1], values[2]
	spanX, spanY := values[3]*cellSize, values[4]*cellSize
	minX := originX + float64(col)*spanX
	var minY, maxY float64
	if corner == "topleft" {
		maxY = originY - float64(rowIndex)*spanY
		minY = maxY - spanY
	} else {
		minY = originY + float64(rowIndex)*spanY
		maxY = minY + spanY
	}
	body, err := json.Marshal([]float64{minX, minY, minX + spanX, maxY})
	if err != nil {
		return nil, err
	}
	return string(body), nil
}

// TILE_MATRIX_POSITION(x,y,origin_x,origin_y,cell_size,tile_width,
// tile_height [,corner]) returns {"tile_col":...,"tile_row":...} for an
// arbitrary OGC TileMatrix.
func evalTileMatrixPosition(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 7, 8); err != nil {
		return nil, err
	}
	values := [7]float64{}
	for i := range values {
		v, err := evalGeoFloatArg(env, ex, row, i)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%s: coordinates and matrix parameters must be finite", ex.Name)
		}
		values[i] = v
	}
	if values[4] <= 0 || values[5] <= 0 || values[6] <= 0 {
		return nil, fmt.Errorf("%s: cell_size, tile_width, and tile_height must be positive", ex.Name)
	}
	corner, err := tileMatrixCornerArg(env, ex, row, 7)
	if err != nil {
		return nil, err
	}
	spanX, spanY := values[5]*values[4], values[6]*values[4]
	col := int64(math.Floor((values[0] - values[2]) / spanX))
	var matrixRow int64
	if corner == "topleft" {
		matrixRow = int64(math.Floor((values[3] - values[1]) / spanY))
	} else {
		matrixRow = int64(math.Floor((values[1] - values[3]) / spanY))
	}
	if col < 0 || matrixRow < 0 {
		return nil, fmt.Errorf("%s: coordinate lies before the TileMatrix origin", ex.Name)
	}
	body, err := json.Marshal(map[string]int64{"tile_col": col, "tile_row": matrixRow})
	if err != nil {
		return nil, err
	}
	return string(body), nil
}

func tileNonNegativeIndexArg(env ExecEnv, ex *FuncCall, row Row, idx int, name string) (int, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return 0, err
	}
	n, err := toInt(v)
	if err != nil || n < 0 {
		if err == nil {
			err = fmt.Errorf("must not be negative")
		}
		return 0, fmt.Errorf("%s %s: %w", ex.Name, name, err)
	}
	return n, nil
}

func tileMatrixCornerArg(env ExecEnv, ex *FuncCall, row Row, idx int) (string, error) {
	if len(ex.Args) <= idx {
		return "topleft", nil
	}
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s: corner must be topLeft or bottomLeft", ex.Name)
	}
	normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "", " ", "").Replace(strings.TrimSpace(s)))
	if normalized != "topleft" && normalized != "bottomleft" {
		return "", fmt.Errorf("%s: corner must be topLeft or bottomLeft", ex.Name)
	}
	return normalized, nil
}

// TILE_QUADKEY(zoom, x, y) returns the Bing Maps quadkey for an XYZ tile: one
// base-4 digit per zoom level, each encoding the quadrant descended into.
func evalTileQuadkey(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 3, 3); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	x, err := tileIndexArg(env, ex, row, 1, zoom, "x")
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 2, zoom, "y")
	if err != nil {
		return nil, err
	}
	return tileQuadkeyFor(zoom, x, y), nil
}

func tileQuadkeyFor(zoom, x, y int) string {
	var sb strings.Builder
	sb.Grow(zoom)
	for i := zoom; i > 0; i-- {
		mask := 1 << uint(i-1)
		digit := byte('0')
		if x&mask != 0 {
			digit++
		}
		if y&mask != 0 {
			digit += 2
		}
		sb.WriteByte(digit)
	}
	return sb.String()
}

// TILE_FROM_QUADKEY(quadkey) reverses TILE_QUADKEY, returning JSON
// {"z":..,"x":..,"y":..} with y in XYZ order. An empty quadkey is the zoom-0
// world tile.
func evalTileFromQuadkey(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	qk, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%s: quadkey must be a string, got %T", ex.Name, v)
	}
	if len(qk) > tileMaxZoom {
		return nil, fmt.Errorf("%s: quadkey length %d exceeds max zoom %d", ex.Name, len(qk), tileMaxZoom)
	}
	var x, y int
	zoom := len(qk)
	for i, c := range qk {
		mask := 1 << uint(zoom-i-1)
		switch c {
		case '0':
		case '1':
			x |= mask
		case '2':
			y |= mask
		case '3':
			x |= mask
			y |= mask
		default:
			return nil, fmt.Errorf("%s: invalid quadkey digit %q", ex.Name, string(c))
		}
	}
	out, err := json.Marshal(map[string]any{"z": zoom, "x": x, "y": y})
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// TILE_PARENT(zoom, x, y) returns the containing tile one zoom level up, as
// JSON {"z":..,"x":..,"y":..}. The zoom-0 tile has no parent and yields NULL.
func evalTileParent(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 3, 3); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	x, err := tileIndexArg(env, ex, row, 1, zoom, "x")
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 2, zoom, "y")
	if err != nil {
		return nil, err
	}
	if zoom == 0 {
		return nil, nil
	}
	out, err := json.Marshal(map[string]any{"z": zoom - 1, "x": x / 2, "y": y / 2})
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// TILE_COUNT(zoom) returns how many tiles a fully populated zoom level holds
// (4^zoom), for estimating tileset size. Returned as a float64 beyond zoom 26
// would lose precision, so the range check keeps it exact in int64.
func evalTileCount(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	if zoom > 31 {
		return nil, fmt.Errorf("%s: zoom %d overflows an exact tile count", ex.Name, zoom)
	}
	side := int64(1) << uint(zoom)
	return side * side, nil
}

// TILE_CONTAINS(zoom, x, y, lon, lat) reports whether the tile covers the point.
// Edges belong to the tile on their north and west sides, matching how
// TILE_X/TILE_Y assign a point to exactly one tile.
func evalTileContains(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 5, 5); err != nil {
		return nil, err
	}
	zoom, err := tileZoomArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	x, err := tileIndexArg(env, ex, row, 1, zoom, "x")
	if err != nil {
		return nil, err
	}
	y, err := tileIndexArg(env, ex, row, 2, zoom, "y")
	if err != nil {
		return nil, err
	}
	lon, err := evalGeoFloatArg(env, ex, row, 3)
	if err != nil {
		return nil, err
	}
	lat, err := evalGeoFloatArg(env, ex, row, 4)
	if err != nil {
		return nil, err
	}
	// Comparing tile indices rather than bounds keeps this exactly consistent
	// with TILE_X/TILE_Y, including at tile edges and clamped latitudes.
	return tileColumnFor(lon, zoom) == x && tileRowFor(lat, zoom) == y, nil
}
