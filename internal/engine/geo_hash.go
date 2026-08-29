// Geohash: encode/decode a lon/lat point to/from the base-32 string Gustavo
// Niemeyer's geohash.org popularized, plus the neighbor and bounding-box
// helpers that make it useful as a coarse spatial-locality key (e.g. a
// GROUP BY or index prefix column that clusters nearby points together
// without needing GEO_SEARCH's grid index).
//
// The algorithm is the standard interleaved-bit one: alternately halve a
// longitude range and a latitude range, appending a 1 bit each time the
// point falls in the upper half, and packing every 5 bits into one base-32
// character using the same non-alphabetic-lookalike alphabet
// (0-9b-hjkmnp-z, omitting a, i, l, o) the reference implementation uses.
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

func getGeoHashFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_GEOHASH_ENCODE":    evalGeoHashEncode,
		"ST_GEOHASH":            evalGeoHashEncode,
		"GEO_GEOHASH_DECODE":    evalGeoHashDecode,
		"GEO_GEOHASH_BBOX":      evalGeoHashBBox,
		"GEO_GEOHASH_NEIGHBORS": evalGeoHashNeighbors,
	}
}

const geoHashAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// geoHashMaxPrecision bounds the requested character length. Beyond ~20
// characters the bit interleaving has long since exceeded float64's mantissa
// precision, so any additional character would encode noise rather than
// real position.
const geoHashMaxPrecision = 20

// geoHashDefaultPrecision matches geohash.org's default, giving ~2.4m x
// 1.2m resolution at the equator -- a reasonable default for "encode this
// point" without a caller having to think about precision first.
const geoHashDefaultPrecision = 9

func geoHashEncode(lon, lat float64, precision int) (string, error) {
	if precision < 1 || precision > geoHashMaxPrecision {
		return "", fmt.Errorf("geohash precision must be in 1..%d", geoHashMaxPrecision)
	}
	if lon < -180 || lon > 180 {
		return "", fmt.Errorf("geohash longitude %g out of range [-180, 180]", lon)
	}
	if lat < -90 || lat > 90 {
		return "", fmt.Errorf("geohash latitude %g out of range [-90, 90]", lat)
	}
	lonRange := [2]float64{-180, 180}
	latRange := [2]float64{-90, 90}

	var out strings.Builder
	bit, ch, isLon := 0, 0, true
	for out.Len() < precision {
		var mid float64
		if isLon {
			mid = (lonRange[0] + lonRange[1]) / 2
			if lon >= mid {
				ch = ch<<1 | 1
				lonRange[0] = mid
			} else {
				ch = ch << 1
				lonRange[1] = mid
			}
		} else {
			mid = (latRange[0] + latRange[1]) / 2
			if lat >= mid {
				ch = ch<<1 | 1
				latRange[0] = mid
			} else {
				ch = ch << 1
				latRange[1] = mid
			}
		}
		isLon = !isLon
		bit++
		if bit == 5 {
			out.WriteByte(geoHashAlphabet[ch])
			bit, ch = 0, 0
		}
	}
	return out.String(), nil
}

// geoHashDecodeBBox is the shared core of decode/bbox: it narrows the same
// lon/lat interval halving the encoder performed and returns the resulting
// cell bounds, from which a decode takes the midpoint.
func geoHashDecodeBBox(hash string) (minLon, minLat, maxLon, maxLat float64, err error) {
	if hash == "" {
		return 0, 0, 0, 0, fmt.Errorf("geohash must not be empty")
	}
	lonRange := [2]float64{-180, 180}
	latRange := [2]float64{-90, 90}
	isLon := true
	for i := 0; i < len(hash); i++ {
		idx := strings.IndexByte(geoHashAlphabet, hash[i])
		if idx < 0 {
			return 0, 0, 0, 0, fmt.Errorf("invalid geohash character %q at position %d", hash[i], i)
		}
		for bit := 4; bit >= 0; bit-- {
			bitVal := (idx >> uint(bit)) & 1
			if isLon {
				mid := (lonRange[0] + lonRange[1]) / 2
				if bitVal == 1 {
					lonRange[0] = mid
				} else {
					lonRange[1] = mid
				}
			} else {
				mid := (latRange[0] + latRange[1]) / 2
				if bitVal == 1 {
					latRange[0] = mid
				} else {
					latRange[1] = mid
				}
			}
			isLon = !isLon
		}
	}
	return lonRange[0], latRange[0], lonRange[1], latRange[1], nil
}

// evalGeoHashEncode accepts (point), (point, precision), or
// (lon, lat, precision) -- deliberately not a bare (lon, lat) two-number
// form, since that would be ambiguous with (point, precision) without
// inspecting argument values at eval time. A caller with raw numbers and no
// precision preference can wrap them in GEO_POINT.
func evalGeoHashEncode(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 3); err != nil {
		return nil, err
	}
	precision := geoHashDefaultPrecision
	var lon, lat float64
	switch len(ex.Args) {
	case 1, 2:
		p, err := evalGeoPointArg(env, ex, row, 0)
		if err != nil {
			return nil, err
		}
		lon, lat = p.Lon, p.Lat
		if len(ex.Args) == 2 {
			f, err := evalGeoFloatArg(env, ex, row, 1)
			if err != nil {
				return nil, err
			}
			if f != math.Trunc(f) {
				return nil, fmt.Errorf("%s precision must be an integer", ex.Name)
			}
			precision = int(f)
		}
	case 3:
		var err error
		lon, err = evalGeoFloatArg(env, ex, row, 0)
		if err != nil {
			return nil, err
		}
		lat, err = evalGeoFloatArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
		f, err := evalGeoFloatArg(env, ex, row, 2)
		if err != nil {
			return nil, err
		}
		if f != math.Trunc(f) {
			return nil, fmt.Errorf("%s precision must be an integer", ex.Name)
		}
		precision = int(f)
	}
	return geoHashEncode(lon, lat, precision)
}

func evalGeoHashDecode(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	hash, err := evalGeoTextArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	minLon, minLat, maxLon, maxLat, err := geoHashDecodeBBox(hash)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return geoPointJSON((minLon+maxLon)/2, (minLat+maxLat)/2, nil)
}

func evalGeoHashBBox(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	hash, err := evalGeoTextArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	minLon, minLat, maxLon, maxLat, err := geoHashDecodeBBox(hash)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	// Marshalled rather than returned as a []float64, matching TILE_BBOX,
	// GEO_BBOX and ST_BBOX. A raw Go slice reaches a client as Go's default
	// "[1 2 3]" formatting, which is not JSON and which nothing downstream can
	// parse -- the WASM bridge renders any non-scalar with %v.
	out, err := json.Marshal([]float64{minLon, minLat, maxLon, maxLat})
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// evalGeoHashNeighbors returns the 8 geohash cells surrounding hash (and
// hash itself as the 9th element, at index 4, matching the row-major N/S x
// W/E/self ordering most geohash-neighbor implementations use) by nudging
// the decoded center by one cell width/height in each direction and
// re-encoding at the same precision.
func evalGeoHashNeighbors(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	hash, err := evalGeoTextArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	minLon, minLat, maxLon, maxLat, err := geoHashDecodeBBox(hash)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	precision := len(hash)
	lonSpan, latSpan := maxLon-minLon, maxLat-minLat
	centerLon, centerLat := (minLon+maxLon)/2, (minLat+maxLat)/2

	cells := make([]string, 0, 9)
	for _, dLat := range []int{1, 0, -1} {
		for _, dLon := range []int{-1, 0, 1} {
			lon := clampLonWrap(centerLon + float64(dLon)*lonSpan)
			lat := centerLat + float64(dLat)*latSpan
			if lat > 90 {
				lat = 90
			}
			if lat < -90 {
				lat = -90
			}
			h, err := geoHashEncode(lon, lat, precision)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", ex.Name, err)
			}
			cells = append(cells, h)
		}
	}
	// A JSON array string, as in evalGeoHashBBox above.
	out, err := json.Marshal(cells)
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

// clampLonWrap wraps a longitude nudged past +/-180 back into range, since a
// neighbor query near the antimeridian must not simply saturate at the
// boundary the way latitude does at the poles.
func clampLonWrap(lon float64) float64 {
	for lon > 180 {
		lon -= 360
	}
	for lon < -180 {
		lon += 360
	}
	return lon
}
