// Coordinate reference system identifiers and axis order helpers.
//
// The functions in this file intentionally understand identifiers, not
// projections.  A CRS can be named by an EPSG label, an OGC URN, or an OGC
// definition URI without tinySQL acquiring a dependency on a particular map
// service.  Reprojection remains the separate responsibility of ST_TRANSFORM
// (or an external PROJ pipeline).
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
)

type crsIdentifier struct {
	Authority string
	Code      string
	Canonical string
	URI       string
}

type crsAxisInfo struct {
	Axes []string
	Unit string
}

// crsAxes is deliberately a small interoperability registry, not a copy of
// the EPSG database.  It covers the global web CRSs plus the ETRS89, INSPIRE,
// AdV/DHDN variants most often encountered in German and European datasets.
// Unknown CRSs remain usable through explicit axis-order arguments.
var crsAxes = map[string]crsAxisInfo{
	"CRS:84":     {Axes: []string{"longitude", "latitude"}, Unit: "degree"},
	"EPSG:4326":  {Axes: []string{"latitude", "longitude"}, Unit: "degree"},
	"EPSG:4258":  {Axes: []string{"latitude", "longitude"}, Unit: "degree"}, // ETRS89
	"EPSG:3857":  {Axes: []string{"easting", "northing"}, Unit: "metre"},
	"EPSG:25832": {Axes: []string{"easting", "northing"}, Unit: "metre"},
	"EPSG:25833": {Axes: []string{"easting", "northing"}, Unit: "metre"},
	"EPSG:3034":  {Axes: []string{"northing", "easting"}, Unit: "metre"}, // INSPIRE LCC
	"EPSG:3035":  {Axes: []string{"northing", "easting"}, Unit: "metre"}, // INSPIRE LAEA
	"EPSG:3044":  {Axes: []string{"northing", "easting"}, Unit: "metre"}, // UTM32 N-E
	"EPSG:3045":  {Axes: []string{"northing", "easting"}, Unit: "metre"}, // UTM33 N-E
	"EPSG:31467": {Axes: []string{"northing", "easting"}, Unit: "metre"}, // DHDN GK3
	"EPSG:31468": {Axes: []string{"northing", "easting"}, Unit: "metre"}, // DHDN GK4
	"EPSG:31469": {Axes: []string{"northing", "easting"}, Unit: "metre"}, // DHDN GK5
	"EPSG:5677":  {Axes: []string{"easting", "northing"}, Unit: "metre"}, // DHDN GK3 GIS
	"EPSG:5678":  {Axes: []string{"easting", "northing"}, Unit: "metre"}, // DHDN GK4 GIS
	"EPSG:5679":  {Axes: []string{"easting", "northing"}, Unit: "metre"}, // DHDN GK5 GIS
	"EPSG:4647":  {Axes: []string{"easting", "northing"}, Unit: "metre"}, // German zE-N
	"EPSG:5652":  {Axes: []string{"northing", "easting"}, Unit: "metre"}, // German N-zE
	"EPSG:10732": {Axes: []string{"easting", "northing"}, Unit: "metre"}, // DREF91/2016 UTM32
	"EPSG:10733": {Axes: []string{"easting", "northing"}, Unit: "metre"}, // DREF91/2016 UTM33
	"EPSG:10289": {Axes: []string{"easting", "northing"}, Unit: "metre"}, // DREF91/2016 zE-N
	"EPSG:10291": {Axes: []string{"easting", "northing"}, Unit: "metre"},
}

func getCRSFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"CRS_NORMALIZE":  evalCRSNormalize,
		"CRS_URI":        evalCRSURI,
		"CRS_AXIS_ORDER": evalCRSAxisOrder,
		"CRS_INFO":       evalCRSInfo,
	}
}

func parseCRSIdentifier(value any) (crsIdentifier, error) {
	var raw string
	switch v := value.(type) {
	case string:
		raw = strings.TrimSpace(v)
	case int:
		raw = strconv.Itoa(v)
	case int64:
		raw = strconv.FormatInt(v, 10)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) {
			return crsIdentifier{}, fmt.Errorf("CRS numeric code must be a finite integer")
		}
		raw = strconv.FormatInt(int64(v), 10)
	default:
		return crsIdentifier{}, fmt.Errorf("CRS identifier must be text or an EPSG integer, got %T", value)
	}
	if raw == "" {
		return crsIdentifier{}, fmt.Errorf("CRS identifier must not be empty")
	}

	upper := strings.ToUpper(raw)
	if upper == "CRS:84" || upper == "OGC:CRS84" || upper == "CRS84" {
		return crsIdentifier{
			Authority: "OGC", Code: "CRS84", Canonical: "CRS:84",
			URI: "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
		}, nil
	}
	if n, err := strconv.ParseUint(raw, 10, 31); err == nil && n > 0 {
		return epsgCRS(strconv.FormatUint(n, 10)), nil
	}

	// OGC definition URI, for example
	// http://www.opengis.net/def/crs/EPSG/0/25832.
	if parsed, err := url.Parse(raw); err == nil && parsed.Host != "" {
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		for i := 0; i+3 < len(parts); i++ {
			if strings.EqualFold(parts[i], "def") && strings.EqualFold(parts[i+1], "crs") {
				authority := strings.ToUpper(parts[i+2])
				code := parts[len(parts)-1]
				if authority == "EPSG" && isPositiveDecimal(code) {
					return epsgCRS(code), nil
				}
				if authority == "OGC" && strings.EqualFold(code, "CRS84") {
					return parseCRSIdentifier("CRS:84")
				}
			}
		}
		// Legacy GML EPSG URL: .../epsg.xml#4326.
		if strings.EqualFold(parsed.Fragment, strings.TrimSpace(parsed.Fragment)) && isPositiveDecimal(parsed.Fragment) &&
			strings.Contains(strings.ToLower(parsed.Path), "epsg") {
			return epsgCRS(parsed.Fragment), nil
		}
	}

	// OGC URN: urn:ogc:def:crs:EPSG:<version>:25832.  The version is
	// optional and deliberately ignored when producing the stable URI.
	parts := strings.Split(raw, ":")
	if len(parts) >= 7 && strings.EqualFold(parts[0], "urn") && strings.EqualFold(parts[1], "ogc") &&
		strings.EqualFold(parts[2], "def") && strings.EqualFold(parts[3], "crs") {
		authority := strings.ToUpper(parts[4])
		code := parts[len(parts)-1]
		if authority == "EPSG" && isPositiveDecimal(code) {
			return epsgCRS(code), nil
		}
		if authority == "OGC" && strings.EqualFold(code, "CRS84") {
			return parseCRSIdentifier("CRS:84")
		}
	}

	// Human-oriented authority labels, accepting EPSG:4326 and EPSG::4326.
	if strings.HasPrefix(upper, "EPSG:") {
		code := strings.TrimLeft(strings.TrimSpace(raw[len("EPSG:"):]), ":")
		if isPositiveDecimal(code) {
			return epsgCRS(code), nil
		}
	}
	return crsIdentifier{}, fmt.Errorf("unsupported CRS identifier %q; use EPSG:<code>, an OGC CRS URI/URN, CRS:84, or an EPSG integer", raw)
}

func epsgCRS(code string) crsIdentifier {
	// Parsing first removes leading zeroes and makes canonical strings stable.
	n, _ := strconv.ParseUint(code, 10, 31)
	code = strconv.FormatUint(n, 10)
	return crsIdentifier{
		Authority: "EPSG", Code: code, Canonical: "EPSG:" + code,
		URI: "http://www.opengis.net/def/crs/EPSG/0/" + code,
	}
}

func isPositiveDecimal(s string) bool {
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 31)
	return err == nil && n > 0
}

func evalCRSArg(env ExecEnv, ex *FuncCall, row Row) (crsIdentifier, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return crsIdentifier{}, err
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return crsIdentifier{}, err
	}
	if v == nil {
		return crsIdentifier{}, fmt.Errorf("%s: CRS identifier must not be NULL", ex.Name)
	}
	id, err := parseCRSIdentifier(v)
	if err != nil {
		return crsIdentifier{}, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return id, nil
}

func evalCRSNormalize(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	id, err := evalCRSArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	return id.Canonical, nil
}

func evalCRSURI(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	id, err := evalCRSArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	return id.URI, nil
}

func evalCRSAxisOrder(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	id, err := evalCRSArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	info, ok := crsAxes[id.Canonical]
	if !ok {
		return nil, nil
	}
	return strings.Join(info.Axes, ","), nil
}

func evalCRSInfo(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	id, err := evalCRSArg(env, ex, row)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"authority": id.Authority,
		"code":      id.Code,
		"canonical": id.Canonical,
		"uri":       id.URI,
	}
	if info, ok := crsAxes[id.Canonical]; ok {
		out["axes"] = info.Axes
		out["unit"] = info.Unit
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}

func crsNeedsYX(id crsIdentifier) (bool, bool) {
	info, ok := crsAxes[id.Canonical]
	if !ok || len(info.Axes) < 2 {
		return false, false
	}
	first := info.Axes[0]
	return first == "latitude" || first == "northing", true
}

func parseExplicitAxisOrder(raw string) (swap bool, err error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	normalized = strings.NewReplacer(" ", "", "_", "", "-", "", ",", "").Replace(normalized)
	switch normalized {
	case "xy", "en", "eastnorth", "eastingnorthing", "lonlat", "longitudelatitude":
		return false, nil
	case "yx", "ne", "northeast", "northingeasting", "latlon", "latitudelongitude":
		return true, nil
	default:
		return false, fmt.Errorf("axis_order must be xy/yx or an equivalent east,north / north,east / lon,lat / lat,lon spelling")
	}
}
