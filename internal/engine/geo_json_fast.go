// A small, allocation-lean decoder for the GeoJSON every GIS function reads.
//
// GEO_* and ST_* functions take geometries as GeoJSON text and decode them on
// every call. encoding/json (on current Go a layer over json/v2, reflecting
// into map[string]any) made that decode more than half of the CPU time of a
// typical GIS query; the geometry arithmetic itself is tiny by comparison.
//
// The decoders here accept only the plain, compact subset that tinySQL itself
// writes (see canonicalGeoJSON): ASCII strings without escapes, JSON-grammar
// numbers. Typed geometry headers reject duplicate members; the generic
// object decoder keeps the last value like encoding/json. These decoders
// report ok=false for
// anything else, and the callers then run the original encoding/json path --
// so malformed input, exotic input and every error message stay exactly what
// they were. When a decoder does accept a value, it is the value encoding/json
// would have produced; geo_json_fast_test.go checks that differentially.
package engine

import (
	"strconv"
	"strings"
)

// fastJSONMaxDepth bounds recursion. encoding/json allows far deeper
// documents; anything nested more than this simply takes the slow path.
const fastJSONMaxDepth = 64

type fastJSON struct {
	b string
	i int
}

func (s *fastJSON) skipSpace() {
	for s.i < len(s.b) {
		switch s.b[s.i] {
		case ' ', '\t', '\n', '\r':
			s.i++
		default:
			return
		}
	}
}

// decodeJSONObjectFast decodes body into the map encoding/json.Unmarshal would
// produce for a map[string]any target, or reports ok=false.
func decodeJSONObjectFast(body string) (map[string]any, bool) {
	s := fastJSON{b: body}
	s.skipSpace()
	if s.i >= len(s.b) || s.b[s.i] != '{' {
		return nil, false
	}
	obj, ok := s.object(0)
	if !ok {
		return nil, false
	}
	s.skipSpace()
	if s.i != len(s.b) {
		return nil, false
	}
	return obj, true
}

func (s *fastJSON) value(depth int) (any, bool) {
	if depth > fastJSONMaxDepth || s.i >= len(s.b) {
		return nil, false
	}
	switch c := s.b[s.i]; {
	case c == '{':
		obj, ok := s.object(depth)
		if !ok {
			return nil, false
		}
		return obj, true
	case c == '[':
		arr, ok := s.array(depth)
		if !ok {
			return nil, false
		}
		return arr, true
	case c == '"':
		str, ok := s.str()
		if !ok {
			return nil, false
		}
		return str, true
	case c == '-' || (c >= '0' && c <= '9'):
		f, ok := s.number()
		if !ok {
			return nil, false
		}
		return f, true
	case c == 't':
		if s.literal("true") {
			return true, true
		}
	case c == 'f':
		if s.literal("false") {
			return false, true
		}
	case c == 'n':
		if s.literal("null") {
			return nil, true
		}
	}
	return nil, false
}

func (s *fastJSON) literal(word string) bool {
	if !strings.HasPrefix(s.b[s.i:], word) {
		return false
	}
	s.i += len(word)
	return true
}

func (s *fastJSON) object(depth int) (map[string]any, bool) {
	s.i++ // '{'
	obj := make(map[string]any, 2)
	s.skipSpace()
	if s.i < len(s.b) && s.b[s.i] == '}' {
		s.i++
		return obj, true
	}
	for {
		s.skipSpace()
		if s.i >= len(s.b) || s.b[s.i] != '"' {
			return nil, false
		}
		key, ok := s.key()
		if !ok {
			return nil, false
		}
		s.skipSpace()
		if s.i >= len(s.b) || s.b[s.i] != ':' {
			return nil, false
		}
		s.i++
		s.skipSpace()
		val, ok := s.value(depth + 1)
		if !ok {
			return nil, false
		}
		obj[key] = val
		s.skipSpace()
		if s.i >= len(s.b) {
			return nil, false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case '}':
			s.i++
			return obj, true
		default:
			return nil, false
		}
	}
}

func (s *fastJSON) array(depth int) ([]any, bool) {
	s.i++ // '['
	s.skipSpace()
	if s.i < len(s.b) && s.b[s.i] == ']' {
		s.i++
		return []any{}, true
	}
	arr := make([]any, 0, 2)
	for {
		s.skipSpace()
		val, ok := s.value(depth + 1)
		if !ok {
			return nil, false
		}
		arr = append(arr, val)
		s.skipSpace()
		if s.i >= len(s.b) {
			return nil, false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			return arr, true
		default:
			return nil, false
		}
	}
}

// rawString scans a string literal and returns its bytes, rejecting anything
// beyond plain ASCII: escapes, control bytes and non-ASCII would need
// encoding/json's exact handling, so they take the slow path.
func (s *fastJSON) rawString() (string, bool) {
	start := s.i + 1
	for j := start; j < len(s.b); j++ {
		switch c := s.b[j]; {
		case c == '"':
			s.i = j + 1
			return s.b[start:j], true
		case c == '\\' || c < 0x20 || c >= 0x80:
			return "", false
		}
	}
	return "", false
}

func (s *fastJSON) str() (string, bool) {
	raw, ok := s.rawString()
	if !ok {
		return "", false
	}
	return internGeoWord(raw), true
}

func (s *fastJSON) key() (string, bool) { return s.str() }

// internGeoWord returns the string for raw without allocating for the words
// that make up nearly every GeoJSON document.
func internGeoWord(raw string) string {
	switch raw {
	case "type":
		return "type"
	case "coordinates":
		return "coordinates"
	case "Point":
		return "Point"
	case "MultiPoint":
		return "MultiPoint"
	case "LineString":
		return "LineString"
	case "MultiLineString":
		return "MultiLineString"
	case "Polygon":
		return "Polygon"
	case "MultiPolygon":
		return "MultiPolygon"
	case "GeometryCollection":
		return "GeometryCollection"
	case "geometries":
		return "geometries"
	}
	// Clone: a substring would keep the whole geometry text alive for as long
	// as the decoded map is.
	return strings.Clone(raw)
}

// exactPow10 holds the powers of ten a float64 represents exactly (10^0..10^22).
var exactPow10 = [...]float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
	1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
}

// number scans a JSON number with the exact grammar encoding/json enforces and
// converts it to the float64 encoding/json would produce.
//
// Coordinates are almost always short decimals, for which the conversion is one
// exact IEEE operation: a mantissa of at most 2^53 is exactly representable, so
// is 10^k for k <= 22, and a single multiplication or division of two exact
// values is correctly rounded (Clinger's fast path, the same shortcut
// strconv.ParseFloat takes internally). The digits are accumulated while they
// are validated, so the common case never makes a second pass over the text.
// Every other number -- long mantissas, large exponents -- goes through
// strconv.ParseFloat.
func (s *fastJSON) number() (float64, bool) {
	start := s.i
	j := s.i
	neg := false
	if j < len(s.b) && s.b[j] == '-' {
		neg = true
		j++
	}
	if j >= len(s.b) {
		return 0, false
	}
	var mant uint64
	digits := 0 // significant digits accumulated into mant (leading zeros excluded)
	exp10 := 0  // decimal exponent applied to mant
	exact := true
	switch c := s.b[j]; {
	case c == '0':
		j++
	case c >= '1' && c <= '9':
		for j < len(s.b) && s.b[j] >= '0' && s.b[j] <= '9' {
			if digits < 19 {
				mant = mant*10 + uint64(s.b[j]-'0')
				digits++
			} else {
				exact = false
			}
			j++
		}
	default:
		return 0, false
	}
	if j < len(s.b) && s.b[j] == '.' {
		j++
		frac := 0
		for j < len(s.b) && s.b[j] >= '0' && s.b[j] <= '9' {
			if digits > 0 || s.b[j] != '0' {
				if digits < 19 {
					mant = mant*10 + uint64(s.b[j]-'0')
					digits++
					exp10--
				} else {
					exact = false
				}
			} else {
				exp10-- // a leading zero after the point only shifts the exponent
			}
			j++
			frac++
		}
		if frac == 0 {
			return 0, false
		}
	}
	if j < len(s.b) && (s.b[j] == 'e' || s.b[j] == 'E') {
		j++
		expNeg := false
		if j < len(s.b) && (s.b[j] == '+' || s.b[j] == '-') {
			expNeg = s.b[j] == '-'
			j++
		}
		e, edigits := 0, 0
		for j < len(s.b) && s.b[j] >= '0' && s.b[j] <= '9' {
			if e < 10000 {
				e = e*10 + int(s.b[j]-'0')
			}
			j++
			edigits++
		}
		if edigits == 0 {
			return 0, false
		}
		if expNeg {
			e = -e
		}
		exp10 += e
	}
	if exact && mant <= 1<<53 && exp10 >= -22 && exp10 <= 22 {
		f := float64(mant)
		if exp10 < 0 {
			f /= exactPow10[-exp10]
		} else {
			f *= exactPow10[exp10]
		}
		if neg {
			f = -f
		}
		s.i = j
		return f, true
	}
	f, err := strconv.ParseFloat(s.b[start:j], 64)
	if err != nil {
		return 0, false // out of range: let encoding/json report it
	}
	s.i = j
	return f, true
}

// ── typed GeoJSON geometry decoding ────────────────────────────────────────
//
// The functions above reproduce encoding/json's generic result. Most geometry
// functions then turn that map into geoPoint / geoRing values; the decoders
// below go straight from text to those types for the four shapes the hot
// functions use, skipping the intermediate map[string]any entirely.

// geoFastHeader locates a geometry's "type" and the byte range of its
// "coordinates" value. It accepts exactly the two members tinySQL writes, in
// either order, each once, and requires the coordinates to contain nothing but
// brackets, numbers, commas and whitespace.
func geoFastHeader(body string) (typ string, coords string, ok bool) {
	s := fastJSON{b: body}
	s.skipSpace()
	if s.i >= len(s.b) || s.b[s.i] != '{' {
		return "", "", false
	}
	s.i++
	var haveType, haveCoords bool
	for {
		s.skipSpace()
		if s.i >= len(s.b) || s.b[s.i] != '"' {
			return "", "", false
		}
		rawKey, ok := s.rawString()
		if !ok {
			return "", "", false
		}
		s.skipSpace()
		if s.i >= len(s.b) || s.b[s.i] != ':' {
			return "", "", false
		}
		s.i++
		s.skipSpace()
		switch rawKey {
		case "type":
			if haveType || s.i >= len(s.b) || s.b[s.i] != '"' {
				return "", "", false
			}
			word, ok := s.str()
			if !ok {
				return "", "", false
			}
			typ, haveType = word, true
		case "coordinates":
			if haveCoords || s.i >= len(s.b) || s.b[s.i] != '[' {
				return "", "", false
			}
			start := s.i
			depth := 0
		scan:
			for ; s.i < len(s.b); s.i++ {
				switch c := s.b[s.i]; {
				case c == '[':
					depth++
				case c == ']':
					depth--
					if depth == 0 {
						s.i++
						break scan
					}
				case c == ',' || c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
					c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' || (c >= '0' && c <= '9'):
				default:
					return "", "", false
				}
			}
			if depth != 0 {
				return "", "", false
			}
			coords, haveCoords = s.b[start:s.i], true
		default:
			return "", "", false
		}
		s.skipSpace()
		if s.i >= len(s.b) {
			return "", "", false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case '}':
			s.i++
			s.skipSpace()
			if s.i != len(s.b) || !haveType || !haveCoords {
				return "", "", false
			}
			return typ, coords, true
		default:
			return "", "", false
		}
	}
}

// geoEqualFold reports whether a equals the ASCII-lowercase word b, ignoring
// the case of a -- the comparison the slow path makes with strings.EqualFold.
func geoEqualFold(a, lower string) bool {
	if len(a) != len(lower) {
		return false
	}
	for i := 0; i < len(a); i++ {
		c := a[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

// position parses one "[lon, lat]" or "[lon, lat, z, ...]" at s.i.
func (s *fastJSON) position() (geoPoint, bool) {
	if s.i >= len(s.b) || s.b[s.i] != '[' {
		return geoPoint{}, false
	}
	s.i++
	s.skipSpace()
	lon, ok := s.number()
	if !ok {
		return geoPoint{}, false
	}
	s.skipSpace()
	if s.i >= len(s.b) || s.b[s.i] != ',' {
		return geoPoint{}, false
	}
	s.i++
	s.skipSpace()
	lat, ok := s.number()
	if !ok {
		return geoPoint{}, false
	}
	p := geoPoint{Lon: lon, Lat: lat}
	for n := 2; ; n++ {
		s.skipSpace()
		if s.i >= len(s.b) {
			return geoPoint{}, false
		}
		switch s.b[s.i] {
		case ']':
			s.i++
			return p, true
		case ',':
			s.i++
			s.skipSpace()
			v, ok := s.number()
			if !ok {
				return geoPoint{}, false
			}
			if n == 2 {
				z := v
				p.Z = &z
			}
		default:
			return geoPoint{}, false
		}
	}
}

// positions parses "[pos, pos, ...]" and requires at least min entries.
func (s *fastJSON) positions(min int) ([]geoPoint, bool) {
	if s.i >= len(s.b) || s.b[s.i] != '[' {
		return nil, false
	}
	s.i++
	s.skipSpace()
	out := make([]geoPoint, 0, 8)
	for {
		s.skipSpace()
		p, ok := s.position()
		if !ok {
			return nil, false
		}
		out = append(out, p)
		s.skipSpace()
		if s.i >= len(s.b) {
			return nil, false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			if len(out) < min {
				return nil, false
			}
			return out, true
		default:
			return nil, false
		}
	}
}

// polygonRings parses "[ring, ring, ...]" with every ring a closed-ring-sized
// (>= 4 positions) position list.
func (s *fastJSON) polygonRings() (geoPolygon, bool) {
	if s.i >= len(s.b) || s.b[s.i] != '[' {
		return geoPolygon{}, false
	}
	s.i++
	poly := geoPolygon{Rings: make([]geoRing, 0, 1)}
	for {
		s.skipSpace()
		ring, ok := s.positions(4)
		if !ok {
			return geoPolygon{}, false
		}
		poly.Rings = append(poly.Rings, geoRing(ring))
		s.skipSpace()
		if s.i >= len(s.b) {
			return geoPolygon{}, false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			return poly, true
		default:
			return geoPolygon{}, false
		}
	}
}

// decodeGeoPointFast is geoPointFromJSON without the intermediate map.
func decodeGeoPointFast(body string) (geoPoint, bool) {
	typ, coords, ok := geoFastHeader(body)
	if !ok || !geoEqualFold(typ, "point") {
		return geoPoint{}, false
	}
	s := fastJSON{b: coords}
	p, ok := s.position()
	if !ok || s.i != len(s.b) {
		return geoPoint{}, false
	}
	return p, true
}

// decodeGeoLineStringFast is geoLineStringFromValue for a textual LineString.
func decodeGeoLineStringFast(body string) (geoLineString, bool) {
	typ, coords, ok := geoFastHeader(body)
	if !ok || !geoEqualFold(typ, "linestring") {
		return nil, false
	}
	s := fastJSON{b: coords}
	pts, ok := s.positions(2)
	if !ok || s.i != len(s.b) {
		return nil, false
	}
	return geoLineString(pts), true
}

// decodeGeoMultiPolygonFast is geoMultiPolygonFromValue for a textual Polygon
// or MultiPolygon.
func decodeGeoMultiPolygonFast(body string) (geoMultiPolygon, bool) {
	typ, coords, ok := geoFastHeader(body)
	if !ok {
		return geoMultiPolygon{}, false
	}
	s := fastJSON{b: coords}
	switch {
	case geoEqualFold(typ, "polygon"):
		poly, ok := s.polygonRings()
		if !ok || s.i != len(s.b) {
			return geoMultiPolygon{}, false
		}
		return geoMultiPolygon{Polygons: []geoPolygon{poly}}, true
	case geoEqualFold(typ, "multipolygon"):
		s.i++ // '['
		mp := geoMultiPolygon{Polygons: make([]geoPolygon, 0, 2)}
		for {
			s.skipSpace()
			poly, ok := s.polygonRings()
			if !ok {
				return geoMultiPolygon{}, false
			}
			mp.Polygons = append(mp.Polygons, poly)
			s.skipSpace()
			if s.i >= len(s.b) {
				return geoMultiPolygon{}, false
			}
			switch s.b[s.i] {
			case ',':
				s.i++
				continue
			case ']':
				s.i++
				if s.i != len(s.b) {
					return geoMultiPolygon{}, false
				}
				return mp, true
			default:
				return geoMultiPolygon{}, false
			}
		}
	}
	return geoMultiPolygon{}, false
}
