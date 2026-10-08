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
	"math"
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

// ── canonical GeoJSON without an intermediate value ────────────────────────
//
// canonicalGeoJSON turns any GeoJSON geometry into stable, sorted-key, compact
// text; it is what every INSERT into a GEOMETRY column and every GEO_AS_GEOJSON
// call runs. The slow route decodes into maps, validates, and re-marshals.
// canonicalGeoJSONFast produces the same text in one pass over the input for
// the common, plain case.

// canonicalGeoJSONFast returns the canonical text of the geometry in text, or
// ok=false when the input is anything but a plain Point, MultiPoint,
// LineString, MultiLineString, Polygon or MultiPolygon carrying only "type" and
// numeric "coordinates" -- in which case the caller runs the slow route, which
// also produces every error message.
func canonicalGeoJSONFast(text string) (string, bool) {
	typ, coords, ok := geoFastHeader(text)
	if !ok {
		return "", false
	}
	depth, minPositions := 0, 0
	switch {
	case geoEqualFold(typ, "point"):
	case geoEqualFold(typ, "multipoint"):
		depth = 1
	case geoEqualFold(typ, "linestring"):
		depth, minPositions = 1, 2
	case geoEqualFold(typ, "multilinestring"), geoEqualFold(typ, "polygon"):
		depth = 2
	case geoEqualFold(typ, "multipolygon"):
		depth = 3
	default:
		return "", false
	}
	buf := make([]byte, 0, len(text)+16)
	buf = append(buf, `{"coordinates":`...)
	s := fastJSON{b: coords}
	var count int
	if buf, count, ok = s.canonicalCoords(buf, depth); !ok || s.i != len(s.b) {
		return "", false
	}
	if count < minPositions {
		return "", false
	}
	buf = append(buf, `,"type":"`...)
	buf = append(buf, typ...)
	buf = append(buf, `"}`...)
	return string(buf), true
}

// canonicalCoords copies one coordinates value to buf in canonical form. depth
// is the number of array levels above the positions: 0 for a single position.
// count is the number of positions written, for the LineString minimum.
func (s *fastJSON) canonicalCoords(buf []byte, depth int) (out []byte, count int, ok bool) {
	if s.i >= len(s.b) || s.b[s.i] != '[' {
		return buf, 0, false
	}
	s.i++
	buf = append(buf, '[')
	s.skipSpace()
	if depth == 0 {
		// A position: at least longitude and latitude, all plain numbers.
		n := 0
		for {
			s.skipSpace()
			if buf, ok = s.canonicalNumber(buf); !ok {
				return buf, 0, false
			}
			n++
			s.skipSpace()
			if s.i >= len(s.b) {
				return buf, 0, false
			}
			switch s.b[s.i] {
			case ',':
				s.i++
				buf = append(buf, ',')
				continue
			case ']':
				s.i++
				if n < 2 {
					return buf, 0, false
				}
				return append(buf, ']'), 1, true
			default:
				return buf, 0, false
			}
		}
	}
	if s.i < len(s.b) && s.b[s.i] == ']' { // an empty group is legal
		s.i++
		return append(buf, ']'), 0, true
	}
	for first := true; ; first = false {
		if !first {
			buf = append(buf, ',')
		}
		s.skipSpace()
		var n int
		if buf, n, ok = s.canonicalCoords(buf, depth-1); !ok {
			return buf, 0, false
		}
		count += n
		s.skipSpace()
		if s.i >= len(s.b) {
			return buf, 0, false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			return append(buf, ']'), count, true
		default:
			return buf, 0, false
		}
	}
}

// scanNumberText scans the JSON number at s.i and reports its text and whether
// that text is already the shortest round-trip form of the float64 it denotes.
//
// A decimal of at most 15 significant digits is its own shortest round-trip
// text as long as it has no trailing zero, no exponent, and is not below 1e-6
// (where number formatting switches to exponent form): 15 digits always survive
// a round trip through float64, so no other digit string of that length or
// shorter denotes the same double. Such text can be copied verbatim. For any
// other number the caller formats f, which is parsed with strconv.ParseFloat.
func (s *fastJSON) scanNumberText() (text string, f float64, verbatim, ok bool) {
	start := s.i
	j := s.i
	if j < len(s.b) && s.b[j] == '-' {
		j++
	}
	if j >= len(s.b) {
		return "", 0, false, false
	}
	sig := 0 // significant digits: those after leading zeros
	switch c := s.b[j]; {
	case c == '0':
		j++
	case c >= '1' && c <= '9':
		for j < len(s.b) && s.b[j] >= '0' && s.b[j] <= '9' {
			sig++
			j++
		}
	default:
		return "", 0, false, false
	}
	verbatim = true
	if j < len(s.b) && s.b[j] == '.' {
		j++
		leadingZeros, digits, last := 0, 0, byte('0')
		for j < len(s.b) && s.b[j] >= '0' && s.b[j] <= '9' {
			if sig == 0 && s.b[j] == '0' && digits == leadingZeros {
				leadingZeros++
			} else {
				sig++
			}
			last = s.b[j]
			digits++
			j++
		}
		if digits == 0 {
			return "", 0, false, false
		}
		if last == '0' || leadingZeros >= 6 {
			verbatim = false // a trailing zero is dropped; below 1e-6 is exponent form
		}
	}
	if j < len(s.b) && (s.b[j] == 'e' || s.b[j] == 'E') {
		verbatim = false
		j++
		if j < len(s.b) && (s.b[j] == '+' || s.b[j] == '-') {
			j++
		}
		digits := 0
		for j < len(s.b) && s.b[j] >= '0' && s.b[j] <= '9' {
			j++
			digits++
		}
		if digits == 0 {
			return "", 0, false, false
		}
	}
	if sig > 15 {
		verbatim = false
	}
	text = s.b[start:j]
	if !verbatim {
		var err error
		if f, err = strconv.ParseFloat(text, 64); err != nil {
			return "", 0, false, false // out of range: the slow route reports it
		}
	}
	s.i = j
	return text, f, verbatim, true
}

// canonicalNumber appends the number at s.i in the form json.Marshal would give
// the float64 it parses to.
func (s *fastJSON) canonicalNumber(buf []byte) ([]byte, bool) {
	text, f, verbatim, ok := s.scanNumberText()
	if !ok {
		return buf, false
	}
	if verbatim {
		return append(buf, text...), true
	}
	return appendGeoJSONFloat(buf, f)
}

// geoFastDepth is the number of array levels above the positions of a plain
// geometry type, and false for any other type.
func geoFastDepth(typ string) (int, bool) {
	switch {
	case geoEqualFold(typ, "point"):
		return 0, true
	case geoEqualFold(typ, "multipoint"), geoEqualFold(typ, "linestring"):
		return 1, true
	case geoEqualFold(typ, "multilinestring"), geoEqualFold(typ, "polygon"):
		return 2, true
	case geoEqualFold(typ, "multipolygon"):
		return 3, true
	}
	return 0, false
}

// geoBBoxFast computes a plain geometry's bounding box straight from its text,
// without decoding it. It reads every position's first two numbers, exactly as
// collectGeoBBox does, and declines (ok=false) for any structure or type the
// slow route should handle, and for a geometry without positions.
func geoBBoxFast(text string) (geoEditBBox, bool) {
	typ, coords, ok := geoFastHeader(text)
	if !ok {
		return geoEditBBox{}, false
	}
	depth, ok := geoFastDepth(typ)
	if !ok {
		return geoEditBBox{}, false
	}
	s := fastJSON{b: coords}
	var bbox geoEditBBox
	if !s.walkPositions(depth, &bbox) || s.i != len(s.b) || !bbox.Set {
		return geoEditBBox{}, false
	}
	return bbox, true
}

// walkPositions visits the positions of one coordinates value, adding each to
// bbox. A position needs at least two plain numbers; further ones are ignored.
func (s *fastJSON) walkPositions(depth int, bbox *geoEditBBox) bool {
	if s.i >= len(s.b) || s.b[s.i] != '[' {
		return false
	}
	s.i++
	s.skipSpace()
	if depth == 0 {
		var xy [2]float64
		for n := 0; ; n++ {
			s.skipSpace()
			v, ok := s.number()
			if !ok {
				return false
			}
			if n < 2 {
				xy[n] = v
			}
			s.skipSpace()
			if s.i >= len(s.b) {
				return false
			}
			switch s.b[s.i] {
			case ',':
				s.i++
			case ']':
				s.i++
				if n < 1 {
					return false
				}
				bbox.add(geoEditPoint{X: xy[0], Y: xy[1]})
				return true
			default:
				return false
			}
		}
	}
	if s.i < len(s.b) && s.b[s.i] == ']' {
		s.i++
		return true
	}
	for {
		s.skipSpace()
		if !s.walkPositions(depth-1, bbox) {
			return false
		}
		s.skipSpace()
		if s.i >= len(s.b) {
			return false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			return true
		default:
			return false
		}
	}
}

// ── WKT without an intermediate value ──────────────────────────────────────

// appendWKTFloat is wktFormatFloat appending to buf.
func appendWKTFloat(buf []byte, v float64) []byte {
	if abs := math.Abs(v); abs != 0 && (abs < 1e-6 || abs >= 1e15) {
		return strconv.AppendFloat(buf, v, 'g', -1, 64)
	}
	return strconv.AppendFloat(buf, v, 'f', -1, 64)
}

// geoWKTFromTextFast writes the WKT of a plain geometry straight from its
// GeoJSON text, byte for byte what geoJSONToWKT produces for the decoded
// object, and reports ok=false for anything the slow route should handle.
func geoWKTFromTextFast(text string) (string, bool) {
	typ, coords, ok := geoFastHeader(text)
	if !ok {
		return "", false
	}
	depth, ok := geoFastDepth(typ)
	if !ok {
		return "", false
	}
	// First pass: the structure must be well formed, and any third ordinate
	// makes the whole geometry 3D.
	probe := fastJSON{b: coords}
	hasZ, positions, ok := probe.survey(depth)
	if !ok || probe.i != len(probe.b) {
		return "", false
	}
	buf := make([]byte, 0, len(text))
	buf = append(buf, strings.ToUpper(typ)...)
	if hasZ {
		buf = append(buf, " Z"...)
	}
	if isEmptyCoordinates(coords) {
		return string(append(buf, " EMPTY"...)), true
	}
	_ = positions
	s := fastJSON{b: coords}
	if depth == 0 {
		buf = append(buf, '(')
		buf, ok = s.writeWKTPosition(buf, hasZ)
		buf = append(buf, ')')
	} else {
		buf, ok = s.writeWKTGroup(buf, depth, hasZ)
	}
	if !ok {
		return "", false
	}
	return string(buf), true
}

// isEmptyCoordinates reports whether the coordinates text is an empty array.
func isEmptyCoordinates(coords string) bool {
	i := 1
	for i < len(coords) && (coords[i] == ' ' || coords[i] == '\t' || coords[i] == '\n' || coords[i] == '\r') {
		i++
	}
	return i < len(coords) && coords[i] == ']'
}

// survey walks one coordinates value, checking that positions of at least two
// plain numbers sit exactly depth array levels down, and reports whether any has
// a third ordinate.
func (s *fastJSON) survey(depth int) (hasZ bool, positions int, ok bool) {
	if s.i >= len(s.b) || s.b[s.i] != '[' {
		return false, 0, false
	}
	s.i++
	s.skipSpace()
	if depth == 0 {
		for n := 0; ; n++ {
			s.skipSpace()
			if _, ok := s.number(); !ok {
				return false, 0, false
			}
			s.skipSpace()
			if s.i >= len(s.b) {
				return false, 0, false
			}
			switch s.b[s.i] {
			case ',':
				s.i++
			case ']':
				s.i++
				return n >= 2, 1, n >= 1
			default:
				return false, 0, false
			}
		}
	}
	if s.i < len(s.b) && s.b[s.i] == ']' {
		s.i++
		return false, 0, true
	}
	for {
		s.skipSpace()
		z, n, ok := s.survey(depth - 1)
		if !ok {
			return false, 0, false
		}
		hasZ = hasZ || z
		positions += n
		s.skipSpace()
		if s.i >= len(s.b) {
			return false, 0, false
		}
		switch s.b[s.i] {
		case ',':
			s.i++
		case ']':
			s.i++
			return hasZ, positions, true
		default:
			return false, 0, false
		}
	}
}

// writeWKTNumber appends the number at s.i the way wktFormatFloat prints it.
func (s *fastJSON) writeWKTNumber(buf []byte) ([]byte, bool) {
	text, f, verbatim, ok := s.scanNumberText()
	if !ok {
		return buf, false
	}
	if verbatim {
		return append(buf, text...), true // already the shortest form WKT prints
	}
	return appendWKTFloat(buf, f), true
}

// writeWKTPosition writes one position as "x y" or "x y z"; a position that has
// no z in a 3D geometry gets 0.
func (s *fastJSON) writeWKTPosition(buf []byte, hasZ bool) ([]byte, bool) {
	s.i++ // '['
	var ok bool
	s.skipSpace()
	if buf, ok = s.writeWKTNumber(buf); !ok {
		return buf, false
	}
	s.skipSpace()
	s.i++ // ','
	buf = append(buf, ' ')
	s.skipSpace()
	if buf, ok = s.writeWKTNumber(buf); !ok {
		return buf, false
	}
	wroteZ := false
	for {
		s.skipSpace()
		if s.i >= len(s.b) {
			return buf, false
		}
		if s.b[s.i] == ']' {
			s.i++
			break
		}
		s.i++ // ','
		s.skipSpace()
		if !wroteZ && hasZ {
			buf = append(buf, ' ')
			if buf, ok = s.writeWKTNumber(buf); !ok {
				return buf, false
			}
			wroteZ = true
			continue
		}
		// A further ordinate is parsed and ignored.
		if _, ok := s.number(); !ok {
			return buf, false
		}
	}
	if hasZ && !wroteZ {
		buf = append(buf, " 0"...)
	}
	return buf, true
}

// writeWKTGroup writes a parenthesized group depth levels above the positions.
func (s *fastJSON) writeWKTGroup(buf []byte, depth int, hasZ bool) ([]byte, bool) {
	s.i++ // '['
	buf = append(buf, '(')
	s.skipSpace()
	if s.i < len(s.b) && s.b[s.i] == ']' {
		s.i++
		return append(buf, ')'), true
	}
	for first := true; ; first = false {
		if !first {
			buf = append(buf, ',')
		}
		s.skipSpace()
		var ok bool
		if depth == 1 {
			buf, ok = s.writeWKTPosition(buf, hasZ)
		} else {
			buf, ok = s.writeWKTGroup(buf, depth-1, hasZ)
		}
		if !ok {
			return buf, false
		}
		s.skipSpace()
		if s.i >= len(s.b) {
			return buf, false
		}
		if s.b[s.i] == ',' {
			s.i++
			continue
		}
		s.i++ // ']'
		return append(buf, ')'), true
	}
}
