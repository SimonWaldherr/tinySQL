// WKT (Well-Known Text) input and output for the GeoJSON geometries every
// other geo_*.go file operates on. tinySQL stores geometry as GeoJSON text
// (see canonicalGeoJSON in geo_functions.go), so these functions are pure
// converters at the edges: ST_GEOMFROMTEXT parses foreign text *into* that
// representation and ST_ASTEXT renders it back out. Nothing in between ever
// sees WKT, which is why there is no WKT-native code path anywhere else --
// adding one would mean maintaining every predicate twice.
//
// Scope notes, all deliberate:
//   - EWKT's "SRID=4326;" prefix is accepted on input. Since tinySQL's geo
//     layer is WGS84 lon/lat throughout, a non-WGS84 SRID is rejected with a
//     pointer to ST_TRANSFORM rather than silently relabelled -- see
//     geoRequireWGS84SRID.
//   - The M (measure) ordinate is parsed and discarded: a GeoJSON position is
//     [lon, lat] or [lon, lat, z] and has nowhere to put it. Z is preserved
//     end to end.
//   - "EMPTY" round-trips through GeoJSON's empty-coordinates form
//     ({"type":"Point","coordinates":[]}). That form is deliberately *not*
//     accepted by a GEOMETRY column (validateGeometryShape requires a real
//     position), so an empty geometry can be read, inspected and re-rendered
//     but not stored. Erroring out mid-import on a single EMPTY row -- which
//     real-world WKT dumps do contain -- would be the worse trade.
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// geoWKTMaxInputBytes bounds a single WKT string. The parser allocates
// proportionally to its input, and unlike a GEOMETRY column value (capped by
// coerceToGeometry) a function argument can be an arbitrarily long literal
// or concatenation, so the bound lives here.
const geoWKTMaxInputBytes = 64 << 20

func getGeoWKTFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_FROM_WKT":       evalGeoFromWKT,
		"ST_GEOMFROMTEXT":    evalGeoFromWKT,
		"ST_GEOMFROMEWKT":    evalGeoFromWKT,
		"ST_GEOMFROMWKT":     evalGeoFromWKT,
		"ST_POINTFROMTEXT":   evalGeoFromWKT,
		"ST_LINEFROMTEXT":    evalGeoFromWKT,
		"ST_POLYGONFROMTEXT": evalGeoFromWKT,
		"GEO_AS_WKT":         evalGeoAsWKT,
		"ST_ASTEXT":          evalGeoAsWKT,
		"GEO_AS_EWKT":        evalGeoAsEWKT,
		"ST_ASEWKT":          evalGeoAsEWKT,
		"GEO_FROM_GEOJSON":   evalGeoFromGeoJSON,
		"ST_GEOMFROMGEOJSON": evalGeoFromGeoJSON,
		"GEO_AS_GEOJSON":     evalGeoAsGeoJSON,
		"ST_ASGEOJSON":       evalGeoAsGeoJSON,
	}
}

// geoWGS84SRID is the only spatial reference tinySQL's lon/lat math is
// correct for. 0 ("unknown") is accepted as a synonym, the same latitude
// PostGIS gives an unset SRID.
const geoWGS84SRID = 4326

// geoRequireWGS84SRID gates every SRID that reaches the engine from a WKT
// prefix or an explicit ST_GeomFromText(..., srid) argument. Accepting an
// arbitrary SRID and then treating the coordinates as degrees would silently
// produce wrong distances and areas, which is exactly the class of bug that
// is hardest to notice in a result set.
func geoRequireWGS84SRID(name string, srid int64) error {
	if srid == 0 || srid == geoWGS84SRID {
		return nil
	}
	return fmt.Errorf("%s: SRID %d is not supported; tinySQL geometry is WGS84 lon/lat (SRID 4326) -- use ST_TRANSFORM to reproject", name, srid)
}

// ---------------------------------------------------------------------------
// WKT parser
// ---------------------------------------------------------------------------

type wktParser struct {
	src string
	pos int
}

// wktDims tracks which optional ordinates the geometry being parsed carries.
// Once the first position has fixed the dimensionality -- either from an
// explicit "Z"/"M"/"ZM" tag or by inference from how many numbers that
// position held -- every later position must agree, so that
// "LINESTRING(0 0, 1 1 1)" is an error rather than a silently ragged
// coordinate array.
type wktDims struct {
	z     bool
	m     bool
	fixed bool
}

func (p *wktParser) errorf(format string, args ...any) error {
	return fmt.Errorf("WKT: %s (at offset %d)", fmt.Sprintf(format, args...), p.pos)
}

func (p *wktParser) skipSpace() {
	for p.pos < len(p.src) {
		switch p.src[p.pos] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			p.pos++
		default:
			return
		}
	}
}

func (p *wktParser) peek() byte {
	p.skipSpace()
	if p.pos >= len(p.src) {
		return 0
	}
	return p.src[p.pos]
}

func (p *wktParser) accept(c byte) bool {
	if p.peek() == c {
		p.pos++
		return true
	}
	return false
}

func (p *wktParser) expect(c byte) error {
	if p.accept(c) {
		return nil
	}
	return p.errorf("expected %q", string(c))
}

// word reads an uppercased run of ASCII letters, the only alphabetic tokens
// WKT has (type names, dimension tags, and EMPTY).
func (p *wktParser) word() string {
	p.skipSpace()
	start := p.pos
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			p.pos++
			continue
		}
		break
	}
	return strings.ToUpper(p.src[start:p.pos])
}

// peekWord reads a word without consuming it, for the lookahead EMPTY and
// the optional Z/M/ZM tag both need.
func (p *wktParser) peekWord() string {
	save := p.pos
	w := p.word()
	p.pos = save
	return w
}

func (p *wktParser) number() (float64, error) {
	p.skipSpace()
	start := p.pos
	if p.pos < len(p.src) && (p.src[p.pos] == '+' || p.src[p.pos] == '-') {
		p.pos++
	}
	sawDigit := false
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if c >= '0' && c <= '9' {
			sawDigit = true
			p.pos++
			continue
		}
		if c == '.' {
			p.pos++
			continue
		}
		// An exponent is only an exponent after a mantissa digit; without
		// that guard the leading 'E' of "EMPTY" would be swallowed here.
		if sawDigit && (c == 'e' || c == 'E') {
			p.pos++
			if p.pos < len(p.src) && (p.src[p.pos] == '+' || p.src[p.pos] == '-') {
				p.pos++
			}
			continue
		}
		break
	}
	if !sawDigit {
		p.pos = start
		return 0, p.errorf("expected a number")
	}
	text := p.src[start:p.pos]
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, p.errorf("invalid number %q", text)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, p.errorf("non-finite number %q", text)
	}
	return f, nil
}

// parseWKT is the entry point: an optional EWKT SRID prefix followed by one
// geometry, with nothing but whitespace after it.
func parseWKT(name, input string) (map[string]any, error) {
	if len(input) > geoWKTMaxInputBytes {
		return nil, fmt.Errorf("%s: WKT input is %d bytes, exceeding the %d byte limit", name, len(input), geoWKTMaxInputBytes)
	}
	p := &wktParser{src: input}
	if srid, ok, err := p.parseSRIDPrefix(); err != nil {
		return nil, err
	} else if ok {
		if err := geoRequireWGS84SRID(name, srid); err != nil {
			return nil, err
		}
	}
	obj, err := p.parseGeometry(0)
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.src) {
		return nil, p.errorf("unexpected trailing input %q", strings.TrimSpace(p.src[p.pos:]))
	}
	return obj, nil
}

// parseSRIDPrefix consumes an EWKT "SRID=4326;" header if present. A missing
// header is not an error (plain WKT is the common case), so the bool reports
// whether one was actually there.
func (p *wktParser) parseSRIDPrefix() (int64, bool, error) {
	save := p.pos
	if w := p.word(); w != "SRID" {
		p.pos = save
		return 0, false, nil
	}
	if !p.accept('=') {
		p.pos = save
		return 0, false, nil
	}
	f, err := p.number()
	if err != nil {
		return 0, false, err
	}
	if err := p.expect(';'); err != nil {
		return 0, false, err
	}
	if f != math.Trunc(f) {
		return 0, false, p.errorf("SRID must be an integer")
	}
	return int64(f), true, nil
}

// geoWKTMaxNesting bounds GEOMETRYCOLLECTION recursion. WKT allows a
// collection to contain collections, so a hostile or corrupt input could
// otherwise recurse until the goroutine stack is exhausted.
const geoWKTMaxNesting = 32

func (p *wktParser) parseGeometry(depth int) (map[string]any, error) {
	if depth > geoWKTMaxNesting {
		return nil, p.errorf("GEOMETRYCOLLECTION nested more than %d deep", geoWKTMaxNesting)
	}
	typeWord := p.word()
	if typeWord == "" {
		return nil, p.errorf("expected a geometry type name")
	}

	var dims wktDims
	// Both "POINT Z (...)" (the ISO/PostGIS spelling) and the "POINTZ(...)"
	// form some exporters emit are accepted.
	for _, suffix := range []string{"ZM", "Z", "M"} {
		if strings.HasSuffix(typeWord, suffix) && typeWord != suffix {
			base := strings.TrimSuffix(typeWord, suffix)
			if isWKTTypeName(base) {
				typeWord = base
				dims = wktDimsFromTag(suffix)
			}
			break
		}
	}
	if !dims.fixed {
		if tag := p.peekWord(); tag == "Z" || tag == "M" || tag == "ZM" {
			p.word()
			dims = wktDimsFromTag(tag)
		}
	}
	if !isWKTTypeName(typeWord) {
		return nil, p.errorf("unknown geometry type %q", typeWord)
	}

	geoJSONType := wktTypeNames[typeWord]
	if p.peekWord() == "EMPTY" {
		p.word()
		if geoJSONType == "GeometryCollection" {
			return map[string]any{"type": geoJSONType, "geometries": []any{}}, nil
		}
		return map[string]any{"type": geoJSONType, "coordinates": []any{}}, nil
	}

	if err := p.expect('('); err != nil {
		return nil, err
	}
	var coords any
	var err error
	switch typeWord {
	case "POINT":
		var pos []any
		pos, err = p.parsePosition(&dims)
		coords = pos
	case "LINESTRING", "CIRCULARSTRING":
		coords, err = p.parsePositionList(&dims)
	case "MULTIPOINT":
		coords, err = p.parseMultiPointBody(&dims)
	case "POLYGON", "MULTILINESTRING", "TRIANGLE":
		coords, err = p.parsePositionListList(&dims)
	case "MULTIPOLYGON":
		coords, err = p.parsePositionListListList(&dims)
	case "GEOMETRYCOLLECTION":
		var geoms []any
		geoms, err = p.parseGeometryList(depth)
		if err != nil {
			return nil, err
		}
		if err := p.expect(')'); err != nil {
			return nil, err
		}
		return map[string]any{"type": geoJSONType, "geometries": geoms}, nil
	default:
		return nil, p.errorf("unsupported geometry type %q", typeWord)
	}
	if err != nil {
		return nil, err
	}
	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return map[string]any{"type": geoJSONType, "coordinates": coords}, nil
}

// wktTypeNames maps every accepted WKT type keyword to the GeoJSON type it
// becomes. CIRCULARSTRING and TRIANGLE are mapped to their closest linear
// GeoJSON equivalents rather than rejected: GeoJSON has no curve type, so a
// CIRCULARSTRING's control points become a LineString's vertices. That is a
// lossy but predictable reading, and it is what makes an OGC-flavoured dump
// importable at all.
var wktTypeNames = map[string]string{
	"POINT":              "Point",
	"LINESTRING":         "LineString",
	"POLYGON":            "Polygon",
	"MULTIPOINT":         "MultiPoint",
	"MULTILINESTRING":    "MultiLineString",
	"MULTIPOLYGON":       "MultiPolygon",
	"GEOMETRYCOLLECTION": "GeometryCollection",
	"CIRCULARSTRING":     "LineString",
	"TRIANGLE":           "Polygon",
}

func isWKTTypeName(w string) bool {
	_, ok := wktTypeNames[w]
	return ok
}

func wktDimsFromTag(tag string) wktDims {
	switch tag {
	case "Z":
		return wktDims{z: true, fixed: true}
	case "M":
		return wktDims{m: true, fixed: true}
	case "ZM":
		return wktDims{z: true, m: true, fixed: true}
	}
	return wktDims{}
}

// parsePosition reads one bare "x y [z] [m]" tuple and returns it as a
// GeoJSON position. On the first position of an untagged geometry the
// ordinate count decides the dimensionality for the whole geometry.
func (p *wktParser) parsePosition(dims *wktDims) ([]any, error) {
	nums := make([]float64, 0, 4)
	for len(nums) < 4 {
		save := p.pos
		f, err := p.number()
		if err != nil {
			p.pos = save
			break
		}
		nums = append(nums, f)
	}
	if len(nums) < 2 {
		return nil, p.errorf("a position needs at least an x and a y ordinate")
	}
	if !dims.fixed {
		switch len(nums) {
		case 3:
			dims.z = true
		case 4:
			dims.z, dims.m = true, true
		}
		dims.fixed = true
	}
	want := 2
	if dims.z {
		want++
	}
	if dims.m {
		want++
	}
	if len(nums) != want {
		return nil, p.errorf("position has %d ordinates, expected %d for this geometry's dimensionality", len(nums), want)
	}
	out := []any{nums[0], nums[1]}
	if dims.z {
		out = append(out, nums[2])
	}
	// nums[3] (or nums[2] for an XYM geometry) is the M ordinate; GeoJSON has
	// no slot for it, so it is dropped here rather than smuggled into Z.
	return out, nil
}

func (p *wktParser) parsePositionList(dims *wktDims) ([]any, error) {
	out := []any{}
	for {
		pos, err := p.parsePosition(dims)
		if err != nil {
			return nil, err
		}
		out = append(out, pos)
		if !p.accept(',') {
			return out, nil
		}
	}
}

// parseMultiPointBody accepts both MULTIPOINT spellings the standard allows:
// the bare "MULTIPOINT(10 40, 40 30)" and the parenthesized
// "MULTIPOINT((10 40), (40 30))". Real data contains both.
func (p *wktParser) parseMultiPointBody(dims *wktDims) ([]any, error) {
	if p.peek() != '(' {
		return p.parsePositionList(dims)
	}
	out := []any{}
	for {
		if err := p.expect('('); err != nil {
			return nil, err
		}
		pos, err := p.parsePosition(dims)
		if err != nil {
			return nil, err
		}
		if err := p.expect(')'); err != nil {
			return nil, err
		}
		out = append(out, pos)
		if !p.accept(',') {
			return out, nil
		}
	}
}

func (p *wktParser) parsePositionListList(dims *wktDims) ([]any, error) {
	out := []any{}
	for {
		if err := p.expect('('); err != nil {
			return nil, err
		}
		inner, err := p.parsePositionList(dims)
		if err != nil {
			return nil, err
		}
		if err := p.expect(')'); err != nil {
			return nil, err
		}
		out = append(out, inner)
		if !p.accept(',') {
			return out, nil
		}
	}
}

func (p *wktParser) parsePositionListListList(dims *wktDims) ([]any, error) {
	out := []any{}
	for {
		if err := p.expect('('); err != nil {
			return nil, err
		}
		inner, err := p.parsePositionListList(dims)
		if err != nil {
			return nil, err
		}
		if err := p.expect(')'); err != nil {
			return nil, err
		}
		out = append(out, inner)
		if !p.accept(',') {
			return out, nil
		}
	}
}

func (p *wktParser) parseGeometryList(depth int) ([]any, error) {
	out := []any{}
	for {
		g, err := p.parseGeometry(depth + 1)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
		if !p.accept(',') {
			return out, nil
		}
	}
}

// ---------------------------------------------------------------------------
// WKT writer
// ---------------------------------------------------------------------------

// geoJSONToWKT renders a GeoJSON geometry object as WKT. Dimensionality is
// decided per geometry by scanning it once: if any position carries a Z the
// whole geometry is written 3D (with 0 filled in for positions that lack
// one), matching how WKT models dimension as a property of the geometry
// rather than of the individual vertex.
func geoJSONToWKT(object map[string]any) (string, error) {
	var b strings.Builder
	if err := writeWKTGeometry(&b, object, 0); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeWKTGeometry(b *strings.Builder, object map[string]any, depth int) error {
	if depth > geoWKTMaxNesting {
		return fmt.Errorf("GEOMETRYCOLLECTION nested more than %d deep", geoWKTMaxNesting)
	}
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "point", "multipoint", "linestring", "multilinestring", "polygon", "multipolygon":
	case "geometrycollection":
		geoms, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		hasZ, err := geoJSONHasZ(object)
		if err != nil {
			return err
		}
		b.WriteString("GEOMETRYCOLLECTION")
		if hasZ {
			b.WriteString(" Z")
		}
		if len(geoms) == 0 {
			b.WriteString(" EMPTY")
			return nil
		}
		b.WriteByte('(')
		for i, g := range geoms {
			if i > 0 {
				b.WriteByte(',')
			}
			child, ok := g.(map[string]any)
			if !ok {
				return fmt.Errorf("geometry %d: expected a GeoJSON geometry object", i)
			}
			if err := writeWKTGeometry(b, child, depth+1); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
		}
		b.WriteByte(')')
		return nil
	case "feature", "featurecollection":
		return fmt.Errorf("WKT has no %s; extract .geometry first", typ)
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}

	coords := object["coordinates"]
	hasZ, err := geoJSONHasZ(object)
	if err != nil {
		return err
	}
	b.WriteString(strings.ToUpper(wktKeywordFor(typ)))
	if hasZ {
		b.WriteString(" Z")
	}
	if geoJSONCoordinatesEmpty(coords) {
		b.WriteString(" EMPTY")
		return nil
	}
	// Nesting depth of the coordinates array, per GeoJSON: a Point is a bare
	// position, MultiPoint/LineString a list of them, and so on.
	nest := map[string]int{
		"point": 0, "multipoint": 1, "linestring": 1,
		"multilinestring": 2, "polygon": 2, "multipolygon": 3,
	}[strings.ToLower(typ)]
	if nest == 0 {
		// A Point's own coordinates are a single bare position with no
		// array wrapping it in GeoJSON, but WKT still always wraps the
		// geometry's payload in one pair of parens ("POINT(30 10)", not
		// "POINT30 10") -- every other nest level gets that same pair from
		// the recursive call one level up in writeWKTCoords, but nest 0 has
		// no such call, so it is added here instead.
		b.WriteByte('(')
		if err := writeWKTCoords(b, coords, 0, hasZ); err != nil {
			return err
		}
		b.WriteByte(')')
		return nil
	}
	return writeWKTCoords(b, coords, nest, hasZ)
}

func wktKeywordFor(geoJSONType string) string {
	switch strings.ToLower(geoJSONType) {
	case "point":
		return "POINT"
	case "multipoint":
		return "MULTIPOINT"
	case "linestring":
		return "LINESTRING"
	case "multilinestring":
		return "MULTILINESTRING"
	case "polygon":
		return "POLYGON"
	case "multipolygon":
		return "MULTIPOLYGON"
	case "geometrycollection":
		return "GEOMETRYCOLLECTION"
	}
	return strings.ToUpper(geoJSONType)
}

// writeWKTCoords emits a coordinates value nest levels deep. nest == 0 is a
// bare position, written without its own parentheses (WKT puts the outer
// parens on the geometry, not the position).
func writeWKTCoords(b *strings.Builder, value any, nest int, hasZ bool) error {
	if nest == 0 {
		p, err := geoPositionFromValue(value)
		if err != nil {
			return err
		}
		b.WriteString(wktFormatFloat(p.Lon))
		b.WriteByte(' ')
		b.WriteString(wktFormatFloat(p.Lat))
		if hasZ {
			b.WriteByte(' ')
			z := 0.0
			if p.Z != nil {
				z = *p.Z
			}
			b.WriteString(wktFormatFloat(z))
		}
		return nil
	}
	items, ok := value.([]any)
	if !ok {
		return fmt.Errorf("expected a coordinates array, got %T", value)
	}
	b.WriteByte('(')
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		if err := writeWKTCoords(b, item, nest-1, hasZ); err != nil {
			return err
		}
	}
	b.WriteByte(')')
	return nil
}

// wktFormatFloat prints the shortest decimal that reparses to exactly v.
// Plain (non-exponent) notation is used across the whole range real
// coordinates occupy; the exponent form only kicks in for magnitudes where
// 'f' would emit an absurd number of digits.
func wktFormatFloat(v float64) string {
	abs := math.Abs(v)
	if abs != 0 && (abs < 1e-6 || abs >= 1e15) {
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// geoJSONCoordinatesEmpty reports the GeoJSON spelling of an empty geometry:
// a missing or zero-length coordinates array.
func geoJSONCoordinatesEmpty(v any) bool {
	if v == nil {
		return true
	}
	arr, ok := v.([]any)
	return ok && len(arr) == 0
}

// geoJSONHasZ reports whether any position anywhere in the geometry carries a
// third ordinate, which is what decides the WKT/WKB dimensionality tag.
func geoJSONHasZ(object map[string]any) (bool, error) {
	found := false
	if err := walkGeoJSONPositions(object, func(pos []any) error {
		if len(pos) > 2 {
			if _, err := geoFloat(pos[2]); err == nil {
				found = true
			}
		}
		return nil
	}); err != nil {
		return false, err
	}
	return found, nil
}

// walkGeoJSONPositions calls visit for every position in a geometry,
// descending through GeometryCollections. It is the shared traversal the WKT
// and WKB writers both need and is kept separate from geo_editing.go's
// walkGeoCoordinateValue, which yields decoded simplifyCoordinate values and
// does not handle GeometryCollection.
func walkGeoJSONPositions(object map[string]any, visit func(pos []any) error) error {
	typ, _ := object["type"].(string)
	lower := strings.ToLower(typ)
	if lower == "geometrycollection" {
		geoms, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		for i, g := range geoms {
			child, ok := g.(map[string]any)
			if !ok {
				return fmt.Errorf("geometry %d: expected a GeoJSON geometry object", i)
			}
			if err := walkGeoJSONPositions(child, visit); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
		}
		return nil
	}
	nest, ok := map[string]int{
		"point": 0, "multipoint": 1, "linestring": 1,
		"multilinestring": 2, "polygon": 2, "multipolygon": 3,
	}[lower]
	if !ok {
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
	return walkGeoJSONPositionsNested(object["coordinates"], nest, visit)
}

func walkGeoJSONPositionsNested(value any, nest int, visit func(pos []any) error) error {
	if value == nil {
		return nil
	}
	arr, ok := value.([]any)
	if !ok {
		return fmt.Errorf("expected a coordinates array, got %T", value)
	}
	if nest == 0 {
		return visit(arr)
	}
	for _, item := range arr {
		if err := walkGeoJSONPositionsNested(item, nest-1, visit); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// SQL functions
// ---------------------------------------------------------------------------

// evalGeoTextArg pulls a string argument, accepting the []byte and
// json.RawMessage forms a column value can arrive in.
func evalGeoTextArg(env ExecEnv, ex *FuncCall, row Row, idx int) (string, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return "", err
	}
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case json.RawMessage:
		return string(x), nil
	default:
		return "", fmt.Errorf("%s arg%d: expected text, got %T", ex.Name, idx+1, v)
	}
}

func evalGeoFromWKT(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 2); err != nil {
		return nil, err
	}
	text, err := evalGeoTextArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	if len(ex.Args) == 2 {
		srid, err := evalGeoFloatArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
		if srid != math.Trunc(srid) {
			return nil, fmt.Errorf("%s: SRID must be an integer", ex.Name)
		}
		if err := geoRequireWGS84SRID(ex.Name, int64(srid)); err != nil {
			return nil, err
		}
	}
	obj, err := parseWKT(ex.Name, text)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	// The typed aliases (ST_POINTFROMTEXT and friends) exist for query
	// portability; PostGIS has them return NULL on a type mismatch, but this
	// engine's geo layer reports bad input as an error everywhere else, so
	// they do too.
	if want, ok := wktAliasExpectedType[ex.Name]; ok {
		if got, _ := obj["type"].(string); !strings.EqualFold(got, want) {
			return nil, fmt.Errorf("%s: expected a %s, got %s", ex.Name, want, got)
		}
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}

// wktAliasExpectedType constrains the OGC-style typed constructors. The
// untyped names (ST_GEOMFROMTEXT, GEO_FROM_WKT) are absent and accept
// anything.
var wktAliasExpectedType = map[string]string{
	"ST_POINTFROMTEXT":   "Point",
	"ST_LINEFROMTEXT":    "LineString",
	"ST_POLYGONFROMTEXT": "Polygon",
}

func evalGeoAsWKT(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	obj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	wkt, err := geoJSONToWKT(obj)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return wkt, nil
}

func evalGeoAsEWKT(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 2); err != nil {
		return nil, err
	}
	obj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	srid := int64(geoWGS84SRID)
	if len(ex.Args) == 2 {
		f, err := evalGeoFloatArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
		if f != math.Trunc(f) {
			return nil, fmt.Errorf("%s: SRID must be an integer", ex.Name)
		}
		if err := geoRequireWGS84SRID(ex.Name, int64(f)); err != nil {
			return nil, err
		}
		srid = int64(f)
	}
	wkt, err := geoJSONToWKT(obj)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return fmt.Sprintf("SRID=%d;%s", srid, wkt), nil
}

// evalGeoObjectArg is the shared "decode argument idx as a GeoJSON geometry
// object" step for every function in the newer geo files. It differs from
// evalGeoPolygonArg/evalGeoLineStringArg only in not constraining the type.
func evalGeoObjectArg(env ExecEnv, ex *FuncCall, row Row, idx int) (map[string]any, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return nil, err
	}
	obj, err := geoObjectFromValue(v)
	if err != nil {
		return nil, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return obj, nil
}

func evalGeoFromGeoJSON(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	out, err := canonicalGeoJSON(v)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return out, nil
}

// evalGeoAsGeoJSON canonicalizes a geometry to stable, sorted-key text, with
// an optional coordinate rounding step (PostGIS's maxdecimaldigits). Rounding
// is applied to the coordinates only, never to any other member.
func evalGeoAsGeoJSON(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 2); err != nil {
		return nil, err
	}
	obj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	if err := validateGeometryShape(obj); err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if len(ex.Args) == 2 {
		digits, err := evalGeoFloatArg(env, ex, row, 1)
		if err != nil {
			return nil, err
		}
		if digits != math.Trunc(digits) || digits < 0 || digits > 15 {
			return nil, fmt.Errorf("%s: max decimal digits must be an integer in 0..15", ex.Name)
		}
		obj = geoDeepCloneObject(obj)
		scale := math.Pow(10, digits)
		if err := walkGeoJSONPositions(obj, func(pos []any) error {
			for i := range pos {
				f, err := geoFloat(pos[i])
				if err != nil {
					return err
				}
				pos[i] = math.Round(f*scale) / scale
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}
