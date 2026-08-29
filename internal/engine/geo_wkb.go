// WKB (Well-Known Binary) input and output, the binary sibling of
// geo_wkt.go's text format. Same role: a converter at the edges between
// tinySQL's GeoJSON-text storage and a foreign wire format, here one that
// typically arrives as a BLOB (e.g. a value read out of another database's
// geometry column, or produced by a client library's ST_AsBinary).
//
// Only the standard, uncompressed OGC WKB encoding is implemented: 1 byte
// byte-order marker, 4-byte little/big-endian geometry type code (optionally
// OR'd with the EWKB 0x80000000 Z-flag and 0x20000000 SRID-flag bits), an
// optional EWKB SRID, then coordinates. This covers what every mainstream
// spatial database (PostGIS, SpatiaLite, SQL Server) emits; the ISO SQL/MM
// variant's differently-numbered Z/M/ZM type codes are not handled.
package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
)

func getGeoWKBFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_FROM_WKB":    evalGeoFromWKB,
		"ST_GEOMFROMWKB":  evalGeoFromWKB,
		"ST_GEOMFROMEWKB": evalGeoFromWKB,
		"GEO_AS_WKB":      evalGeoAsWKB,
		"ST_ASBINARY":     evalGeoAsWKB,
		"GEO_AS_EWKB":     evalGeoAsEWKB,
		"ST_ASEWKB":       evalGeoAsEWKB,
	}
}

// wkbGeometryType is the OGC WKB type code, independent of any Z/SRID flag
// bits the 4-byte type field may also carry.
type wkbGeometryType uint32

const (
	wkbPoint              wkbGeometryType = 1
	wkbLineString         wkbGeometryType = 2
	wkbPolygon            wkbGeometryType = 3
	wkbMultiPoint         wkbGeometryType = 4
	wkbMultiLineString    wkbGeometryType = 5
	wkbMultiPolygon       wkbGeometryType = 6
	wkbGeometryCollection wkbGeometryType = 7
)

// EWKB's high-bit flags on the 4-byte type field (PostGIS's extension over
// plain OGC WKB, not to be confused with the ISO SQL/MM +1000/+2000/+3000
// numbering, which this reader does not need to special-case since it masks
// down to the low byte before switching).
const (
	ewkbZFlag    uint32 = 0x80000000
	ewkbMFlag    uint32 = 0x40000000
	ewkbSRIDFlag uint32 = 0x20000000
	wkbTypeMask  uint32 = 0x000000ff
)

type wkbReader struct {
	data []byte
	pos  int
	ord  binary.ByteOrder
}

func (r *wkbReader) errorf(format string, args ...any) error {
	return fmt.Errorf("WKB: %s (at byte %d)", fmt.Sprintf(format, args...), r.pos)
}

func (r *wkbReader) byte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, r.errorf("unexpected end of input")
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *wkbReader) uint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, r.errorf("unexpected end of input")
	}
	v := r.ord.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *wkbReader) float64() (float64, error) {
	if r.pos+8 > len(r.data) {
		return 0, r.errorf("unexpected end of input")
	}
	bits := r.ord.Uint64(r.data[r.pos:])
	r.pos += 8
	f := math.Float64frombits(bits)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, r.errorf("non-finite coordinate")
	}
	return f, nil
}

// geoWKBMaxNesting mirrors geoWKTMaxNesting: bounds GeometryCollection
// recursion against a hostile or corrupt input.
const geoWKBMaxNesting = 32

// parseWKB decodes name's argument bytes as WKB or EWKB into a GeoJSON
// geometry object.
func parseWKB(name string, data []byte) (map[string]any, error) {
	if len(data) > geoWKTMaxInputBytes {
		return nil, fmt.Errorf("%s: WKB input is %d bytes, exceeding the %d byte limit", name, len(data), geoWKTMaxInputBytes)
	}
	r := &wkbReader{data: data}
	obj, err := r.readGeometry(0)
	if err != nil {
		return nil, err
	}
	if r.pos != len(r.data) {
		return nil, r.errorf("%d trailing byte(s) after geometry", len(r.data)-r.pos)
	}
	return obj, nil
}

func (r *wkbReader) readGeometry(depth int) (map[string]any, error) {
	if depth > geoWKBMaxNesting {
		return nil, r.errorf("GeometryCollection nested more than %d deep", geoWKBMaxNesting)
	}
	order, err := r.byte()
	if err != nil {
		return nil, err
	}
	switch order {
	case 0:
		r.ord = binary.BigEndian
	case 1:
		r.ord = binary.LittleEndian
	default:
		return nil, r.errorf("invalid byte order marker 0x%02x", order)
	}

	rawType, err := r.uint32()
	if err != nil {
		return nil, err
	}
	hasZ := rawType&ewkbZFlag != 0
	hasSRID := rawType&ewkbSRIDFlag != 0
	geomType := wkbGeometryType(rawType & wkbTypeMask)
	// The ISO SQL/MM numbering (Point=1001/2001/3001 for Z/M/ZM instead of a
	// flag bit) is folded down the same way: 1000s and 100s digits both
	// encode a Z/M/ZM variant of the low three digits' base type.
	if geomType > 100 {
		hasZ = hasZ || (uint32(geomType)/1000) >= 1
		geomType = wkbGeometryType(uint32(geomType) % 1000)
	}

	if hasSRID {
		srid, err := r.uint32()
		if err != nil {
			return nil, err
		}
		if err := geoRequireWGS84SRID("WKB", int64(srid)); err != nil {
			return nil, err
		}
	}

	switch geomType {
	case wkbPoint:
		pos, err := r.readPosition(hasZ)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "Point", "coordinates": pos}, nil
	case wkbLineString:
		coords, err := r.readPositionList(hasZ)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "LineString", "coordinates": coords}, nil
	case wkbPolygon:
		coords, err := r.readPositionListList(hasZ)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "Polygon", "coordinates": coords}, nil
	case wkbMultiPoint:
		return r.readWKBCollectionAsCoords("MultiPoint", depth, hasZ, func(child map[string]any) (any, error) {
			if t, _ := child["type"].(string); t != "Point" {
				return nil, r.errorf("MultiPoint member must be a Point, got %s", t)
			}
			return child["coordinates"], nil
		})
	case wkbMultiLineString:
		return r.readWKBCollectionAsCoords("MultiLineString", depth, hasZ, func(child map[string]any) (any, error) {
			if t, _ := child["type"].(string); t != "LineString" {
				return nil, r.errorf("MultiLineString member must be a LineString, got %s", t)
			}
			return child["coordinates"], nil
		})
	case wkbMultiPolygon:
		return r.readWKBCollectionAsCoords("MultiPolygon", depth, hasZ, func(child map[string]any) (any, error) {
			if t, _ := child["type"].(string); t != "Polygon" {
				return nil, r.errorf("MultiPolygon member must be a Polygon, got %s", t)
			}
			return child["coordinates"], nil
		})
	case wkbGeometryCollection:
		count, err := r.uint32()
		if err != nil {
			return nil, err
		}
		geoms := make([]any, 0, count)
		for i := uint32(0); i < count; i++ {
			child, err := r.readGeometry(depth + 1)
			if err != nil {
				return nil, err
			}
			geoms = append(geoms, child)
		}
		return map[string]any{"type": "GeometryCollection", "geometries": geoms}, nil
	default:
		return nil, r.errorf("unsupported WKB geometry type code %d", geomType)
	}
}

// readWKBCollectionAsCoords reads a Multi* geometry, which WKB encodes as a
// count followed by that many full sub-geometries (each with its own byte
// order and type header), and re-projects each parsed child down to just
// its coordinates via extract -- the shape a GeoJSON Multi* needs.
func (r *wkbReader) readWKBCollectionAsCoords(name string, depth int, _ bool, extract func(map[string]any) (any, error)) (map[string]any, error) {
	count, err := r.uint32()
	if err != nil {
		return nil, err
	}
	coords := make([]any, 0, count)
	for i := uint32(0); i < count; i++ {
		child, err := r.readGeometry(depth + 1)
		if err != nil {
			return nil, err
		}
		c, err := extract(child)
		if err != nil {
			return nil, err
		}
		coords = append(coords, c)
	}
	return map[string]any{"type": name, "coordinates": coords}, nil
}

func (r *wkbReader) readPosition(hasZ bool) ([]any, error) {
	x, err := r.float64()
	if err != nil {
		return nil, err
	}
	y, err := r.float64()
	if err != nil {
		return nil, err
	}
	pos := []any{x, y}
	if hasZ {
		z, err := r.float64()
		if err != nil {
			return nil, err
		}
		pos = append(pos, z)
	}
	return pos, nil
}

func (r *wkbReader) readPositionList(hasZ bool) ([]any, error) {
	count, err := r.uint32()
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, count)
	for i := uint32(0); i < count; i++ {
		pos, err := r.readPosition(hasZ)
		if err != nil {
			return nil, err
		}
		out = append(out, pos)
	}
	return out, nil
}

func (r *wkbReader) readPositionListList(hasZ bool) ([]any, error) {
	count, err := r.uint32()
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, count)
	for i := uint32(0); i < count; i++ {
		ring, err := r.readPositionList(hasZ)
		if err != nil {
			return nil, err
		}
		out = append(out, ring)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// WKB writer
// ---------------------------------------------------------------------------

// geoJSONToWKB renders a GeoJSON geometry as little-endian WKB (optionally
// EWKB, with an SRID header) into raw bytes.
func geoJSONToWKB(object map[string]any, ewkb bool, srid int64) ([]byte, error) {
	var buf []byte
	if err := appendWKBGeometry(&buf, object, ewkb, srid, true, 0); err != nil {
		return nil, err
	}
	return buf, nil
}

func appendWKBGeometry(buf *[]byte, object map[string]any, ewkb bool, srid int64, topLevel bool, depth int) error {
	if depth > geoWKBMaxNesting {
		return fmt.Errorf("GeometryCollection nested more than %d deep", geoWKBMaxNesting)
	}
	typ, _ := object["type"].(string)
	hasZ, err := geoJSONHasZ(object)
	if err != nil {
		return err
	}

	var code wkbGeometryType
	switch typ {
	case "Point":
		code = wkbPoint
	case "LineString":
		code = wkbLineString
	case "Polygon":
		code = wkbPolygon
	case "MultiPoint":
		code = wkbMultiPoint
	case "MultiLineString":
		code = wkbMultiLineString
	case "MultiPolygon":
		code = wkbMultiPolygon
	case "GeometryCollection":
		code = wkbGeometryCollection
	case "Feature", "FeatureCollection":
		return fmt.Errorf("WKB has no %s; extract .geometry first", typ)
	default:
		return fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}

	*buf = append(*buf, 1) // little-endian marker
	rawType := uint32(code)
	if hasZ {
		rawType |= ewkbZFlag
	}
	if ewkb && topLevel {
		rawType |= ewkbSRIDFlag
	}
	*buf = appendUint32LE(*buf, rawType)
	if ewkb && topLevel {
		*buf = appendUint32LE(*buf, uint32(srid))
	}

	switch typ {
	case "Point":
		pos, err := geoPositionFromValue(object["coordinates"])
		if err != nil {
			return err
		}
		appendWKBPosition(buf, pos, hasZ)
		return nil
	case "LineString":
		return appendWKBPositionList(buf, object["coordinates"], hasZ)
	case "Polygon":
		return appendWKBPositionListList(buf, object["coordinates"], hasZ)
	case "MultiPoint":
		items, ok := object["coordinates"].([]any)
		if !ok {
			return fmt.Errorf("MultiPoint coordinates must be an array")
		}
		*buf = appendUint32LE(*buf, uint32(len(items)))
		for _, item := range items {
			if err := appendWKBGeometry(buf, map[string]any{"type": "Point", "coordinates": item}, ewkb, srid, false, depth+1); err != nil {
				return err
			}
		}
		return nil
	case "MultiLineString":
		items, ok := object["coordinates"].([]any)
		if !ok {
			return fmt.Errorf("MultiLineString coordinates must be an array")
		}
		*buf = appendUint32LE(*buf, uint32(len(items)))
		for _, item := range items {
			if err := appendWKBGeometry(buf, map[string]any{"type": "LineString", "coordinates": item}, ewkb, srid, false, depth+1); err != nil {
				return err
			}
		}
		return nil
	case "MultiPolygon":
		items, ok := object["coordinates"].([]any)
		if !ok {
			return fmt.Errorf("MultiPolygon coordinates must be an array")
		}
		*buf = appendUint32LE(*buf, uint32(len(items)))
		for _, item := range items {
			if err := appendWKBGeometry(buf, map[string]any{"type": "Polygon", "coordinates": item}, ewkb, srid, false, depth+1); err != nil {
				return err
			}
		}
		return nil
	case "GeometryCollection":
		geoms, ok := object["geometries"].([]any)
		if !ok {
			return fmt.Errorf("GeometryCollection geometries must be an array")
		}
		*buf = appendUint32LE(*buf, uint32(len(geoms)))
		for i, g := range geoms {
			child, ok := g.(map[string]any)
			if !ok {
				return fmt.Errorf("geometry %d: expected a GeoJSON geometry object", i)
			}
			if err := appendWKBGeometry(buf, child, ewkb, srid, false, depth+1); err != nil {
				return fmt.Errorf("geometry %d: %w", i, err)
			}
		}
		return nil
	}
	return nil
}

func appendUint32LE(buf []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendFloat64LE(buf []byte, v float64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v))
	return append(buf, tmp[:]...)
}

func appendWKBPosition(buf *[]byte, p geoPoint, hasZ bool) {
	*buf = appendFloat64LE(*buf, p.Lon)
	*buf = appendFloat64LE(*buf, p.Lat)
	if hasZ {
		z := 0.0
		if p.Z != nil {
			z = *p.Z
		}
		*buf = appendFloat64LE(*buf, z)
	}
}

func appendWKBPositionList(buf *[]byte, value any, hasZ bool) error {
	items, ok := value.([]any)
	if !ok {
		return fmt.Errorf("expected a coordinates array, got %T", value)
	}
	*buf = appendUint32LE(*buf, uint32(len(items)))
	for i, item := range items {
		p, err := geoPositionFromValue(item)
		if err != nil {
			return fmt.Errorf("position %d: %w", i, err)
		}
		appendWKBPosition(buf, p, hasZ)
	}
	return nil
}

func appendWKBPositionListList(buf *[]byte, value any, hasZ bool) error {
	items, ok := value.([]any)
	if !ok {
		return fmt.Errorf("expected a coordinates array, got %T", value)
	}
	*buf = appendUint32LE(*buf, uint32(len(items)))
	for i, item := range items {
		if err := appendWKBPositionList(buf, item, hasZ); err != nil {
			return fmt.Errorf("ring %d: %w", i, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// SQL functions
// ---------------------------------------------------------------------------

// evalGeoBlobArg pulls a binary argument, accepting a BLOB ([]byte), a hex
// string (the common way to pass WKB through a text-only client), or a
// json.RawMessage (defensive, mirrors evalGeoTextArg).
func evalGeoBlobArg(env ExecEnv, ex *FuncCall, row Row, idx int) ([]byte, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case []byte:
		return x, nil
	case json.RawMessage:
		return []byte(x), nil
	case string:
		return decodeGeoWKBHexOrRaw(x)
	default:
		return nil, fmt.Errorf("%s arg%d: expected BLOB or hex text, got %T", ex.Name, idx+1, v)
	}
}

// decodeGeoWKBHexOrRaw accepts a hex-encoded WKB string (what a text
// protocol carries WKB as; PostGIS's own ST_AsText(bytea) round-trips this
// way). A string that is not valid hex of even length is treated as an
// error rather than silently reinterpreted as raw bytes, since a text
// column holding raw binary is not a case this engine's BLOB type exists to
// avoid.
func decodeGeoWKBHexOrRaw(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("expected hex-encoded WKB (even length), got %d characters", len(s))
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, ok1 := hexNibble(s[i*2])
		lo, ok2 := hexNibble(s[i*2+1])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("expected hex-encoded WKB, invalid character at position %d", i*2)
		}
		out[i] = hi<<4 | lo
	}
	return out, nil
}

func evalGeoFromWKB(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 2); err != nil {
		return nil, err
	}
	data, err := evalGeoBlobArg(env, ex, row, 0)
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
	obj, err := parseWKB(ex.Name, data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}

func evalGeoAsWKB(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	obj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	body, err := geoJSONToWKB(obj, false, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return body, nil
}

func evalGeoAsEWKB(env ExecEnv, ex *FuncCall, row Row) (any, error) {
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
	body, err := geoJSONToWKB(obj, true, srid)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return body, nil
}
