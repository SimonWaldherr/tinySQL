// Package geoencoding contains service- and storage-independent geospatial
// wire-format codecs shared by SQL functions and file importers.
package geoencoding

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	maxWKBBytes  = 64 << 20
	maxWKBDepth  = 32
	ewkbZFlag    = uint32(0x80000000)
	ewkbMFlag    = uint32(0x40000000)
	ewkbSRIDFlag = uint32(0x20000000)
)

// WKBResult contains a decoded GeoJSON-shaped geometry and every EWKB SRID
// encountered in its top-level or nested geometry headers. Plain OGC/ISO WKB
// has no embedded SRID and leaves SRIDs empty.
type WKBResult struct {
	Geometry map[string]any
	SRIDs    []uint32
}

// DecodeWKB decodes OGC WKB, EWKB flags, and ISO SQL/MM Z/M/ZM type offsets.
// Z is preserved and M is consumed but dropped because GeoJSON has no measure
// ordinate.
func DecodeWKB(data []byte) (WKBResult, error) {
	if len(data) > maxWKBBytes {
		return WKBResult{}, fmt.Errorf("WKB input is %d bytes, exceeding the %d byte limit", len(data), maxWKBBytes)
	}
	reader := &wkbReader{data: data}
	geometry, err := reader.readGeometry(0)
	if err != nil {
		return WKBResult{}, err
	}
	if reader.pos != len(reader.data) {
		return WKBResult{}, reader.errorf("%d trailing byte(s) after geometry", len(reader.data)-reader.pos)
	}
	return WKBResult{Geometry: geometry, SRIDs: reader.srids}, nil
}

type wkbReader struct {
	data  []byte
	pos   int
	order binary.ByteOrder
	srids []uint32
}

func (reader *wkbReader) errorf(format string, args ...any) error {
	return fmt.Errorf("WKB: %s (at byte %d)", fmt.Sprintf(format, args...), reader.pos)
}

func (reader *wkbReader) readByte() (byte, error) {
	if reader.pos >= len(reader.data) {
		return 0, reader.errorf("unexpected end of input")
	}
	value := reader.data[reader.pos]
	reader.pos++
	return value, nil
}

func (reader *wkbReader) readUint32() (uint32, error) {
	if reader.pos+4 > len(reader.data) {
		return 0, reader.errorf("unexpected end of input")
	}
	value := reader.order.Uint32(reader.data[reader.pos:])
	reader.pos += 4
	return value, nil
}

func (reader *wkbReader) readFloat64() (float64, error) {
	if reader.pos+8 > len(reader.data) {
		return 0, reader.errorf("unexpected end of input")
	}
	value := math.Float64frombits(reader.order.Uint64(reader.data[reader.pos:]))
	reader.pos += 8
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, reader.errorf("non-finite coordinate")
	}
	return value, nil
}

func (reader *wkbReader) readGeometry(depth int) (map[string]any, error) {
	if depth > maxWKBDepth {
		return nil, reader.errorf("GeometryCollection nested more than %d deep", maxWKBDepth)
	}
	marker, err := reader.readByte()
	if err != nil {
		return nil, err
	}
	switch marker {
	case 0:
		reader.order = binary.BigEndian
	case 1:
		reader.order = binary.LittleEndian
	default:
		return nil, reader.errorf("invalid byte order marker 0x%02x", marker)
	}
	rawType, err := reader.readUint32()
	if err != nil {
		return nil, err
	}
	hasZ := rawType&ewkbZFlag != 0
	hasM := rawType&ewkbMFlag != 0
	baseCode := rawType &^ (ewkbZFlag | ewkbMFlag | ewkbSRIDFlag)
	switch {
	case baseCode >= 3000 && baseCode < 4000:
		hasZ, hasM, baseCode = true, true, baseCode-3000
	case baseCode >= 2000 && baseCode < 3000:
		hasM, baseCode = true, baseCode-2000
	case baseCode >= 1000 && baseCode < 2000:
		hasZ, baseCode = true, baseCode-1000
	}
	if rawType&ewkbSRIDFlag != 0 {
		srid, err := reader.readUint32()
		if err != nil {
			return nil, err
		}
		reader.srids = append(reader.srids, srid)
	}

	switch baseCode {
	case 1:
		position, err := reader.readPosition(hasZ, hasM)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "Point", "coordinates": position}, nil
	case 2:
		positions, err := reader.readPositionList(hasZ, hasM)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "LineString", "coordinates": positions}, nil
	case 3:
		rings, err := reader.readPositionListList(hasZ, hasM)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "Polygon", "coordinates": rings}, nil
	case 4:
		return reader.readCollectionCoordinates("MultiPoint", "Point", depth)
	case 5:
		return reader.readCollectionCoordinates("MultiLineString", "LineString", depth)
	case 6:
		return reader.readCollectionCoordinates("MultiPolygon", "Polygon", depth)
	case 7:
		count, err := reader.readGeometryCount()
		if err != nil {
			return nil, err
		}
		geometries := make([]any, 0, count)
		for i := uint32(0); i < count; i++ {
			child, err := reader.readGeometry(depth + 1)
			if err != nil {
				return nil, err
			}
			geometries = append(geometries, child)
		}
		return map[string]any{"type": "GeometryCollection", "geometries": geometries}, nil
	default:
		return nil, reader.errorf("unsupported WKB geometry type code %d", baseCode)
	}
}

func (reader *wkbReader) readCollectionCoordinates(name, childType string, depth int) (map[string]any, error) {
	count, err := reader.readGeometryCount()
	if err != nil {
		return nil, err
	}
	coordinates := make([]any, 0, count)
	for i := uint32(0); i < count; i++ {
		child, err := reader.readGeometry(depth + 1)
		if err != nil {
			return nil, err
		}
		if actual, _ := child["type"].(string); actual != childType {
			return nil, reader.errorf("%s member must be a %s, got %s", name, childType, actual)
		}
		coordinates = append(coordinates, child["coordinates"])
	}
	return map[string]any{"type": name, "coordinates": coordinates}, nil
}

func (reader *wkbReader) readGeometryCount() (uint32, error) {
	count, err := reader.readUint32()
	if err != nil {
		return 0, err
	}
	if uint64(count) > uint64(len(reader.data)-reader.pos)/5 {
		return 0, reader.errorf("geometry count %d exceeds remaining input", count)
	}
	return count, nil
}

func (reader *wkbReader) readPosition(hasZ, hasM bool) ([]any, error) {
	x, err := reader.readFloat64()
	if err != nil {
		return nil, err
	}
	y, err := reader.readFloat64()
	if err != nil {
		return nil, err
	}
	position := []any{x, y}
	if hasZ {
		z, err := reader.readFloat64()
		if err != nil {
			return nil, err
		}
		position = append(position, z)
	}
	if hasM {
		if _, err := reader.readFloat64(); err != nil {
			return nil, err
		}
	}
	return position, nil
}

func (reader *wkbReader) readPositionList(hasZ, hasM bool) ([]any, error) {
	count, err := reader.readUint32()
	if err != nil {
		return nil, err
	}
	bytesPerPosition := 16
	if hasZ {
		bytesPerPosition += 8
	}
	if hasM {
		bytesPerPosition += 8
	}
	if uint64(count) > uint64(len(reader.data)-reader.pos)/uint64(bytesPerPosition) {
		return nil, reader.errorf("position count %d exceeds remaining input", count)
	}
	positions := make([]any, 0, count)
	for i := uint32(0); i < count; i++ {
		position, err := reader.readPosition(hasZ, hasM)
		if err != nil {
			return nil, err
		}
		positions = append(positions, position)
	}
	return positions, nil
}

func (reader *wkbReader) readPositionListList(hasZ, hasM bool) ([]any, error) {
	count, err := reader.readUint32()
	if err != nil {
		return nil, err
	}
	if uint64(count) > uint64(len(reader.data)-reader.pos)/4 {
		return nil, reader.errorf("ring count %d exceeds remaining input", count)
	}
	lists := make([]any, 0, count)
	for i := uint32(0); i < count; i++ {
		positions, err := reader.readPositionList(hasZ, hasM)
		if err != nil {
			return nil, err
		}
		lists = append(lists, positions)
	}
	return lists, nil
}
