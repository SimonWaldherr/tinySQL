// Package gpkg implements the service-independent binary parts of the OGC
// GeoPackage standard.  It deliberately has no SQLite dependency: callers can
// inspect a GeoPackageBinary geometry received from a database, file, message,
// or SQL BLOB in exactly the same way.
package gpkg

import (
	"encoding/binary"
	"fmt"
	"math"
)

const fixedHeaderSize = 8

// Geometry is a parsed GeoPackageBinary header plus its embedded OGC WKB.
// Envelope preserves the standard's on-wire order
// (minx,maxx,miny,maxy[,minz,maxz][,minm,maxm]); BBox exposes the conventional
// [minx,miny,maxx,maxy] order used by GeoJSON, TileJSON, and tinySQL.
type Geometry struct {
	Version   byte
	SRID      int32
	Empty     bool
	Extended  bool
	Envelope  []float64
	BBox      []float64
	WKB       []byte
	HeaderLen int
}

// ParseGeometry validates and splits an OGC GeoPackageBinary geometry.
func ParseGeometry(data []byte) (Geometry, error) {
	if len(data) < fixedHeaderSize {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: input is %d bytes, need at least %d", len(data), fixedHeaderSize)
	}
	if data[0] != 'G' || data[1] != 'P' {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: invalid magic 0x%02x%02x, want GP", data[0], data[1])
	}
	version := data[2]
	if version != 0 {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: unsupported version %d", version)
	}
	flags := data[3]
	if flags&0xc0 != 0 {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: reserved flag bits must be zero")
	}
	envelopeCode := int((flags >> 1) & 0x07)
	empty := flags&0x10 != 0
	envelopeDoubles := 0
	switch envelopeCode {
	case 0:
	case 1:
		envelopeDoubles = 4 // XY
	case 2, 3:
		envelopeDoubles = 6 // XYZ or XYM
	case 4:
		envelopeDoubles = 8 // XYZM
	default:
		return Geometry{}, fmt.Errorf("GeoPackageBinary: invalid envelope indicator %d", envelopeCode)
	}
	if empty && envelopeCode != 0 {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: empty geometry must not contain an envelope")
	}
	headerLen := fixedHeaderSize + envelopeDoubles*8
	if len(data) < headerLen {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: truncated %d-byte header (input is %d bytes)", headerLen, len(data))
	}

	var order binary.ByteOrder = binary.BigEndian
	if flags&0x01 != 0 {
		order = binary.LittleEndian
	}
	g := Geometry{
		Version: version, SRID: int32(order.Uint32(data[4:8])),
		Empty: empty, Extended: flags&0x20 != 0,
		HeaderLen: headerLen,
	}
	if envelopeDoubles > 0 {
		g.Envelope = make([]float64, envelopeDoubles)
		for i := range g.Envelope {
			bits := order.Uint64(data[fixedHeaderSize+i*8:])
			g.Envelope[i] = math.Float64frombits(bits)
		}
		if !finiteEnvelope(g.Envelope) {
			return Geometry{}, fmt.Errorf("GeoPackageBinary: non-empty geometry has a non-finite envelope")
		}
		for i := 0; i < len(g.Envelope); i += 2 {
			if g.Envelope[i] > g.Envelope[i+1] {
				return Geometry{}, fmt.Errorf("GeoPackageBinary: envelope minimum exceeds maximum for ordinate pair %d", i/2+1)
			}
		}
		g.BBox = []float64{g.Envelope[0], g.Envelope[2], g.Envelope[1], g.Envelope[3]}
	}
	g.WKB = data[headerLen:]
	if !g.Empty && len(g.WKB) == 0 {
		return Geometry{}, fmt.Errorf("GeoPackageBinary: non-empty geometry has no WKB payload")
	}
	return g, nil
}

func finiteEnvelope(values []float64) bool {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}
