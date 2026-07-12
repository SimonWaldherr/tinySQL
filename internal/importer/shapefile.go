//go:build shapefile

package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	shp "github.com/jonas-p/go-shp"
)

// ImportShapefile imports a .shp file (and associated DBF) and converts each
// record into a table row. Attributes become properties, geometry stored as
// JSON in the geometry column (GeometryType).
func ImportShapefile(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	filePath string,
	opts *ImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	// Open shp reader
	r, err := shp.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open shapefile: %w", err)
	}
	defer r.Close()

	// Collect features
	fields := r.Fields()
	var features []map[string]any

	for r.Next() {
		idx, shape := r.Shape()
		props := make(map[string]any)
		for fi, fld := range fields {
			// ReadAttribute returns string value
			val := r.ReadAttribute(idx, fi)
			// Field.Name is expected, but fall back to index name
			name := fld.String()
			props[name] = val
		}

		geom := shpGeometryFromShape(shape)

		feat := map[string]any{"type": "Feature", "properties": props, "geometry": geom}
		features = append(features, feat)
	}

	if len(features) == 0 {
		return nil, fmt.Errorf("no features found in shapefile %s", filepath.Base(filePath))
	}

	// Reuse GeoJSON importer logic by marshaling features to an in-memory
	// FeatureCollection JSON and delegating to ImportGeoJSON via decoder.
	fc := map[string]any{"type": "FeatureCollection", "features": features}
	b, err := json.Marshal(fc)
	if err != nil {
		return nil, fmt.Errorf("marshal featurecollection: %w", err)
	}

	// Call ImportGeoJSON using a bytes.Reader
	return ImportGeoJSON(ctx, db, tenant, tableName, bytes.NewReader(b), opts)
}

// ImportShapefileZip imports a ZIP archive containing a .shp file and its
// optional sidecar files. It is intended for upload flows that only have an
// io.Reader, such as DataDock multipart imports.
func ImportShapefileZip(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	tmp, err := os.CreateTemp("", "tinysql-shapefile-*.zip")
	if err != nil {
		return nil, fmt.Errorf("create temp shapefile zip: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("write temp shapefile zip: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("close temp shapefile zip: %w", err)
	}

	names, err := shp.ShapesInZip(tmpName)
	if err != nil {
		return nil, fmt.Errorf("read shapefile zip: %w", err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("shapefile zip contains no .shp file")
	}
	r, err := shp.OpenShapeFromZip(tmpName, names[0])
	if err != nil {
		return nil, fmt.Errorf("open shapefile zip: %w", err)
	}
	defer r.Close()

	fields := r.Fields()
	features := make([]map[string]any, 0)
	for r.Next() {
		idx, shape := r.Shape()
		props := make(map[string]any, len(fields)+1)
		props["shape_id"] = idx
		for fi, fld := range fields {
			name := strings.TrimSpace(fld.String())
			if name == "" {
				name = fmt.Sprintf("field_%d", fi+1)
			}
			props[name] = strings.TrimRight(r.Attribute(fi), "\x00 ")
		}
		features = append(features, map[string]any{
			"type":       "Feature",
			"properties": props,
			"geometry":   shpGeometryFromShape(shape),
		})
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("read shapefile zip records: %w", err)
	}
	if len(features) == 0 {
		return nil, fmt.Errorf("no features found in shapefile zip %s", filepath.Base(tmpName))
	}

	fc := map[string]any{"type": "FeatureCollection", "features": features}
	b, err := json.Marshal(fc)
	if err != nil {
		return nil, fmt.Errorf("marshal featurecollection: %w", err)
	}
	return ImportGeoJSON(ctx, db, tenant, tableName, bytes.NewReader(b), opts)
}

func shpGeometryFromShape(shape shp.Shape) any {
	switch s := shape.(type) {
	case *shp.Point:
		return map[string]any{"type": "Point", "coordinates": []float64{s.X, s.Y}}
	case *shp.PointZ:
		return map[string]any{"type": "Point", "coordinates": []float64{s.X, s.Y}}
	case *shp.PointM:
		return map[string]any{"type": "Point", "coordinates": []float64{s.X, s.Y}}
	case *shp.MultiPoint:
		return map[string]any{"type": "MultiPoint", "coordinates": shpPointCoords(s.Points)}
	case *shp.MultiPointZ:
		return map[string]any{"type": "MultiPoint", "coordinates": shpPointCoords(s.Points)}
	case *shp.MultiPointM:
		return map[string]any{"type": "MultiPoint", "coordinates": shpPointCoords(s.Points)}
	case *shp.PolyLine:
		return shpLineGeometry(s.Parts, s.Points)
	case *shp.PolyLineZ:
		return shpLineGeometry(s.Parts, s.Points)
	case *shp.PolyLineM:
		return shpLineGeometry(s.Parts, s.Points)
	case *shp.Polygon:
		poly := shp.PolyLine(*s)
		return shpPolygonGeometry(poly.Parts, poly.Points)
	case *shp.PolygonZ:
		poly := shp.PolyLineZ(*s)
		return shpPolygonGeometry(poly.Parts, poly.Points)
	case *shp.PolygonM:
		poly := shp.PolyLineZ(*s)
		return shpPolygonGeometry(poly.Parts, poly.Points)
	default:
		return nil
	}
}

func shpPointCoords(points []shp.Point) [][]float64 {
	coords := make([][]float64, len(points))
	for i, p := range points {
		coords[i] = []float64{p.X, p.Y}
	}
	return coords
}

func shpLineGeometry(parts []int32, points []shp.Point) any {
	lineParts := shpLineParts(parts, points)
	if len(lineParts) == 0 {
		return nil
	}
	if len(lineParts) == 1 {
		return map[string]any{"type": "LineString", "coordinates": lineParts[0]}
	}
	return map[string]any{"type": "MultiLineString", "coordinates": lineParts}
}

func shpPolygonGeometry(parts []int32, points []shp.Point) any {
	rings := shpLineParts(parts, points)
	if len(rings) == 0 {
		return nil
	}
	if len(rings) <= 1 {
		return map[string]any{"type": "Polygon", "coordinates": rings}
	}
	polys := make([][][][]float64, 0, len(rings))
	for _, ring := range rings {
		polys = append(polys, [][][]float64{ring})
	}
	return map[string]any{"type": "MultiPolygon", "coordinates": polys}
}

func shpLineParts(parts []int32, points []shp.Point) [][][]float64 {
	if len(points) == 0 {
		return nil
	}
	if len(parts) == 0 {
		parts = []int32{0}
	}
	out := make([][][]float64, 0, len(parts))
	for i, startRaw := range parts {
		start := int(startRaw)
		end := len(points)
		if i+1 < len(parts) {
			end = int(parts[i+1])
		}
		if start < 0 || start >= len(points) || end < start || end > len(points) {
			continue
		}
		coords := make([][]float64, 0, end-start)
		for _, p := range points[start:end] {
			coords = append(coords, []float64{p.X, p.Y})
		}
		out = append(out, coords)
	}
	return out
}
