package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

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

		// Build simple GeoJSON-like geometry
		var geom any
		switch s := shape.(type) {
		case *shp.Point:
			geom = map[string]any{"type": "Point", "coordinates": []float64{s.X, s.Y}}
		case *shp.PolyLine:
			coords := make([][]float64, len(s.Points))
			for i, p := range s.Points {
				coords[i] = []float64{p.X, p.Y}
			}
			geom = map[string]any{"type": "LineString", "coordinates": coords}
		case *shp.Polygon:
			// Treat polygon points as a single linear ring
			ring := make([][]float64, len(s.Points))
			for i, p := range s.Points {
				ring[i] = []float64{p.X, p.Y}
			}
			geom = map[string]any{"type": "Polygon", "coordinates": []any{ring}}
		default:
			geom = nil
		}

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
