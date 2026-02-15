package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ImportGeoJSON imports a GeoJSON file. It supports FeatureCollection and
// individual Feature objects. Properties become table columns; geometry is
// stored in a `geometry` column with type GeometryType.
func ImportGeoJSON(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	// Read into a generic structure so we can support either FeatureCollection
	// or a sequence of Feature objects (NDJSON style).
	br := bufio.NewReader(src)
	dec := json.NewDecoder(br)

	// Decode top-level value
	var top any
	if err := dec.Decode(&top); err != nil {
		return nil, fmt.Errorf("decode geojson: %w", err)
	}

	var features []map[string]any

	// Helper to collect a single feature-like object
	addFeature := func(obj any) error {
		m, ok := obj.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid feature object")
		}
		// If this is a FeatureCollection, its features are in .features
		if t, ok := m["type"].(string); ok && t == "FeatureCollection" {
			if farr, ok := m["features"].([]any); ok {
				for _, fi := range farr {
					if fm, ok := fi.(map[string]any); ok {
						features = append(features, fm)
					}
				}
				return nil
			}
		}

		// If object is a Feature, accept it
		if t, ok := m["type"].(string); ok && t == "Feature" {
			features = append(features, m)
			return nil
		}

		// If object lacks "type: Feature" but is an object with properties,
		// treat it as a single record with geometry if present.
		features = append(features, m)
		return nil
	}

	if err := addFeature(top); err != nil {
		return nil, err
	}

	// If the decoder has more JSON values (NDJSON), read them too
	for dec.More() {
		var obj any
		if err := dec.Decode(&obj); err != nil {
			break
		}
		_ = addFeature(obj)
	}

	if len(features) == 0 {
		return nil, fmt.Errorf("no features found in GeoJSON")
	}

	// Build column names: properties keys + geometry
	first := features[0]
	propKeys := make([]string, 0)
	if props, ok := first["properties"].(map[string]any); ok {
		for k := range props {
			propKeys = append(propKeys, k)
		}
	} else {
		// Fallback: use top-level keys except geometry
		for k := range first {
			if k == "geometry" || k == "type" {
				continue
			}
			propKeys = append(propKeys, k)
		}
	}

	// Ensure deterministic order
	sanitizeColumnNames(propKeys)

	colNames := append([]string{}, propKeys...)
	geomCol := "geometry"
	colNames = append(colNames, geomCol)

	// Build sample data for type inference (stringified properties)
	sampleData := make([][]string, 0, len(features))
	for _, f := range features {
		row := make([]string, len(propKeys))
		var props map[string]any
		if p, ok := f["properties"].(map[string]any); ok {
			props = p
		} else {
			// If properties absent, try top-level keys
			props = make(map[string]any)
			for _, k := range propKeys {
				if v, ok := f[k]; ok {
					props[k] = v
				}
			}
		}
		for i, k := range propKeys {
			if v, ok := props[k]; ok && v != nil {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		sampleData = append(sampleData, row)
	}

	// Infer types for properties; geometry column uses GeometryType
	var colTypes []storage.ColType
	if opts.TypeInference {
		colTypes = inferColumnTypes(sampleData, len(propKeys), opts)
	} else {
		colTypes = make([]storage.ColType, len(propKeys))
		for i := range colTypes {
			colTypes[i] = storage.TextType
		}
	}
	colTypes = append(colTypes, storage.GeometryType)

	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	if opts.CreateTable {
		if err := createTable(ctx, db, tenant, tableName, colNames, colTypes); err != nil {
			return nil, err
		}
	}
	if opts.Truncate {
		if err := truncateTable(ctx, db, tenant, tableName); err != nil {
			return nil, err
		}
	}

	// Insert rows
	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	for i, f := range features {
		row := make([]any, len(colNames))

		var props map[string]any
		if p, ok := f["properties"].(map[string]any); ok {
			props = p
		} else {
			props = make(map[string]any)
			for _, k := range propKeys {
				if v, ok := f[k]; ok {
					props[k] = v
				}
			}
		}

		for j, k := range propKeys {
			if v, ok := props[k]; ok && v != nil {
				s := fmt.Sprintf("%v", v)
				conv, err := convertValue(s, colTypes[j], opts.DateTimeFormats, opts.NullLiterals)
				if err != nil && opts.StrictTypes {
					result.Errors = append(result.Errors, fmt.Sprintf("row %d, col %s: %v", i+1, k, err))
					result.RowsSkipped++
					continue
				}
				if err != nil {
					row[j] = s
				} else {
					row[j] = conv
				}
			}
		}

		// Geometry: marshal geometry object to JSON and store as json.RawMessage
		var geom any
		if g, ok := f["geometry"]; ok && g != nil {
			geom = g
		}
		if geom != nil {
			if b, err := json.Marshal(geom); err == nil {
				// store as json.RawMessage so consumers can reparse easily
				row[len(row)-1] = json.RawMessage(b)
			} else {
				row[len(row)-1] = nil
			}
		} else {
			row[len(row)-1] = nil
		}

		tbl.Rows = append(tbl.Rows, row)
		result.RowsInserted++
	}

	return result, nil
}
