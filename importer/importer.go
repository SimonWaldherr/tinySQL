// Package importer exposes stable data import helpers backed by tinySQL's
// internal importer implementation.
package importer

import (
	"context"
	"io"

	tinysql "github.com/SimonWaldherr/tinySQL"
	ii "github.com/SimonWaldherr/tinySQL/internal/importer"
)

// ImportOptions controls structured data imports.
type ImportOptions = ii.ImportOptions

// ImportResult contains metadata about an import operation.
type ImportResult = ii.ImportResult

// FuzzyImportOptions extends ImportOptions with tolerant parsing behavior.
type FuzzyImportOptions = ii.FuzzyImportOptions

// ImportFile detects a file format and imports it into a table.
func ImportFile(ctx context.Context, db *tinysql.DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportFile(ctx, db, tenant, tableName, filePath, opts)
}

// ImportCSV imports CSV or TSV-like data from src.
func ImportCSV(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportCSV(ctx, db, tenant, tableName, src, opts)
}

// ImportJSON imports JSON or NDJSON-like data from src.
func ImportJSON(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportYAML imports a YAML mapping or sequence of mappings from src.
func ImportYAML(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportYAML(ctx, db, tenant, tableName, src, opts)
}

// ImportXML imports row-oriented XML from src.
func ImportXML(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportXML(ctx, db, tenant, tableName, src, opts)
}

// ImportGeoJSON imports GeoJSON feature data from src.
func ImportGeoJSON(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportGeoJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportKML imports KML placemark data from src.
func ImportKML(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportKML(ctx, db, tenant, tableName, src, opts)
}

// ImportShapefile imports an ESRI Shapefile path into a table.
func ImportShapefile(ctx context.Context, db *tinysql.DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportShapefile(ctx, db, tenant, tableName, filePath, opts)
}

// OpenFile creates a new DB and imports filePath into it.
func OpenFile(ctx context.Context, filePath string, opts *ImportOptions) (*tinysql.DB, string, error) {
	return ii.OpenFile(ctx, filePath, opts)
}

// FuzzyImportCSV imports malformed CSV-like data using tolerant parsing.
func FuzzyImportCSV(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *FuzzyImportOptions) (*ImportResult, error) {
	return ii.FuzzyImportCSV(ctx, db, tenant, tableName, src, opts)
}

// FuzzyImportJSON imports malformed JSON-like data using tolerant parsing.
func FuzzyImportJSON(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *FuzzyImportOptions) (*ImportResult, error) {
	return ii.FuzzyImportJSON(ctx, db, tenant, tableName, src, opts)
}
