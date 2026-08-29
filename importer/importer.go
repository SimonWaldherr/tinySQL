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

// ImportXLSX imports the first worksheet (or opts.XLSXSheet) of an Excel
// workbook from src.
func ImportXLSX(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportXLSX(ctx, db, tenant, tableName, src, opts)
}

// ImportGeoJSON imports GeoJSON feature data from src.
func ImportGeoJSON(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportGeoJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportTopoJSON imports a TopoJSON Topology from src, resolving arc
// references back to plain geometry coordinates before import.
func ImportTopoJSON(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportTopoJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportKML imports KML placemark data from src.
func ImportKML(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportKML(ctx, db, tenant, tableName, src, opts)
}

// ImportShapefile imports an ESRI Shapefile path into a table.
func ImportShapefile(ctx context.Context, db *tinysql.DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportShapefile(ctx, db, tenant, tableName, filePath, opts)
}

// ImportShapefileZip imports a ZIP archive containing Shapefile sidecar files from src.
func ImportShapefileZip(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportShapefileZip(ctx, db, tenant, tableName, src, opts)
}

// ImportOSM imports OSM XML (.osm or .osm.xml) from src.
func ImportOSM(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportOSM(ctx, db, tenant, tableName, src, opts)
}

// ImportMBTiles imports tiles from an MBTiles SQLite database path.
func ImportMBTiles(ctx context.Context, db *tinysql.DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportMBTiles(ctx, db, tenant, tableName, filePath, opts)
}

// ImportMBTilesReader imports MBTiles from src by spooling to a temporary SQLite file.
func ImportMBTilesReader(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportMBTilesReader(ctx, db, tenant, tableName, src, opts)
}

// ImportRoutingGraph imports routing graph JSON or CSV edge-list data from src.
func ImportRoutingGraph(ctx context.Context, db *tinysql.DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return ii.ImportRoutingGraph(ctx, db, tenant, tableName, src, opts)
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

// ─────────────────────────── MBTiles tilesets ────────────────────────────────

// ExportMBTilesOptions configures writing a tileset to an .mbtiles file.
type ExportMBTilesOptions = ii.ExportMBTilesOptions

// ExportMBTilesResult reports what an export wrote.
type ExportMBTilesResult = ii.ExportMBTilesResult

// OpenMBTilesOptions configures querying an .mbtiles file in place.
type OpenMBTilesOptions = ii.OpenMBTilesOptions

// OpenMBTilesResult reports what an in-place open exposed.
type OpenMBTilesResult = ii.OpenMBTilesResult

// MBTilesArtifactSchema selects the physical representation of a published
// dataset.tinysql artifact.
type MBTilesArtifactSchema = ii.MBTilesArtifactSchema
type MBTilesArtifactOptions = ii.MBTilesArtifactOptions
type MBTilesProgress = ii.MBTilesProgress
type MBTilesResourceEstimate = ii.MBTilesResourceEstimate
type MBTilesArtifactTable = ii.MBTilesArtifactTable
type MBTilesArtifactIndex = ii.MBTilesArtifactIndex
type MBTilesArtifactManifest = ii.MBTilesArtifactManifest
type MBTilesArtifactResult = ii.MBTilesArtifactResult
type MBTilesReader = ii.MBTilesReader

const (
	MBTilesSchemaAuto       = ii.MBTilesSchemaAuto
	MBTilesSchemaFlat       = ii.MBTilesSchemaFlat
	MBTilesSchemaNormalized = ii.MBTilesSchemaNormalized
)

// ImportMBTilesArtifact builds and atomically publishes a validated
// dataset.tinysql directory. It is available with the sqliteimport build tag.
func ImportMBTilesArtifact(ctx context.Context, sourcePath, artifactPath string, opts *MBTilesArtifactOptions) (*MBTilesArtifactResult, error) {
	return ii.ImportMBTilesArtifact(ctx, sourcePath, artifactPath, opts)
}

// ValidateMBTilesArtifact verifies the marker, manifest, checksums, table
// counts, unique tile keys, index coverage and tile payload parity.
func ValidateMBTilesArtifact(ctx context.Context, artifactPath string) (*MBTilesArtifactManifest, error) {
	return ii.ValidateMBTilesArtifact(ctx, artifactPath)
}

// OpenMBTilesArtifact validates an artifact before opening its read-only
// paged-index database.
func OpenMBTilesArtifact(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*tinysql.DB, *MBTilesArtifactManifest, error) {
	return ii.OpenMBTilesArtifact(ctx, artifactPath, maxMemoryBytes)
}

func OpenMBTilesReader(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*MBTilesReader, error) {
	return ii.OpenMBTilesReader(ctx, artifactPath, maxMemoryBytes)
}

// ExportMBTiles writes a tinySQL tileset to filePath as a spec-compliant MBTiles
// (SQLite) database, so the result is usable by any MBTiles-aware tool.
// Requires the sqliteimport build tag.
func ExportMBTiles(ctx context.Context, db *tinysql.DB, tenant, filePath string, opts *ExportMBTilesOptions) (*ExportMBTilesResult, error) {
	return ii.ExportMBTiles(ctx, db, tenant, filePath, opts)
}

// OpenMBTiles exposes an existing .mbtiles file as queryable tinySQL tables
// without copying the whole tileset. Use OpenMBTilesOptions.Zooms and
// WithoutTileData to work with a tileset larger than memory.
// Requires the sqliteimport build tag.
func OpenMBTiles(ctx context.Context, db *tinysql.DB, tenant, filePath string, opts *OpenMBTilesOptions) (*OpenMBTilesResult, error) {
	return ii.OpenMBTiles(ctx, db, tenant, filePath, opts)
}

// LookupMBTilesTile reads one tile's raw bytes directly from filePath's own
// SQLite storage -- an indexed point query, not a zoom-range scan -- given
// z/x/y in the ordinary XYZ/Slippy-map convention. found is false with a nil
// error when the tileset does not cover that tile. Requires the sqliteimport
// build tag.
func LookupMBTilesTile(ctx context.Context, filePath string, z, x, y int) (data []byte, found bool, err error) {
	return ii.LookupMBTilesTile(ctx, filePath, z, x, y)
}
