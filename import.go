package tinysql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	internalexporter "github.com/SimonWaldherr/tinySQL/internal/exporter"
	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ImportOptions re-exports importer.ImportOptions for convenience.
// Configure import behavior including type inference, batching, and null handling.
type ImportOptions = importer.ImportOptions

// ImportResult re-exports importer.ImportResult for convenience.
// Contains metadata about the import operation.
type ImportResult = importer.ImportResult

// ExportOptions configures JSON table export.
type ExportOptions = internalexporter.Options

// ExportTableJSON writes all rows from tableName as a JSON array. The table
// name is quoted before parsing and never concatenated as executable SQL.
func ExportTableJSON(ctx context.Context, w io.Writer, db *DB, tenant, tableName string, opts ExportOptions) error {
	if db == nil {
		return fmt.Errorf("tinysql: nil DB")
	}
	if strings.TrimSpace(tableName) == "" {
		return fmt.Errorf("tinysql: empty table name")
	}
	stmt, err := ParseSQL(`SELECT * FROM "` + strings.ReplaceAll(tableName, `"`, `""`) + `"`)
	if err != nil {
		return err
	}
	rs, err := Execute(ctx, db, tenant, stmt)
	if err != nil {
		return err
	}
	if err := decodeJSONTableColumns(db, tenant, tableName, rs); err != nil {
		return err
	}
	return internalexporter.ExportJSON(w, rs, opts)
}

// decodeJSONTableColumns restores imported JSON text to native JSON values for
// the table-export API. Query-level ExportJSON remains representation-neutral.
func decodeJSONTableColumns(db *DB, tenant, tableName string, rs *ResultSet) error {
	table, err := db.Get(tenant, tableName)
	if err != nil {
		return err
	}
	for _, column := range table.Cols {
		if column.Type != storage.JsonType {
			continue
		}
		key := strings.ToLower(column.Name)
		for _, row := range rs.Rows {
			var source []byte
			switch value := row[key].(type) {
			case string:
				source = []byte(value)
			case []byte:
				source = value
			default:
				continue
			}
			var decoded any
			decoder := json.NewDecoder(bytes.NewReader(source))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err != nil {
				return fmt.Errorf("tinysql: decode JSON column %q: %w", column.Name, err)
			}
			row[key] = decoded
		}
	}
	return nil
}

// GeoPackageInfo describes an OGC GeoPackage and its vector feature layers.
type GeoPackageInfo = importer.GeoPackageInfo

// GeoPackageLayer describes one standards-discovered GeoPackage feature table.
type GeoPackageLayer = importer.GeoPackageLayer

// ImportFile imports a structured data file into a table.
// The format is auto-detected from the file extension or content.
//
// Supported formats:
//   - CSV (.csv) - Comma-separated values with auto-detected delimiters
//   - TSV (.tsv, .tab) - Tab-separated values
//   - JSON (.json) - Array of objects format: [{"id": 1, "name": "Alice"}, ...]
//   - YAML (.yaml, .yml) - Sequence of mappings or a single mapping
//   - XML (.xml) - Simple row-based XML (limited support)
//   - GeoJSON (.geojson), KML (.kml), Shapefile (.shp)
//   - Shapefile ZIP archives (.zip)
//   - OSM XML (.osm, .osm.xml)
//   - OGC GeoPackage (.gpkg, .gpkx) and MBTiles (.mbtiles)
//   - Routing graph (.rg, .routinggraph)
//   - Compressed (.gz) - Transparent gzip decompression
//
// Example:
//
//	db := tinysql.NewDB()
//	result, err := tinysql.ImportFile(ctx, db, "default", "users", "data.csv", &tinysql.ImportOptions{
//	    CreateTable: true,
//	    TypeInference: true,
//	    BatchSize: 1000,
//	})
//	fmt.Printf("Imported %d rows\n", result.RowsInserted)
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name (use "default" for single-tenant mode)
//   - tableName: Target table name (if empty, derived from filename)
//   - filePath: Path to the file to import
//   - opts: Optional configuration (nil uses sensible defaults)
//
// Returns ImportResult with metadata and any error encountered.
func ImportFile(ctx context.Context, db *DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportFile(ctx, db, tenant, tableName, filePath, opts)
}

// ImportCSV imports CSV/TSV data from a reader into a table.
// Use this for streaming imports or when you already have an io.Reader.
//
// Example:
//
//	f, _ := os.Open("data.csv")
//	defer f.Close()
//	result, err := tinysql.ImportCSV(ctx, db, "default", "users", f, &tinysql.ImportOptions{
//	    HeaderMode: "auto",
//	    DelimiterCandidates: []rune{',', ';', '\t'},
//	})
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name
//   - tableName: Target table name
//   - src: Input reader (file, network stream, stdin, etc.)
//   - opts: Optional configuration (nil uses defaults)
//
// Returns ImportResult with metadata and any error encountered.
func ImportCSV(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportCSV(ctx, db, tenant, tableName, src, opts)
}

// ImportJSON imports JSON data from a reader into a table.
// Supports array of objects format: [{"id": 1, "name": "Alice"}, ...]
//
// Example:
//
//	jsonData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
//	result, err := tinysql.ImportJSON(ctx, db, "default", "users",
//	    strings.NewReader(jsonData), nil)
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name
//   - tableName: Target table name
//   - src: Input reader
//   - opts: Optional configuration
//
// Returns ImportResult with metadata and any error encountered.
func ImportJSON(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportYAML imports YAML data from a reader into a table.
func ImportYAML(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportYAML(ctx, db, tenant, tableName, src, opts)
}

// ImportXML imports row-oriented XML data from a reader into a table.
func ImportXML(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportXML(ctx, db, tenant, tableName, src, opts)
}

// ImportGeoJSON imports GeoJSON FeatureCollection or Feature data from a reader.
func ImportGeoJSON(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportGeoJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportTopoJSON imports a TopoJSON Topology from a reader, resolving arc
// references into GeoJSON-equivalent geometry per object.
func ImportTopoJSON(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportTopoJSON(ctx, db, tenant, tableName, src, opts)
}

// ImportKML imports KML Placemark data from a reader.
func ImportKML(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportKML(ctx, db, tenant, tableName, src, opts)
}

// ImportShapefile imports an ESRI Shapefile path and its sidecar DBF attributes.
// It requires a build with -tags=shapefile.
func ImportShapefile(ctx context.Context, db *DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportShapefile(ctx, db, tenant, tableName, filePath, opts)
}

// ImportShapefileZip imports a ZIP archive containing Shapefile sidecar files from a reader.
// It requires a build with -tags=shapefile.
func ImportShapefileZip(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportShapefileZip(ctx, db, tenant, tableName, src, opts)
}

// ImportOSM imports OSM XML (.osm or .osm.xml) from a reader.
func ImportOSM(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportOSM(ctx, db, tenant, tableName, src, opts)
}

// InspectGeoPackage lists vector feature layers through the standard
// gpkg_contents/gpkg_geometry_columns catalog. It requires -tags=sqliteimport.
func InspectGeoPackage(ctx context.Context, filePath string) (*GeoPackageInfo, error) {
	return importer.InspectGeoPackage(ctx, filePath)
}

// ImportGeoPackage imports one OGC GeoPackage feature layer. It requires
// -tags=sqliteimport. Use ImportOptions.GeoPackageLayer for multi-layer files;
// projected geometry remains in its native binary CRS unless explicitly
// reprojected before requesting GeoJSON.
func ImportGeoPackage(ctx context.Context, db *DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportGeoPackage(ctx, db, tenant, tableName, filePath, opts)
}

// ImportGeoPackageReader imports one OGC GeoPackage feature layer from a
// reader by spooling it to a temporary random-access file. It requires
// -tags=sqliteimport.
func ImportGeoPackageReader(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportGeoPackageReader(ctx, db, tenant, tableName, src, opts)
}

// ImportMBTiles imports tiles from an MBTiles SQLite database path. It requires
// a build with -tags=sqliteimport; SQLite remains the recommended production
// backend for standard MBTiles serving.
func ImportMBTiles(ctx context.Context, db *DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportMBTiles(ctx, db, tenant, tableName, filePath, opts)
}

// ImportMBTilesReader imports MBTiles from a reader by spooling to a temporary
// SQLite file. It requires a build with -tags=sqliteimport.
func ImportMBTilesReader(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportMBTilesReader(ctx, db, tenant, tableName, src, opts)
}

// ImportRoutingGraph imports routing graph JSON or CSV edge-list data from a reader.
func ImportRoutingGraph(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportRoutingGraph(ctx, db, tenant, tableName, src, opts)
}

// OpenFile opens a data file and returns a DB with the data loaded.
// This is a convenience function for quick data exploration.
//
// Example:
//
//	db, tableName, _ := tinysql.OpenFile(context.Background(), "data.csv", nil)
//	stmt, _ := tinysql.ParseSQL(fmt.Sprintf("SELECT * FROM %s LIMIT 10", tableName))
//	rs, _ := tinysql.Execute(ctx, db, "default", stmt)
//	for _, row := range rs.Rows {
//	    fmt.Println(row)
//	}
//
// Parameters:
//   - ctx: Context for cancellation
//   - filePath: Path to the data file
//   - opts: Optional import configuration
//
// Returns:
//   - db: New database instance with imported data
//   - tableName: The table name where data was loaded
//   - error: Any error encountered during import
func OpenFile(ctx context.Context, filePath string, opts *ImportOptions) (*DB, string, error) {
	return importer.OpenFile(ctx, filePath, opts)
}

// FuzzyImportOptions extends ImportOptions with fuzzy parsing capabilities.
// Use this for importing data that may have formatting issues, inconsistent
// delimiters, malformed quotes, or other common data quality problems.
type FuzzyImportOptions = importer.FuzzyImportOptions

// FuzzyImportCSV is a more forgiving version of ImportCSV that handles malformed data.
// It attempts to automatically fix common issues like:
//   - Inconsistent column counts (pads/truncates rows)
//   - Unmatched quotes in CSV fields
//   - Mixed delimiters within the same file
//   - Invalid UTF-8 characters
//   - Numbers with thousand separators
//   - Mixed data types in columns
//
// Example:
//
//	opts := &tinysql.FuzzyImportOptions{
//	    ImportOptions: &tinysql.ImportOptions{
//	        CreateTable: true,
//	        TypeInference: true,
//	    },
//	    SkipInvalidRows: true,
//	    FixQuotes: true,
//	    CoerceTypes: true,
//	}
//	result, err := tinysql.FuzzyImportCSV(ctx, db, "default", "messy_data", file, opts)
//
// Parameters are the same as ImportCSV.
// Returns ImportResult which includes errors encountered (non-fatal in fuzzy mode).
func FuzzyImportCSV(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *FuzzyImportOptions) (*ImportResult, error) {
	return importer.FuzzyImportCSV(ctx, db, tenant, tableName, src, opts)
}

// FuzzyImportJSON attempts to parse malformed JSON data.
// It handles common JSON issues like:
//   - Single quotes instead of double quotes
//   - Unquoted object keys
//   - Line-delimited JSON (NDJSON format)
//   - Trailing commas
//
// Example:
//
//	// Works even with malformed JSON like {'name': 'Alice', 'age': 30}
//	result, err := tinysql.FuzzyImportJSON(ctx, db, "default", "users", file, nil)
//
// Parameters are the same as ImportJSON.
func FuzzyImportJSON(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *FuzzyImportOptions) (*ImportResult, error) {
	return importer.FuzzyImportJSON(ctx, db, tenant, tableName, src, opts)
}
