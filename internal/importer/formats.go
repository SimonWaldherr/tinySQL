package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// File Format Detection and Import
// ============================================================================

// ImportFile detects the file format and imports it into a tinySQL table.
// Supports: CSV, TSV, JSON, XML (with automatic detection based on extension/content).
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
func ImportFile(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	filePath string,
	opts *ImportOptions,
) (*ImportResult, error) {
	// Open file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// Detect format from extension
	ext := strings.ToLower(filepath.Ext(filePath))

	// If table name not provided, derive from filename
	if tableName == "" {
		base := filepath.Base(filePath)
		tableName = strings.TrimSuffix(base, filepath.Ext(base))
		tableName = sanitizeTableName(tableName)
	}

	// Remove .gz extension if present for format detection
	if ext == ".gz" {
		base := strings.TrimSuffix(filePath, ".gz")
		ext = strings.ToLower(filepath.Ext(base))
	}

	// Import based on format
	switch ext {
	case ".geojson":
		return ImportGeoJSON(ctx, db, tenant, tableName, f, opts)

	case ".shp":
		// Shapefiles are multi-file sets; pass the path for the reader to open.
		return ImportShapefile(ctx, db, tenant, tableName, filePath, opts)

	case ".kml":
		return ImportKML(ctx, db, tenant, tableName, f, opts)
	case ".csv":
		if opts == nil {
			opts = &ImportOptions{}
		}
		if len(opts.DelimiterCandidates) == 0 {
			opts.DelimiterCandidates = []rune{','}
		}
		return ImportCSV(ctx, db, tenant, tableName, f, opts)

	case ".tsv", ".tab":
		if opts == nil {
			opts = &ImportOptions{}
		}
		opts.DelimiterCandidates = []rune{'\t'}
		return ImportCSV(ctx, db, tenant, tableName, f, opts)

	case ".json", ".ndjson", ".jsonl":
		return ImportJSON(ctx, db, tenant, tableName, f, opts)

	case ".yaml", ".yml":
		if opts == nil {
			opts = &ImportOptions{}
		}
		// Simple YAML import: convert to records and reuse JSON path
		return ImportYAML(ctx, db, tenant, tableName, f, opts)

	case ".xml":
		return ImportXML(ctx, db, tenant, tableName, f, opts)

	default:
		// Try auto-detection by content
		return importByContent(ctx, db, tenant, tableName, f, opts)
	}
}

// importByContent attempts to detect format by examining file content.
func importByContent(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	f *os.File,
	opts *ImportOptions,
) (*ImportResult, error) {
	// Peek at first bytes
	br := bufio.NewReader(f)
	peek, _ := br.Peek(512)

	// Check for JSON
	trimmed := strings.TrimSpace(string(peek))
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		// Seek back to start
		f.Seek(0, 0)
		return ImportJSON(ctx, db, tenant, tableName, f, opts)
	}

	// Check for XML
	if strings.HasPrefix(trimmed, "<?xml") || strings.HasPrefix(trimmed, "<") {
		f.Seek(0, 0)
		return ImportXML(ctx, db, tenant, tableName, f, opts)
	}

	// Default to CSV with auto-detection
	f.Seek(0, 0)
	return ImportCSV(ctx, db, tenant, tableName, f, opts)
}

// ImportYAML provides basic YAML -> table import. It supports a YAML list
// of mappings or a single mapping. This is a convenience helper and does
// not attempt to support every YAML feature; it converts YAML to Go maps
// and processes them similarly to JSON imports.
func ImportYAML(
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

	all, err := io.ReadAll(src)
	if err != nil {
		return nil, fmt.Errorf("read YAML: %w", err)
	}

	var records []map[string]any
	// Try YAML list
	if err := yaml.Unmarshal(all, &records); err == nil {
		// ok
	} else {
		// Try single mapping
		var single map[string]any
		if err2 := yaml.Unmarshal(all, &single); err2 == nil {
			records = append(records, single)
		} else {
			return nil, fmt.Errorf("unsupported YAML structure: %v / %v", err, err2)
		}
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no records found in YAML")
	}

	// Extract column names
	colNames := make([]string, 0, len(records[0]))
	for k := range records[0] {
		colNames = append(colNames, k)
	}
	sanitizeColumnNames(colNames)

	// Convert to strings for type inference
	sampleData := make([][]string, 0, len(records))
	for _, rec := range records {
		row := make([]string, len(colNames))
		for i, col := range colNames {
			if v, ok := rec[col]; ok && v != nil {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		sampleData = append(sampleData, row)
	}

	var colTypes []storage.ColType
	if opts.TypeInference {
		colTypes = inferColumnTypes(sampleData, len(colNames), opts)
	} else {
		colTypes = make([]storage.ColType, len(colNames))
		for i := range colTypes {
			colTypes[i] = storage.TextType
		}
	}

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

	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	for i, rec := range records {
		row := make([]any, len(colNames))
		for j, col := range colNames {
			if val, ok := rec[col]; ok {
				converted, err := convertValue(fmt.Sprintf("%v", val), colTypes[j], opts.DateTimeFormats, opts.NullLiterals)
				if err != nil && opts.StrictTypes {
					result.Errors = append(result.Errors, fmt.Sprintf("row %d, col %s: %v", i+1, col, err))
					result.RowsSkipped++
					continue
				}
				row[j] = converted
			}
		}
		tbl.Rows = append(tbl.Rows, row)
		result.RowsInserted++
	}

	return result, nil
}

// sanitizeTableName converts a filename to a valid table name.
func sanitizeTableName(name string) string {
	// Replace non-alphanumeric chars with underscore
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)

	// Remove leading digits
	name = strings.TrimLeftFunc(name, func(r rune) bool {
		return r >= '0' && r <= '9'
	})

	if name == "" {
		name = "imported_table"
	}

	return name
}

// ============================================================================
// JSON Import
// ============================================================================

// ImportJSON imports JSON data from a reader into a tinySQL table.
// Supports:
//   - Array of objects: [{"id": 1, "name": "Alice"}, ...]
//   - JSON Lines format: {"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}
//   - Single object: {"id": 1, "name": "Alice"} (creates single-row table)
//
//nolint:gocyclo // JSON import supports arrays, objects, and error recovery in one routine.
func ImportJSON(
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

	result := &ImportResult{
		Encoding: "utf-8",
		Errors:   make([]string, 0),
	}

	// Use a buffered reader so we can peek and then stream decode.
	br := bufio.NewReader(src)
	peek, _ := br.Peek(512)
	trimmed := strings.TrimSpace(string(peek))

	var records []map[string]any

	// If input is a JSON array, stream-decode each element using json.Decoder.
	if strings.HasPrefix(trimmed, "[") {
		dec := json.NewDecoder(br)
		// consume '['
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("read JSON array token: %w", err)
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '[' {
			return nil, fmt.Errorf("expected JSON array")
		}
		for dec.More() {
			var rec map[string]any
			if err := dec.Decode(&rec); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("decode record: %v", err))
				continue
			}
			records = append(records, rec)
		}
	} else if strings.HasPrefix(trimmed, "{") {
		// Treat input as NDJSON (line-delimited JSON). Stream by scanning lines
		scanner := bufio.NewScanner(br)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("decode record: %v", err))
				continue
			}
			records = append(records, rec)
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("scan NDJSON: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unsupported JSON format")
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no records found in JSON")
	}

	// Extract column names from first record
	colNames := make([]string, 0, len(records[0]))
	for key := range records[0] {
		colNames = append(colNames, key)
	}
	sanitizeColumnNames(colNames)
	result.ColumnNames = colNames

	// Convert records to rows
	sampleData := make([][]string, 0, len(records))
	for _, rec := range records {
		row := make([]string, len(colNames))
		for i, col := range colNames {
			if val, ok := rec[col]; ok && val != nil {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		sampleData = append(sampleData, row)
	}

	// Infer types
	var colTypes []storage.ColType
	if opts.TypeInference {
		colTypes = inferColumnTypes(sampleData, len(colNames), opts)
	} else {
		colTypes = make([]storage.ColType, len(colNames))
		for i := range colTypes {
			colTypes[i] = storage.TextType
		}
	}
	result.ColumnTypes = colTypes

	// Create table
	if opts.CreateTable {
		if err := createTable(ctx, db, tenant, tableName, colNames, colTypes); err != nil {
			return nil, err
		}
	}

	// Truncate if requested
	if opts.Truncate {
		if err := truncateTable(ctx, db, tenant, tableName); err != nil {
			return nil, err
		}
	}

	// Insert data
	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	for i, rec := range records {
		row := make([]any, len(colNames))
		for j, col := range colNames {
			if val, ok := rec[col]; ok {
				converted, err := convertValue(fmt.Sprintf("%v", val), colTypes[j],
					opts.DateTimeFormats, opts.NullLiterals)
				if err != nil && opts.StrictTypes {
					result.Errors = append(result.Errors,
						fmt.Sprintf("row %d, col %s: %v", i+1, col, err))
					result.RowsSkipped++
					continue
				}
				row[j] = converted
			}
		}
		tbl.Rows = append(tbl.Rows, row)
		result.RowsInserted++
	}

	return result, nil
}

// ============================================================================
// XML Import
// ============================================================================

// XMLRecord represents a generic XML element for table import.
type XMLRecord struct {
	XMLName xml.Name
	Attrs   []xml.Attr  `xml:",any,attr"`
	Content string      `xml:",chardata"`
	Nodes   []XMLRecord `xml:",any"`
}

// ImportXML imports XML data from a reader into a tinySQL table.
// Supports simple row-based XML like:
//
//	<root>
//	  <record id="1" name="Alice" />
//	  <record id="2" name="Bob" />
//	</root>
//
// or:
//
//	<root>
//	  <record><id>1</id><name>Alice</name></record>
//	  <record><id>2</id><name>Bob</name></record>
//	</root>
func ImportXML(
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

	result := &ImportResult{
		Encoding: "utf-8",
		Errors:   make([]string, 0),
	}

	// For now, return a helpful error - XML parsing is complex
	// and would require knowing the schema
	_ = result // Keep for future implementation
	return nil, fmt.Errorf("XML import not yet implemented - please convert to CSV or JSON first")
}

// ============================================================================
// OpenFile - Convenience function to open and query files directly
// ============================================================================

// OpenFile opens a data file (CSV, TSV, JSON, XML) and returns a DB with the data loaded.
// The table name is derived from the filename unless specified in options.
//
// This is a convenience function for quick data exploration:
//
//	db, _ := importer.OpenFile(context.Background(), "data.csv", nil)
//	// Now you can query it with tinySQL
func OpenFile(ctx context.Context, filePath string, opts *ImportOptions) (*storage.DB, string, error) {
	db := storage.NewDB()
	tenant := "default"

	// Derive table name from file if not specified
	tableName := ""
	if opts != nil && opts.TableName != "" {
		tableName = opts.TableName
	} else {
		base := filepath.Base(filePath)
		tableName = strings.TrimSuffix(base, filepath.Ext(base))
		tableName = sanitizeTableName(tableName)
	}

	_, err := ImportFile(ctx, db, tenant, tableName, filePath, opts)
	if err != nil {
		return nil, "", err
	}

	return db, tableName, nil
}
