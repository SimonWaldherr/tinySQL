//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/geoencoding"
	"github.com/SimonWaldherr/tinySQL/internal/gpkg"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	geoPackageApplicationID int64 = 0x47504b47 // ASCII "GPKG", GeoPackage >= 1.2
	geoPackage10ID          int64 = 0x47503130 // ASCII "GP10"
	geoPackage11ID          int64 = 0x47503131 // ASCII "GP11"
)

type geoPackageColumn struct {
	Name         string
	DeclaredType string
	PrimaryKey   bool
}

// InspectGeoPackage reads only OGC catalog tables and returns the available
// vector feature layers. It never contacts an external service.
func InspectGeoPackage(ctx context.Context, filePath string) (*GeoPackageInfo, error) {
	source, err := openGeoPackage(ctx, filePath)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	return inspectGeoPackageDB(ctx, source)
}

func openGeoPackage(ctx context.Context, filePath string) (*sql.DB, error) {
	clean := filepath.Clean(filePath)
	dsn := "file:" + filepath.ToSlash(clean) + "?mode=ro&immutable=1"
	source, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open GeoPackage: %w", err)
	}
	if err := source.PingContext(ctx); err != nil {
		_ = source.Close()
		return nil, fmt.Errorf("open GeoPackage: %w", err)
	}
	return source, nil
}

func inspectGeoPackageDB(ctx context.Context, source *sql.DB) (*GeoPackageInfo, error) {
	info := &GeoPackageInfo{}
	if err := source.QueryRowContext(ctx, "PRAGMA application_id").Scan(&info.ApplicationID); err != nil {
		return nil, fmt.Errorf("read GeoPackage application_id: %w", err)
	}
	if info.ApplicationID != geoPackageApplicationID && info.ApplicationID != geoPackage10ID && info.ApplicationID != geoPackage11ID {
		return nil, fmt.Errorf("not an OGC GeoPackage: application_id is 0x%x, want GPKG, GP10, or GP11", info.ApplicationID)
	}
	if err := source.QueryRowContext(ctx, "PRAGMA user_version").Scan(&info.UserVersion); err != nil {
		return nil, fmt.Errorf("read GeoPackage user_version: %w", err)
	}
	switch info.ApplicationID {
	case geoPackage10ID:
		info.Version = "1.0"
	case geoPackage11ID:
		info.Version = "1.1"
	case geoPackageApplicationID:
		if info.UserVersion < 10200 {
			return nil, fmt.Errorf("invalid OGC GeoPackage: GPKG application_id requires user_version >= 10200, got %d", info.UserVersion)
		}
		info.Version = fmt.Sprintf("%d.%d.%d", info.UserVersion/10000, (info.UserVersion/100)%100, info.UserVersion%100)
	}

	definitionColumn := "s.definition"
	if sqliteTableHasColumn(ctx, source, "gpkg_spatial_ref_sys", "definition_12_063") {
		// OGC's CRS WKT extension carries ISO 19162/WKT2 in this column. Prefer
		// it over the mandatory legacy definition while retaining compatibility
		// with base GeoPackage 1.x files that do not implement the extension.
		definitionColumn = "COALESCE(s.definition_12_063, s.definition)"
	}

	catalogQuery := `
		SELECT c.table_name, COALESCE(c.identifier, ''), COALESCE(c.description, ''),
		       COALESCE(c.last_change, ''), c.min_x, c.min_y, c.max_x, c.max_y,
		       g.column_name, g.geometry_type_name, g.srs_id, g.z, g.m,
		       COALESCE(s.srs_name, ''), COALESCE(s.organization, ''),
		       COALESCE(s.organization_coordsys_id, 0), COALESCE(` + definitionColumn + `, '')
		FROM gpkg_contents AS c
		JOIN gpkg_geometry_columns AS g ON g.table_name = c.table_name
		LEFT JOIN gpkg_spatial_ref_sys AS s ON s.srs_id = g.srs_id
		WHERE lower(c.data_type) = 'features'
		ORDER BY c.table_name`
	rows, err := source.QueryContext(ctx, catalogQuery)
	if err != nil {
		return nil, fmt.Errorf("read GeoPackage feature catalog: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var layer GeoPackageLayer
		var minX, minY, maxX, maxY sql.NullFloat64
		if err := rows.Scan(
			&layer.TableName, &layer.Identifier, &layer.Description, &layer.LastChange,
			&minX, &minY, &maxX, &maxY,
			&layer.GeometryColumn, &layer.GeometryType, &layer.SRID, &layer.Z, &layer.M,
			&layer.SRSName, &layer.SRSOrganization, &layer.SRSOrganizationID, &layer.SRSDefinition,
		); err != nil {
			return nil, fmt.Errorf("scan GeoPackage feature catalog: %w", err)
		}
		if layer.Z < 0 || layer.Z > 2 || layer.M < 0 || layer.M > 2 {
			return nil, fmt.Errorf("GeoPackage layer %q has invalid z/m flags %d/%d", layer.TableName, layer.Z, layer.M)
		}
		layer.MinX, layer.MinY, layer.MaxX, layer.MaxY = nullFloatPointer(minX), nullFloatPointer(minY), nullFloatPointer(maxX), nullFloatPointer(maxY)
		info.Layers = append(info.Layers, layer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read GeoPackage feature catalog: %w", err)
	}
	return info, nil
}

func sqliteTableHasColumn(ctx context.Context, source *sql.DB, tableName, columnName string) bool {
	var count int
	err := source.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info(?) WHERE lower(name) = lower(?)`, tableName, columnName).Scan(&count)
	return err == nil && count > 0
}

func nullFloatPointer(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	v := value.Float64
	return &v
}

// ImportGeoPackage imports one standards-discovered vector feature layer.
// See ImportOptions.GeoPackageLayer and GeoPackageGeometryMode.
func ImportGeoPackage(
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

	source, err := openGeoPackage(ctx, filePath)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	info, err := inspectGeoPackageDB(ctx, source)
	if err != nil {
		return nil, err
	}
	layer, err := selectGeoPackageLayer(info.Layers, opts.GeoPackageLayer)
	if err != nil {
		return nil, err
	}
	if tableName == "" {
		tableName = sanitizeTableName(layer.TableName)
	}
	return importGeoPackageLayer(ctx, db, tenant, tableName, source, info, layer, opts)
}

// ImportGeoPackageReader spools a standards-compliant GeoPackage stream to a
// temporary random-access file, then imports it like ImportGeoPackage.
func ImportGeoPackageReader(
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
	tmp, err := os.CreateTemp("", "tinysql-geopackage-*.gpkg")
	if err != nil {
		return nil, fmt.Errorf("create temporary GeoPackage: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	limited := limitInput(ctx, src, opts)
	if _, err := io.Copy(tmp, limited); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("write temporary GeoPackage: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("close temporary GeoPackage: %w", err)
	}
	return ImportGeoPackage(ctx, db, tenant, tableName, tmpName, opts)
}

func selectGeoPackageLayer(layers []GeoPackageLayer, requested string) (GeoPackageLayer, error) {
	requested = strings.TrimSpace(requested)
	if requested != "" {
		for _, layer := range layers {
			if layer.TableName == requested {
				return layer, nil
			}
		}
		return GeoPackageLayer{}, fmt.Errorf("GeoPackage feature layer %q not found; available layers: %s", requested, geoPackageLayerNames(layers))
	}
	switch len(layers) {
	case 0:
		return GeoPackageLayer{}, fmt.Errorf("GeoPackage contains no vector feature layer")
	case 1:
		return layers[0], nil
	default:
		return GeoPackageLayer{}, fmt.Errorf("GeoPackage contains %d feature layers; set ImportOptions.GeoPackageLayer to one of: %s", len(layers), geoPackageLayerNames(layers))
	}
}

func geoPackageLayerNames(layers []GeoPackageLayer) string {
	names := make([]string, len(layers))
	for i := range layers {
		names[i] = layers[i].TableName
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func importGeoPackageLayer(
	ctx context.Context,
	db *storage.DB,
	tenant, tableName string,
	source *sql.DB,
	info *GeoPackageInfo,
	layer GeoPackageLayer,
	opts *ImportOptions,
) (*ImportResult, error) {
	columns, err := geoPackageColumns(ctx, source, layer.TableName)
	if err != nil {
		return nil, err
	}
	geometryIndex := -1
	sourceNames := make([]string, len(columns))
	for i, column := range columns {
		sourceNames[i] = column.Name
		if column.Name == layer.GeometryColumn {
			geometryIndex = i
		}
	}
	if geometryIndex < 0 {
		return nil, fmt.Errorf("GeoPackage layer %q geometry column %q does not exist", layer.TableName, layer.GeometryColumn)
	}
	columnNames := uniqueSanitizedColumnNames(sourceNames)
	mode, geometryType, err := geoPackageGeometryMode(opts.GeoPackageGeometryMode, layer.SRID)
	if err != nil {
		return nil, fmt.Errorf("GeoPackage layer %q: %w", layer.TableName, err)
	}
	columnTypes := make([]storage.ColType, len(columns))
	for i, column := range columns {
		columnTypes[i] = sqliteDeclaredColumnType(column.DeclaredType)
	}
	columnTypes[geometryIndex] = geometryType

	result := &ImportResult{
		Encoding: "OGC GeoPackage", Errors: make([]string, 0),
		ColumnNames: columnNames, ColumnTypes: columnTypes,
	}
	query := "SELECT * FROM " + quoteSQLiteIdentifier(layer.TableName)
	sourceRows, err := source.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query GeoPackage layer %q: %w", layer.TableName, err)
	}
	defer sourceRows.Close()

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	appendOpts := *opts
	appendOpts.CreateTable = false
	appendOpts.Truncate = false
	batchOpts := opts
	batch := make([][]any, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		batchResult := &ImportResult{Encoding: result.Encoding, Errors: make([]string, 0), ColumnNames: columnNames, ColumnTypes: columnTypes}
		if err := insertTypedRows(ctx, db, tenant, tableName, columnNames, columnTypes, batch, batchOpts, batchResult); err != nil {
			return err
		}
		result.RowsInserted += batchResult.RowsInserted
		result.RowsSkipped += batchResult.RowsSkipped
		result.Errors = append(result.Errors, batchResult.Errors...)
		batch = batch[:0]
		batchOpts = &appendOpts
		return nil
	}

	rowNumber := int64(0)
	for sourceRows.Next() {
		rowNumber++
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := sourceRows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan GeoPackage layer %q row %d: %w", layer.TableName, rowNumber, err)
		}
		row, err := convertGeoPackageRow(values, columnTypes, geometryIndex, layer, mode)
		if err != nil {
			message := fmt.Sprintf("row %d: %v", rowNumber, err)
			if opts.StrictTypes {
				return nil, fmt.Errorf("GeoPackage layer %q: %s", layer.TableName, message)
			}
			result.RowsSkipped++
			result.Errors = append(result.Errors, message)
			continue
		}
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
	}
	if err := sourceRows.Err(); err != nil {
		return nil, fmt.Errorf("read GeoPackage layer %q: %w", layer.TableName, err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if result.RowsInserted == 0 && opts.CreateTable {
		if err := insertTypedRows(ctx, db, tenant, tableName, columnNames, columnTypes, nil, opts, result); err != nil {
			return nil, err
		}
	}
	if err := importGeoPackageMetadata(ctx, db, tenant, tableName+"_metadata", info, layer, mode, opts); err != nil {
		return nil, err
	}
	return result, nil
}

func geoPackageColumns(ctx context.Context, source *sql.DB, tableName string) ([]geoPackageColumn, error) {
	rows, err := source.QueryContext(ctx, `SELECT name, type, pk FROM pragma_table_info(?) ORDER BY cid`, tableName)
	if err != nil {
		return nil, fmt.Errorf("read GeoPackage layer %q columns: %w", tableName, err)
	}
	defer rows.Close()
	var columns []geoPackageColumn
	for rows.Next() {
		var column geoPackageColumn
		var pk int
		if err := rows.Scan(&column.Name, &column.DeclaredType, &pk); err != nil {
			return nil, fmt.Errorf("scan GeoPackage layer %q columns: %w", tableName, err)
		}
		column.PrimaryKey = pk > 0
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read GeoPackage layer %q columns: %w", tableName, err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("GeoPackage layer %q has no columns", tableName)
	}
	return columns, nil
}

func uniqueSanitizedColumnNames(names []string) []string {
	out := sanitizeColumnNames(names)
	seen := make(map[string]int, len(out))
	for i, name := range out {
		key := strings.ToLower(name)
		seen[key]++
		if seen[key] == 1 {
			continue
		}
		base := name
		for suffix := seen[key]; ; suffix++ {
			candidate := base + "_" + strconv.Itoa(suffix)
			candidateKey := strings.ToLower(candidate)
			if seen[candidateKey] == 0 {
				out[i] = candidate
				seen[candidateKey] = 1
				break
			}
		}
	}
	return out
}

func sqliteDeclaredColumnType(declared string) storage.ColType {
	upper := strings.ToUpper(strings.TrimSpace(declared))
	switch {
	case strings.Contains(upper, "BOOL"):
		return storage.BoolType
	case strings.Contains(upper, "INT"):
		return storage.Int64Type
	case strings.Contains(upper, "REAL"), strings.Contains(upper, "FLOA"), strings.Contains(upper, "DOUB"):
		return storage.Float64Type
	case strings.Contains(upper, "BLOB") || upper == "":
		return storage.BlobType
	default:
		// GeoPackage TEXT, DATE and DATETIME values all have normative textual
		// encodings. Preserve them exactly instead of losing precision or zone
		// information through an eager time conversion.
		return storage.TextType
	}
}

func geoPackageGeometryMode(requested string, srid int64) (string, storage.ColType, error) {
	mode := strings.ToLower(strings.TrimSpace(requested))
	if mode == "" {
		mode = "auto"
	}
	switch mode {
	case "auto":
		if srid == 4326 {
			return "geojson", storage.GeometryType, nil
		}
		return "gpkg", storage.BlobType, nil
	case "geojson":
		if srid != 4326 {
			return "", 0, fmt.Errorf("geojson mode requires EPSG:4326, got SRID %d; reproject explicitly before RFC 7946 conversion", srid)
		}
		return mode, storage.GeometryType, nil
	case "wkb":
		return mode, storage.BlobType, nil
	case "gpkg", "native":
		return "gpkg", storage.BlobType, nil
	default:
		return "", 0, fmt.Errorf("GeoPackageGeometryMode must be auto, geojson, wkb, gpkg, or native")
	}
}

func convertGeoPackageRow(values []any, columnTypes []storage.ColType, geometryIndex int, layer GeoPackageLayer, mode string) ([]any, error) {
	row := make([]any, len(values))
	for i, value := range values {
		if i == geometryIndex {
			converted, err := convertGeoPackageGeometry(value, layer, mode)
			if err != nil {
				return nil, err
			}
			row[i] = converted
			continue
		}
		switch value := value.(type) {
		case []byte:
			if columnTypes[i] == storage.TextType {
				row[i] = string(value)
			} else {
				row[i] = append([]byte(nil), value...)
			}
		case int64:
			if columnTypes[i] == storage.BoolType {
				row[i] = value != 0
			} else {
				row[i] = value
			}
		default:
			row[i] = value
		}
	}
	return row, nil
}

func convertGeoPackageGeometry(value any, layer GeoPackageLayer, mode string) (any, error) {
	if value == nil {
		return nil, nil
	}
	body, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("geometry column %q is %T, want GeoPackageBinary BLOB", layer.GeometryColumn, value)
	}
	geometry, err := gpkg.ParseGeometry(body)
	if err != nil {
		return nil, err
	}
	if int64(geometry.SRID) != layer.SRID {
		return nil, fmt.Errorf("geometry SRID %d does not match gpkg_geometry_columns SRID %d", geometry.SRID, layer.SRID)
	}
	switch mode {
	case "gpkg":
		return append([]byte(nil), body...), nil
	case "wkb":
		return append([]byte(nil), geometry.WKB...), nil
	case "geojson":
		if geometry.Empty {
			return nil, nil
		}
		decoded, err := geoencoding.DecodeWKB(geometry.WKB)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(decoded.Geometry)
		if err != nil {
			return nil, fmt.Errorf("encode GeoJSON geometry: %w", err)
		}
		return json.RawMessage(encoded), nil
	default:
		return nil, fmt.Errorf("unsupported internal geometry mode %q", mode)
	}
}

func importGeoPackageMetadata(ctx context.Context, db *storage.DB, tenant, tableName string, info *GeoPackageInfo, layer GeoPackageLayer, mode string, opts *ImportOptions) error {
	rows := [][]any{
		{"format", "OGC GeoPackage"},
		{"application_id", fmt.Sprintf("0x%08x", info.ApplicationID)},
		{"user_version", strconv.Itoa(info.UserVersion)},
		{"geopackage_version", info.Version},
		{"source_layer", layer.TableName},
		{"identifier", layer.Identifier},
		{"description", layer.Description},
		{"last_change", layer.LastChange},
		{"geometry_column", layer.GeometryColumn},
		{"geometry_type", layer.GeometryType},
		{"geometry_encoding", mode},
		{"srs_id", strconv.FormatInt(layer.SRID, 10)},
		{"srs_name", layer.SRSName},
		{"srs_organization", layer.SRSOrganization},
		{"srs_organization_coordsys_id", strconv.FormatInt(layer.SRSOrganizationID, 10)},
		{"srs_definition", layer.SRSDefinition},
	}
	if layer.MinX != nil && layer.MinY != nil && layer.MaxX != nil && layer.MaxY != nil {
		bounds, _ := json.Marshal([]float64{*layer.MinX, *layer.MinY, *layer.MaxX, *layer.MaxY})
		rows = append(rows, []any{"bounds", string(bounds)})
	}
	metadataOpts := *opts
	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: []string{"name", "value"}, ColumnTypes: []storage.ColType{storage.TextType, storage.TextType}}
	return insertTypedRows(ctx, db, tenant, tableName, result.ColumnNames, result.ColumnTypes, rows, &metadataOpts, result)
}

func quoteSQLiteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
