package tinysql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// VectorCacheConfig configures the optional process-wide VEC_SEARCH result
// cache and its opt-in analytics ring buffer.
type VectorCacheConfig = engine.VectorCacheConfig

// VectorCacheStats reports bounded result-cache state and, when enabled, the
// recent query window. It is a snapshot and safe to serialize for monitoring.
type VectorCacheStats = engine.VectorCacheStats

// VectorQueryEvent describes one recent VEC_SEARCH shape without exposing its
// raw vector contents.
type VectorQueryEvent = engine.VectorQueryEvent

// ConfigureVectorCache replaces the process-wide VEC_SEARCH result-cache and
// analytics configuration. A zero ResultCacheEntries disables result caching.
func ConfigureVectorCache(cfg VectorCacheConfig) { engine.ConfigureVectorCache(cfg) }

// VectorCacheAnalytics returns a point-in-time cache and optional query-window
// snapshot. It does not expose raw query-vector contents.
func VectorCacheAnalytics() VectorCacheStats { return engine.VectorCacheAnalytics() }

// DefaultVectorCacheConfig returns bounded cache/analytics defaults with both
// optional features disabled until explicitly enabled by the caller.
func DefaultVectorCacheConfig() VectorCacheConfig { return engine.DefaultVectorCacheConfig() }

// RAGPreFilter is the public before-ranking authorization/metadata boundary
// for RAG_SEARCH, HYBRID_SEARCH, VEC_SEARCH_FILTERED, and
// FTS_SEARCH_FILTERED. It deliberately represents stable application IDs,
// never mutable physical table-row offsets.
//
// Set IDColumn when AllowedRowIDs refers to a column other than a single-column
// PRIMARY KEY. Equals is an AND of exact metadata values (for example tenant,
// visibility, or document type); it is intersected with AllowedRowIDs when
// both are present. See RAGPreFilterJSON for the SQL options payload.
type RAGPreFilter struct {
	IDColumn      string            `json:"id_column,omitempty"`
	AllowedRowIDs []any             `json:"allowed_row_ids"`
	Equals        map[string]any    `json:"equals"`
	Spatial       *RAGSpatialFilter `json:"spatial,omitempty"`
}

// RAGSpatialFilter limits retrieval to georeferenced rows before ranking.
// Set GeometryColumn and exactly one of BBox or Center+RadiusMeters. Coordinates
// are WGS84 x/y: BBox is [west,south,east,north], Center is [lon,lat]. BBox
// matches geometry extents; radius measures from geometry centroids.
type RAGSpatialFilter struct {
	GeometryColumn string    `json:"geometry_column"`
	BBox           []float64 `json:"bbox,omitempty"`
	Center         []float64 `json:"center,omitempty"`
	RadiusMeters   float64   `json:"radius_meters,omitempty"`
}

// RAGPreFilterJSON encodes filter as the explicit {"pre_filter": ...} JSON
// options object accepted by TinySQL's filtered retrieval functions. It is
// useful for database/sql callers that bind the options JSON as one parameter:
//
//	options, err := tinysql.RAGPreFilterJSON(tinysql.RAGPreFilter{
//		Equals: map[string]any{"tenant_id": "acme"},
//	})
//	rows, err := db.QueryContext(ctx, `SELECT * FROM HYBRID_SEARCH(?, ?, ?, ?, ?, ?, ?)`,
//		"chunks", "embedding", "search_text", query, vector, 8, options)
//
// Unlike an outer WHERE, this boundary is applied before vector/FTS candidate
// selection, reciprocal-rank fusion, and neighbor-context expansion.
func RAGPreFilterJSON(filter RAGPreFilter) (string, error) {
	if filter.AllowedRowIDs == nil && len(filter.Equals) == 0 && filter.Spatial == nil {
		return "", fmt.Errorf("RAG pre-filter requires AllowedRowIDs, at least one Equals value, or Spatial")
	}
	if filter.AllowedRowIDs == nil && strings.TrimSpace(filter.IDColumn) != "" {
		return "", fmt.Errorf("RAG pre-filter IDColumn requires AllowedRowIDs")
	}
	if spatial := filter.Spatial; spatial != nil {
		if strings.TrimSpace(spatial.GeometryColumn) == "" {
			return "", fmt.Errorf("RAG spatial pre-filter requires GeometryColumn")
		}
		bboxMode := spatial.BBox != nil
		radiusMode := spatial.Center != nil || spatial.RadiusMeters != 0
		if bboxMode == radiusMode {
			return "", fmt.Errorf("RAG spatial pre-filter requires exactly one of BBox or Center with RadiusMeters")
		}
		finite := func(values []float64) bool {
			for _, value := range values {
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return false
				}
			}
			return true
		}
		if bboxMode && (len(spatial.BBox) != 4 || !finite(spatial.BBox)) {
			return "", fmt.Errorf("RAG spatial pre-filter BBox must contain four finite values")
		}
		if bboxMode && (spatial.BBox[0] < -180 || spatial.BBox[0] > 180 || spatial.BBox[2] < -180 || spatial.BBox[2] > 180 || spatial.BBox[1] < -90 || spatial.BBox[3] > 90 || spatial.BBox[1] > spatial.BBox[3]) {
			return "", fmt.Errorf("RAG spatial pre-filter BBox requires WGS84 west/east and ordered south/north bounds")
		}
		if radiusMode && (len(spatial.Center) != 2 || !finite(spatial.Center) || spatial.RadiusMeters <= 0 || math.IsNaN(spatial.RadiusMeters) || math.IsInf(spatial.RadiusMeters, 0)) {
			return "", fmt.Errorf("RAG spatial pre-filter requires a finite two-value Center and positive finite RadiusMeters")
		}
		if radiusMode && (spatial.Center[0] < -180 || spatial.Center[0] > 180 || spatial.Center[1] < -90 || spatial.Center[1] > 90) {
			return "", fmt.Errorf("RAG spatial pre-filter Center requires WGS84 longitude/latitude")
		}
	}

	// A pointer to the slice preserves the important distinction between a nil
	// AllowedRowIDs (not supplied) and []any{} (an explicit deny-all ACL), while
	// omitting unrelated null fields from the generated SQL options object.
	type encodedFilter struct {
		IDColumn      string            `json:"id_column,omitempty"`
		AllowedRowIDs *[]any            `json:"allowed_row_ids,omitempty"`
		Equals        map[string]any    `json:"equals,omitempty"`
		Spatial       *RAGSpatialFilter `json:"spatial,omitempty"`
	}
	var allowedRowIDs *[]any
	if filter.AllowedRowIDs != nil {
		allowedRowIDs = &filter.AllowedRowIDs
	}
	value := struct {
		PreFilter encodedFilter `json:"pre_filter"`
	}{PreFilter: encodedFilter{
		IDColumn:      filter.IDColumn,
		AllowedRowIDs: allowedRowIDs,
		Equals:        filter.Equals,
		Spatial:       filter.Spatial,
	}}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode RAG pre-filter: %w", err)
	}
	return string(raw), nil
}

// --- Agent context ---

// AgentContextConfig controls how much live database metadata is exported
// for AI agents. Larger values produce denser prompts; smaller values keep
// the context bounded.
type AgentContextConfig struct {
	MaxTables          int
	MaxColumnsPerTable int
	MaxRelations       int
	MaxFunctions       int
	MaxViews           int
	MaxTriggers        int
	MaxJobs            int
	MaxConnections     int
	MaxChars           int
}

// DefaultAgentContextConfig returns a balanced profile that stays dense while
// still fitting comfortably in a typical model context window.
func DefaultAgentContextConfig() AgentContextConfig {
	return AgentContextConfig{
		MaxTables:          12,
		MaxColumnsPerTable: 6,
		MaxRelations:       12,
		MaxFunctions:       16,
		MaxViews:           8,
		MaxTriggers:        8,
		MaxJobs:            8,
		MaxConnections:     8,
		MaxChars:           6000,
	}
}

func (cfg AgentContextConfig) normalized() AgentContextConfig {
	defaults := DefaultAgentContextConfig()
	if cfg.MaxTables <= 0 {
		cfg.MaxTables = defaults.MaxTables
	}
	if cfg.MaxColumnsPerTable <= 0 {
		cfg.MaxColumnsPerTable = defaults.MaxColumnsPerTable
	}
	if cfg.MaxRelations <= 0 {
		cfg.MaxRelations = defaults.MaxRelations
	}
	if cfg.MaxFunctions <= 0 {
		cfg.MaxFunctions = defaults.MaxFunctions
	}
	if cfg.MaxViews <= 0 {
		cfg.MaxViews = defaults.MaxViews
	}
	if cfg.MaxTriggers <= 0 {
		cfg.MaxTriggers = defaults.MaxTriggers
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = defaults.MaxJobs
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = defaults.MaxConnections
	}
	if cfg.MaxChars <= 0 {
		cfg.MaxChars = defaults.MaxChars
	}
	return cfg
}

// BuildAgentContext returns a compact, prompt-ready database profile for AI
// agents. It is based on live sys.* / catalog.* metadata and can be tuned with
// AgentContextConfig to trade breadth for token budget.
func BuildAgentContext(ctx context.Context, db *DB, tenant string, cfg AgentContextConfig) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg = cfg.normalized()
	if tenant == "" {
		tenant = "default"
	}

	builder := newAgentContextBuilder(cfg)
	builder.writeLine("tinySQL agent profile")

	vars, err := fetchKeyValueRows(ctx, db, tenant, "SELECT name, value FROM sys.variables ORDER BY name")
	if err != nil {
		return "", err
	}
	builder.writeLine(fmt.Sprintf(
		"version: %s | storage: %s | tenant: %s",
		valueOrUnknown(vars, "version"),
		valueOrUnknown(vars, "storage_mode"),
		tenant,
	))
	builder.writeLine(fmt.Sprintf(
		"runtime: go=%s | os=%s | arch=%s",
		valueOrUnknown(vars, "go_version"),
		valueOrUnknown(vars, "os"),
		valueOrUnknown(vars, "arch"),
	))

	if cfg.MaxChars > 0 {
		builder.writeKeyValueSection("config", cfg.MaxChars/8, func() ([]Row, error) {
			return fetchRows(ctx, db, tenant, "SELECT key, value FROM sys.config ORDER BY key")
		})
		builder.writeKeyValueSection("status", cfg.MaxChars/8, func() ([]Row, error) {
			return fetchRows(ctx, db, tenant, "SELECT key, value FROM sys.status ORDER BY key")
		})
	}

	tables, relations, err := collectAgentTableData(ctx, db, tenant, cfg)
	if err != nil {
		return "", err
	}
	builder.writeTableSection(tables)
	builder.writeRelationSection(relations)
	builder.writeOneLineSection("views", cfg.MaxViews, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT schema, name FROM sys.views ORDER BY schema, name")
	}, func(row Row) string {
		schema := rowString(row, "schema")
		name := rowString(row, "name")
		if schema != "" && schema != "main" {
			return schema + "." + name
		}
		return name
	})
	builder.writeOneLineSection("functions", cfg.MaxFunctions, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT name, function_type FROM sys.functions ORDER BY name")
	}, func(row Row) string {
		name := rowString(row, "name")
		fnType := strings.ToLower(rowString(row, "function_type"))
		if fnType == "" {
			return name
		}
		return fmt.Sprintf("%s[%s]", name, fnType)
	})
	builder.writeOneLineSection("triggers", cfg.MaxTriggers, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT name, table, timing, event FROM sys.triggers ORDER BY name")
	}, func(row Row) string {
		name, table, timing, event := rowString(row, "name"), rowString(row, "table"), rowString(row, "timing"), rowString(row, "event")
		return fmt.Sprintf("%s on %s %s %s", name, table, timing, event)
	})
	builder.writeOneLineSection("jobs", cfg.MaxJobs, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT name, schedule_type, enabled FROM catalog.jobs ORDER BY name")
	}, func(row Row) string {
		state := "disabled"
		if rowBool(row, "enabled") {
			state = "enabled"
		}
		name, schedule := rowString(row, "name"), rowString(row, "schedule_type")
		if schedule == "" {
			return fmt.Sprintf("%s[%s]", name, state)
		}
		return fmt.Sprintf("%s[%s,%s]", name, schedule, state)
	})
	builder.writeOneLineSection("connections", cfg.MaxConnections, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT tenant, table_count, total_rows FROM sys.connections ORDER BY tenant")
	}, func(row Row) string {
		return fmt.Sprintf("%s[tables=%s,rows=%s]", rowString(row, "tenant"), rowString(row, "table_count"), rowString(row, "total_rows"))
	})
	builder.writeLine("features: selects; inserts; updates; deletes; joins; group by; having; order by; limit; subqueries; ctes; window functions; views; indexes(metadata); full-text search; hybrid/spatial RAG; triggers; jobs; vector search; geometry/GIS; WMS/WMTS tile cover; json; yaml; url; hash; bitmap; decimal; money; multi-tenancy; mvcc+wal; sys.* and catalog.* introspection")
	builder.writeLine("gaps: foreign key enforcement; check constraints; returning; upsert; savepoints; attach/detach; partial indexes; vacuum; sqlite_master")

	return builder.String(), nil
}

func fetchRows(ctx context.Context, db *DB, tenant, sql string) ([]Row, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", sql, err)
	}
	rs, err := Execute(ctx, db, tenant, stmt)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return rs.Rows, nil
}

func fetchKeyValueRows(ctx context.Context, db *DB, tenant, sql string) (map[string]string, error) {
	rows, err := fetchRows(ctx, db, tenant, sql)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, row := range rows {
		out[strings.ToLower(rowString(row, "name"))] = rowString(row, "value")
	}
	return out, nil
}

func collectAgentTableData(ctx context.Context, db *DB, tenant string, cfg AgentContextConfig) ([]agentTableSummary, []agentRelationSummary, error) {
	tableRows, err := fetchRows(ctx, db, tenant, "SELECT tenant, name, columns, rows, is_temp, version FROM sys.tables ORDER BY rows DESC, tenant, name")
	if err != nil {
		return nil, nil, fmt.Errorf("collect tables: %w", err)
	}

	tables := make(map[string]*agentTableSummary, len(tableRows))
	tableOrder := make([]string, 0, len(tableRows))
	for _, row := range tableRows {
		tn := rowString(row, "tenant")
		name := rowString(row, "name")
		key := tn + "." + name
		tables[key] = &agentTableSummary{
			Tenant:  tn,
			Name:    name,
			Rows:    rowString(row, "rows"),
			Columns: rowString(row, "columns"),
			Temp:    rowBool(row, "is_temp"),
			Version: rowString(row, "version"),
		}
		tableOrder = append(tableOrder, key)
	}

	columnRows, err := fetchRows(ctx, db, tenant, "SELECT tenant, table_name, name, position, data_type, constraint, fk_table, fk_column FROM sys.columns ORDER BY tenant, table_name, position")
	if err != nil {
		return nil, nil, fmt.Errorf("collect columns: %w", err)
	}

	relations := make([]agentRelationSummary, 0, cfg.MaxRelations)
	for _, row := range columnRows {
		tn := rowString(row, "tenant")
		tableName := rowString(row, "table_name")
		key := tn + "." + tableName
		table := tables[key]
		if table == nil {
			continue
		}

		colDesc := fmt.Sprintf("%s:%s", rowString(row, "name"), rowString(row, "data_type"))
		if strings.EqualFold(rowString(row, "constraint"), "PRIMARY KEY") {
			colDesc += " pk"
		}
		fkTable := rowString(row, "fk_table")
		fkColumn := rowString(row, "fk_column")
		if fkTable != "" {
			colDesc += fmt.Sprintf(" fk->%s.%s", fkTable, fkColumn)
			relations = append(relations, agentRelationSummary{
				FromTable:  tableLabel(tn, tableName),
				FromColumn: rowString(row, "name"),
				ToTable:    tableLabel(tn, fkTable),
				ToColumn:   fkColumn,
			})
		}
		if len(table.ColumnsS) < cfg.MaxColumnsPerTable {
			table.ColumnsS = append(table.ColumnsS, colDesc)
		}
	}

	resultTables := make([]agentTableSummary, 0, minInt(cfg.MaxTables, len(tableOrder)))
	for i, key := range tableOrder {
		if i >= cfg.MaxTables {
			break
		}
		if table := tables[key]; table != nil {
			resultTables = append(resultTables, *table)
		}
	}
	if len(relations) > cfg.MaxRelations {
		relations = relations[:cfg.MaxRelations]
	}

	return resultTables, relations, nil
}

type agentTableSummary struct {
	Tenant   string
	Name     string
	Rows     string
	Columns  string
	Temp     bool
	Version  string
	ColumnsS []string
}

type agentRelationSummary struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

type agentContextBuilder struct {
	cfg       AgentContextConfig
	builder   strings.Builder
	truncated bool
}

func newAgentContextBuilder(cfg AgentContextConfig) *agentContextBuilder {
	return &agentContextBuilder{cfg: cfg}
}

func (b *agentContextBuilder) writeLine(line string) {
	if b.truncated {
		return
	}
	if b.cfg.MaxChars > 0 && b.builder.Len()+len(line)+1 > b.cfg.MaxChars {
		marker := "... context truncated to stay within the configured budget ..."
		if b.builder.Len()+len(marker)+1 <= b.cfg.MaxChars {
			if b.builder.Len() > 0 {
				b.builder.WriteByte('\n')
			}
			b.builder.WriteString(marker)
		}
		b.truncated = true
		return
	}
	if b.builder.Len() > 0 {
		b.builder.WriteByte('\n')
	}
	b.builder.WriteString(line)
}

func (b *agentContextBuilder) writeTableSection(tables []agentTableSummary) {
	if len(tables) == 0 {
		b.writeLine("tables: none")
		return
	}
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		label := tableLabel(table.Tenant, table.Name)
		cols := strings.Join(table.ColumnsS, ", ")
		if cols == "" {
			cols = "..."
		}
		temp := ""
		if table.Temp {
			temp = " temp"
		}
		parts = append(parts, fmt.Sprintf("%s rows=%s cols=%s%s [%s]", label, table.Rows, table.Columns, temp, cols))
	}
	b.writeLine(fmt.Sprintf("tables(%d): %s", len(parts), strings.Join(parts, " | ")))
}

func (b *agentContextBuilder) writeRelationSection(relations []agentRelationSummary) {
	if len(relations) == 0 {
		b.writeLine("relations: none")
		return
	}
	parts := make([]string, 0, len(relations))
	for _, relation := range relations {
		parts = append(parts, fmt.Sprintf("%s.%s -> %s.%s", relation.FromTable, relation.FromColumn, relation.ToTable, relation.ToColumn))
	}
	b.writeLine(fmt.Sprintf("relations(%d): %s", len(parts), strings.Join(parts, "; ")))
}

func (b *agentContextBuilder) writeOneLineSection(name string, limit int, fetch func() ([]Row, error), render func(Row) string) {
	rows, err := fetch()
	if err != nil {
		b.writeLine(fmt.Sprintf("%s: error: %v", name, err))
		return
	}
	if len(rows) == 0 {
		b.writeLine(fmt.Sprintf("%s: none", name))
		return
	}
	if limit > len(rows) {
		limit = len(rows)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, render(rows[i]))
	}
	line := fmt.Sprintf("%s(%d/%d): %s", name, len(parts), len(rows), strings.Join(parts, ", "))
	if len(parts) < len(rows) {
		line += fmt.Sprintf(" ... +%d more", len(rows)-len(parts))
	}
	b.writeLine(line)
}

func (b *agentContextBuilder) writeKeyValueSection(name string, limit int, fetch func() ([]Row, error)) {
	rows, err := fetch()
	if err != nil {
		b.writeLine(fmt.Sprintf("%s: error: %v", name, err))
		return
	}
	if len(rows) == 0 {
		b.writeLine(fmt.Sprintf("%s: none", name))
		return
	}
	if limit > len(rows) {
		limit = len(rows)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		key, val := rowString(rows[i], "key"), rowString(rows[i], "value")
		parts = append(parts, fmt.Sprintf("%s=%s", key, val))
	}
	line := fmt.Sprintf("%s(%d/%d): %s", name, len(parts), len(rows), strings.Join(parts, ", "))
	if len(parts) < len(rows) {
		line += fmt.Sprintf(" ... +%d more", len(rows)-len(parts))
	}
	b.writeLine(line)
}

func (b *agentContextBuilder) String() string {
	return b.builder.String()
}

func rowString(row Row, name string) string {
	v, ok := GetVal(row, name)
	if !ok || v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func rowBool(row Row, name string) bool {
	v, ok := GetVal(row, name)
	if !ok || v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return strings.EqualFold(fmt.Sprint(v), "true")
}

func valueOrUnknown(values map[string]string, key string) string {
	if value, ok := values[strings.ToLower(key)]; ok && value != "" {
		return value
	}
	return "unknown"
}

func tableLabel(tenant, name string) string {
	if tenant != "" && tenant != "default" {
		return tenant + "." + name
	}
	return name
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
