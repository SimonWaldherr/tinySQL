package tinysql

import (
	"context"
	"fmt"
	"strings"
)

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
		return fmt.Sprintf("%s on %s %s %s", rowString(row, "name"), rowString(row, "table"), rowString(row, "timing"), rowString(row, "event"))
	})
	builder.writeOneLineSection("jobs", cfg.MaxJobs, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT name, schedule_type, enabled FROM catalog.jobs ORDER BY name")
	}, func(row Row) string {
		state := "disabled"
		if rowBool(row, "enabled") {
			state = "enabled"
		}
		schedule := rowString(row, "schedule_type")
		if schedule == "" {
			return fmt.Sprintf("%s[%s]", rowString(row, "name"), state)
		}
		return fmt.Sprintf("%s[%s,%s]", rowString(row, "name"), schedule, state)
	})
	builder.writeOneLineSection("connections", cfg.MaxConnections, func() ([]Row, error) {
		return fetchRows(ctx, db, tenant, "SELECT tenant, table_count, total_rows FROM sys.connections ORDER BY tenant")
	}, func(row Row) string {
		return fmt.Sprintf("%s[tables=%s,rows=%s]", rowString(row, "tenant"), rowString(row, "table_count"), rowString(row, "total_rows"))
	})
	builder.writeLine("features: selects; inserts; updates; deletes; joins; group by; having; order by; limit; subqueries; ctes; window functions; views; indexes(metadata); full-text search; triggers; jobs; vector search; json; yaml; url; hash; bitmap; geometry; decimal; money; multi-tenancy; mvcc+wal; sys.* and catalog.* introspection")
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
		parts = append(parts, fmt.Sprintf("%s=%s", rowString(rows[i], "key"), rowString(rows[i], "value")))
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
	return fmt.Sprintf("%v", v)
}

func rowBool(row Row, name string) bool {
	v, ok := GetVal(row, name)
	if !ok || v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return strings.EqualFold(fmt.Sprintf("%v", v), "true")
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
