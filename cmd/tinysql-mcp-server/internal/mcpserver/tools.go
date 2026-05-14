package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"tinysql-mcp-server/internal/tinysqldb"
)

// ─── input/output schemas ─────────────────────────────────────────────────────

// ReadQueryArgs is the input schema for the read_query tool.
type ReadQueryArgs struct {
	Query string `json:"query" jsonschema:"SQL SELECT (or CTE) statement to execute"`
}

// WriteQueryArgs is the input schema for the write_query tool.
type WriteQueryArgs struct {
	Query string `json:"query" jsonschema:"Mutating SQL statement (INSERT, UPDATE, DELETE)"`
}

// CreateTableArgs is the input schema for the create_table tool.
type CreateTableArgs struct {
	Query string `json:"query" jsonschema:"CREATE TABLE statement"`
}

// DescribeTableArgs is the input schema for the describe_table tool.
type DescribeTableArgs struct {
	TableName string `json:"table_name" jsonschema:"Name of the table to describe"`
}

// AppendInsightArgs is the input schema for the append_insight tool.
type AppendInsightArgs struct {
	Insight string `json:"insight" jsonschema:"Analytical observation to record"`
}

// AgentContextArgs is the input schema for the agent_context tool.
type AgentContextArgs struct {
	MaxTables int `json:"max_tables,omitempty" jsonschema:"Maximum number of tables to include (default 8)"`
	MaxChars  int `json:"max_chars,omitempty"  jsonschema:"Maximum character budget for the context (default 4000)"`
}

// SampleTableArgs is the input schema for the sample_table tool.
type SampleTableArgs struct {
	TableName string `json:"table_name" jsonschema:"Name of the table to sample"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum number of rows to return (default 10)"`
}

// ─── server ────────────────────────────────────────────────────────────────────

// Server wraps an MCP server and the tinySQL store, wiring tools, resources,
// and prompts together.
type Server struct {
	store    *tinysqldb.Store
	insights *InsightStore
	mcpSrv   *mcp.Server
}

// New creates a new Server wrapping store.
func New(store *tinysqldb.Store) *Server {
	s := &Server{
		store:    store,
		insights: &InsightStore{},
	}
	s.mcpSrv = s.buildMCPServer()
	return s
}

// Run starts the MCP server on the stdio transport and blocks until ctx is done
// or the peer disconnects.
func (s *Server) Run(ctx context.Context) error {
	return s.mcpSrv.Run(ctx, &mcp.StdioTransport{})
}

// MCP returns the underlying *mcp.Server (useful for testing).
func (s *Server) MCP() *mcp.Server { return s.mcpSrv }

// ─── tool handlers ─────────────────────────────────────────────────────────────

// HandleReadQuery executes a read-only SQL statement and returns results.
func (s *Server) HandleReadQuery(ctx context.Context, args ReadQueryArgs) (*mcp.CallToolResult, error) {
	kind := classifySQL(args.Query)
	if !kind.isReadOnly() {
		return toolErrorf("read_query only accepts SELECT statements; got %s", describeKind(kind))
	}

	rows, cols, truncated, err := s.queryRows(ctx, args.Query)
	if err != nil {
		return toolErrorf("query failed: %v", err)
	}

	return queryResultContent(rows, cols, truncated)
}

// HandleWriteQuery executes a mutating SQL statement (INSERT/UPDATE/DELETE).
func (s *Server) HandleWriteQuery(ctx context.Context, args WriteQueryArgs) (*mcp.CallToolResult, error) {
	if s.store.Cfg.ReadOnly {
		return toolErrorf("server is in read-only mode")
	}

	kind := classifySQL(args.Query)
	if !kind.isMutating() {
		return toolErrorf("write_query only accepts INSERT, UPDATE, or DELETE statements; got %s", describeKind(kind))
	}

	affected, available, err := s.execSQL(ctx, args.Query)
	if err != nil {
		return toolErrorf("exec failed: %v", err)
	}

	var msg string
	if available {
		msg = fmt.Sprintf(`{"affected_rows":%d}`, affected)
	} else {
		msg = `{"affected_rows_note":"row count unavailable for this operation"}`
	}
	return textContent(msg), nil
}

// HandleCreateTable executes a CREATE TABLE statement.
func (s *Server) HandleCreateTable(ctx context.Context, args CreateTableArgs) (*mcp.CallToolResult, error) {
	if s.store.Cfg.ReadOnly {
		return toolErrorf("server is in read-only mode")
	}

	kind := classifySQL(args.Query)
	if !kind.isCreateTable() {
		return toolErrorf("create_table only accepts CREATE TABLE statements; got %s", describeKind(kind))
	}

	_, _, err := s.execSQL(ctx, args.Query)
	if err != nil {
		return toolErrorf("create table failed: %v", err)
	}
	return textContent("Table created successfully."), nil
}

// HandleListTables returns all tables visible to the current tenant.
func (s *Server) HandleListTables(ctx context.Context) (*mcp.CallToolResult, error) {
	q := `SELECT tenant, name, columns, rows, is_temp FROM sys.tables ORDER BY tenant, name`
	rows, cols, _, err := s.queryRows(ctx, q)
	if err != nil {
		// sys.tables unavailable; fall back to a best-effort approach.
		return toolErrorf("list_tables failed: %v", err)
	}
	return queryResultContent(rows, cols, false)
}

// HandleDescribeTable returns column metadata for the named table.
func (s *Server) HandleDescribeTable(ctx context.Context, args DescribeTableArgs) (*mcp.CallToolResult, error) {
	name := strings.TrimSpace(args.TableName)
	if name == "" {
		return toolErrorf("table_name is required")
	}
	if !validIdentifier(name) {
		return toolErrorf("invalid table name: %q", name)
	}

	q := `SELECT name, position, data_type, constraint, fk_table, fk_column FROM sys.columns WHERE table_name = ? ORDER BY position`
	rows, cols, _, err := s.queryRowsArgs(ctx, q, name)
	if err != nil {
		return toolErrorf("describe_table failed: %v", err)
	}
	if len(rows) == 0 {
		return toolErrorf("table %q not found or has no columns", name)
	}
	return queryResultContent(rows, cols, false)
}

// HandleAppendInsight stores a new analytical observation.
func (s *Server) HandleAppendInsight(ctx context.Context, args AppendInsightArgs) (*mcp.CallToolResult, error) {
	text := strings.TrimSpace(args.Insight)
	if text == "" {
		return toolErrorf("insight text is required")
	}
	idx := s.insights.Append(text)

	// Notify any subscribed MCP clients that the memo resource has changed.
	_ = s.mcpSrv.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{
		URI: "memo://insights",
	})

	return textContent(fmt.Sprintf("Insight #%d recorded.", idx)), nil
}

// HandleAgentContext returns a compact, prompt-ready description of the database.
func (s *Server) HandleAgentContext(ctx context.Context, args AgentContextArgs) (*mcp.CallToolResult, error) {
	maxTables := args.MaxTables
	if maxTables <= 0 {
		maxTables = 8
	}
	maxChars := args.MaxChars
	if maxChars <= 0 {
		maxChars = 4000
	}

	out, err := s.buildAgentContextFallback(ctx, maxTables, maxChars)
	if err != nil {
		return toolErrorf("agent_context failed: %v", err)
	}
	return textContent(out), nil
}

// HandleSampleTable returns the first N rows of a table.
func (s *Server) HandleSampleTable(ctx context.Context, args SampleTableArgs) (*mcp.CallToolResult, error) {
	name := strings.TrimSpace(args.TableName)
	if name == "" {
		return toolErrorf("table_name is required")
	}
	if !validIdentifier(name) {
		return toolErrorf("invalid table name: %q", name)
	}
	limit := args.Limit
	if limit <= 0 {
		limit = 10
	}
	maxRows := s.store.Cfg.MaxRows
	if maxRows > 0 && limit > maxRows {
		limit = maxRows
	}

	// Use a placeholder-free query with a validated identifier.
	q := fmt.Sprintf("SELECT * FROM %s LIMIT %d", quoteIdent(name), limit)
	rows, cols, truncated, err := s.queryRows(ctx, q)
	if err != nil {
		return toolErrorf("sample_table failed: %v", err)
	}
	return queryResultContent(rows, cols, truncated)
}

// ─── database helpers ─────────────────────────────────────────────────────────

func (s *Server) queryRows(ctx context.Context, query string) (rows []map[string]any, cols []string, truncated bool, err error) {
	return s.queryRowsArgs(ctx, query)
}

func (s *Server) queryRowsArgs(ctx context.Context, query string, args ...any) (rows []map[string]any, cols []string, truncated bool, err error) {
	qCtx := ctx
	if s.store.Cfg.QueryTimeout > 0 {
		var cancel context.CancelFunc
		qCtx, cancel = context.WithTimeout(ctx, s.store.Cfg.QueryTimeout)
		defer cancel()
	}

	sqlRows, err := s.store.DB.QueryContext(qCtx, query, args...)
	if err != nil {
		return nil, nil, false, err
	}
	defer sqlRows.Close()

	cols, err = sqlRows.Columns()
	if err != nil {
		return nil, nil, false, err
	}

	maxRows := s.store.Cfg.MaxRows
	for sqlRows.Next() {
		if maxRows > 0 && len(rows) >= maxRows {
			truncated = true
			break
		}
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, cols, false, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = dest[i]
		}
		rows = append(rows, row)
	}
	if err := sqlRows.Err(); err != nil {
		return nil, cols, false, err
	}
	return rows, cols, truncated, nil
}

func (s *Server) execSQL(ctx context.Context, query string) (affected int64, available bool, err error) {
	qCtx := ctx
	if s.store.Cfg.QueryTimeout > 0 {
		var cancel context.CancelFunc
		qCtx, cancel = context.WithTimeout(ctx, s.store.Cfg.QueryTimeout)
		defer cancel()
	}

	res, err := s.store.DB.ExecContext(qCtx, query)
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		// tinySQL may not always report affected rows.
		return 0, false, nil //nolint:nilerr
	}
	return n, true, nil
}

// buildAgentContextFallback queries sys.* tables to produce a compact
// database profile for AI agents.  This fallback is used instead of
// tinysql.BuildAgentContext because the MCP server integrates through
// database/sql and does not hold a direct *tinysql.DB reference.
func (s *Server) buildAgentContextFallback(ctx context.Context, maxTables, maxChars int) (string, error) {
	var b strings.Builder
	write := func(line string) {
		if maxChars > 0 && b.Len()+len(line)+1 > maxChars {
			return
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(line)
	}

	write("tinySQL agent profile (via MCP fallback)")
	write(fmt.Sprintf("tenant: %s", s.store.Tenant))

	// Tables and columns.
	tableRows, _, _, err := s.queryRows(ctx, `SELECT tenant, name, columns, rows, is_temp FROM sys.tables ORDER BY rows DESC, tenant, name`)
	if err == nil {
		type tableInfo struct {
			tenant  string
			name    string
			columns string
			rows    string
			isTemp  bool
		}
		var tables []tableInfo
		for _, r := range tableRows {
			tables = append(tables, tableInfo{
				tenant:  fmt.Sprintf("%v", nullStr(r["tenant"])),
				name:    fmt.Sprintf("%v", nullStr(r["name"])),
				columns: fmt.Sprintf("%v", nullStr(r["columns"])),
				rows:    fmt.Sprintf("%v", nullStr(r["rows"])),
				isTemp:  nullBool(r["is_temp"]),
			})
		}
		if maxTables > 0 && len(tables) > maxTables {
			tables = tables[:maxTables]
		}
		if len(tables) == 0 {
			write("tables: none")
		} else {
			colRows, _, _, _ := s.queryRows(ctx, `SELECT tenant, table_name, name, position, data_type, constraint FROM sys.columns ORDER BY tenant, table_name, position`)
			colMap := map[string][]string{}
			for _, r := range colRows {
				tname := fmt.Sprintf("%v", nullStr(r["table_name"]))
				cname := fmt.Sprintf("%v", nullStr(r["name"]))
				dtype := fmt.Sprintf("%v", nullStr(r["data_type"]))
				constraint := fmt.Sprintf("%v", nullStr(r["constraint"]))
				desc := cname + ":" + dtype
				if strings.EqualFold(constraint, "PRIMARY KEY") {
					desc += " pk"
				}
				colMap[tname] = append(colMap[tname], desc)
			}

			parts := make([]string, 0, len(tables))
			for _, t := range tables {
				label := t.name
				if t.tenant != "" && t.tenant != "default" {
					label = t.tenant + "." + t.name
				}
				colList := strings.Join(colMap[t.name], ", ")
				if colList == "" {
					colList = "..."
				}
				temp := ""
				if t.isTemp {
					temp = " temp"
				}
				parts = append(parts, fmt.Sprintf("%s rows=%s cols=%s%s [%s]", label, t.rows, t.columns, temp, colList))
			}
			write(fmt.Sprintf("tables(%d): %s", len(parts), strings.Join(parts, " | ")))
		}
	} else {
		write(fmt.Sprintf("tables: unavailable (%v)", err))
	}

	// Views.
	viewRows, _, _, _ := s.queryRows(ctx, `SELECT schema, name FROM sys.views ORDER BY schema, name`)
	if len(viewRows) == 0 {
		write("views: none")
	} else {
		names := make([]string, 0, len(viewRows))
		for _, r := range viewRows {
			schema := fmt.Sprintf("%v", nullStr(r["schema"]))
			name := fmt.Sprintf("%v", nullStr(r["name"]))
			if schema != "" && schema != "main" {
				names = append(names, schema+"."+name)
			} else {
				names = append(names, name)
			}
		}
		write(fmt.Sprintf("views(%d): %s", len(names), strings.Join(names, ", ")))
	}

	features := []string{
		"selects", "inserts", "updates", "deletes", "joins",
		"group by", "having", "order by", "limit", "subqueries", "ctes",
		"window functions", "views", "indexes(metadata)", "full-text search",
		"triggers", "jobs", "vector search", "json", "yaml", "url", "hash",
		"bitmap", "geometry", "decimal", "money", "multi-tenancy", "mvcc+wal",
		"sys.* and catalog.* introspection",
	}
	write("features: " + strings.Join(features, "; "))
	write("note: agent context generated via SQL fallback (BuildAgentContext requires direct engine access)")

	return b.String(), nil
}

// ─── response helpers ─────────────────────────────────────────────────────────

func textContent(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
	}
}

func toolErrorf(format string, a ...any) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf(format, a...)},
		},
		IsError: true,
	}, nil
}

// queryResultContent marshals rows to JSON and builds a human-readable summary.
func queryResultContent(rows []map[string]any, cols []string, truncated bool) (*mcp.CallToolResult, error) {
	// Marshal rows to pretty JSON.
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return toolErrorf("marshal results: %v", err)
	}

	summary := fmt.Sprintf("%d row(s) returned", len(rows))
	if truncated {
		summary += " (results truncated by --max-rows limit)"
	}
	if len(cols) > 0 {
		summary += fmt.Sprintf("; columns: %s", strings.Join(cols, ", "))
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(data)},
			&mcp.TextContent{Text: summary},
		},
	}, nil
}

// ─── identifier helpers ──────────────────────────────────────────────────────

var reValidIdent = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validIdentifier returns true when name looks like a safe SQL identifier
// (letters, digits, underscores, must start with a letter or underscore).
func validIdentifier(name string) bool {
	return reValidIdent.MatchString(name)
}

// quoteIdent wraps an identifier in double-quotes, escaping embedded
// double-quotes by doubling them.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// describeKind returns a human-readable name for a statement kind.
func describeKind(k stmtKind) string {
	switch k {
	case kindSelect:
		return "SELECT"
	case kindInsert:
		return "INSERT"
	case kindUpdate:
		return "UPDATE"
	case kindDelete:
		return "DELETE"
	case kindCreate:
		return "CREATE"
	case kindCreateTable:
		return "CREATE TABLE"
	case kindDrop:
		return "DROP"
	case kindAlter:
		return "ALTER"
	case kindOther:
		return "OTHER"
	default:
		return "UNKNOWN"
	}
}

// ─── null-safe helpers ─────────────────────────────────────────────────────────

func nullStr(v any) string {
	if v == nil {
		return ""
	}
	if ns, ok := v.(sql.NullString); ok {
		if ns.Valid {
			return ns.String
		}
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func nullBool(v any) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	if nb, ok := v.(sql.NullBool); ok {
		return nb.Valid && nb.Bool
	}
	return strings.EqualFold(fmt.Sprintf("%v", v), "true")
}
