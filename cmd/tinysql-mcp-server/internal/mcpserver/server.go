package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"tinysql-mcp-server/internal/tinysqldb"
)

// buildMCPServer creates and configures the *mcp.Server with all tools,
// resources, and prompts registered.
func (s *Server) buildMCPServer() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "tinysql-mcp-server",
		Version: "1.0.0",
	}, nil)

	s.mcpSrv = srv // set early so resource/prompt handlers can reference it

	registerTools(srv, s)
	s.registerResources()
	s.registerPrompts()

	return srv
}

// ─── tool registration ────────────────────────────────────────────────────────

func registerTools(srv *mcp.Server, s *Server) {
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "read_query",
		Description: "Execute a read-only SQL SELECT statement against the tinySQL database. CTEs (WITH … SELECT) are also accepted. Mutating statements are rejected.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ReadQueryArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleReadQuery(ctx, args)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "write_query",
		Description: "Execute a mutating SQL statement (INSERT, UPDATE, or DELETE). Blocked when the server is started with --readonly.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args WriteQueryArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleWriteQuery(ctx, args)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "create_table",
		Description: "Execute a CREATE TABLE statement. Only CREATE TABLE is accepted; other DDL is rejected. Blocked when the server is started with --readonly.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args CreateTableArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleCreateTable(ctx, args)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "list_tables",
		Description: "List all tables in the active tinySQL tenant, with metadata from sys.tables.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleListTables(ctx)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "describe_table",
		Description: "Return column names, data types, constraints, and foreign-key metadata for a table using sys.columns.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args DescribeTableArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleDescribeTable(ctx, args)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "append_insight",
		Description: "Append an analytical observation to the in-memory insight memo. Updates the memo://insights resource.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args AppendInsightArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleAppendInsight(ctx, args)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "agent_context",
		Description: "Return a compact, prompt-ready description of the current tinySQL database state (tables, columns, views, features). Uses sys.* metadata tables via SQL fallback.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args AgentContextArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleAgentContext(ctx, args)
		return res, nil, err
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "sample_table",
		Description: "Return the first N rows of a table (default 10). Table name is validated and identifier-quoted to prevent injection.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args SampleTableArgs) (*mcp.CallToolResult, any, error) {
		res, err := s.HandleSampleTable(ctx, args)
		return res, nil, err
	})
}

// ─── resource registration ────────────────────────────────────────────────────

func (s *Server) registerResources() {
	// memo://insights – live insight memo
	s.mcpSrv.AddResource(&mcp.Resource{
		URI:         "memo://insights",
		Name:        "Database Insights",
		Description: "In-memory analytical observations recorded by append_insight.",
		MIMEType:    "text/markdown",
	}, func(_ context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      "memo://insights",
					MIMEType: "text/markdown",
					Text:     s.insights.Memo(),
				},
			},
		}, nil
	})

	// tinysql://schema – current database schema as JSON + summary
	s.mcpSrv.AddResource(&mcp.Resource{
		URI:         "tinysql://schema",
		Name:        "Database Schema",
		Description: "Current tinySQL schema: tables, columns, views, and tenant metadata.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		body, err := s.buildSchemaResource(ctx)
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      "tinysql://schema",
					MIMEType: "application/json",
					Text:     body,
				},
			},
		}, nil
	})

	// tinysql://agent-context – prompt-ready database profile
	s.mcpSrv.AddResource(&mcp.Resource{
		URI:         "tinysql://agent-context",
		Name:        "Agent Context",
		Description: "Compact, prompt-ready tinySQL database profile for AI agents.",
		MIMEType:    "text/plain",
	}, func(ctx context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		text, err := s.buildAgentContextFallback(ctx, 8, 4000)
		if err != nil {
			return nil, fmt.Errorf("agent-context: %w", err)
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      "tinysql://agent-context",
					MIMEType: "text/plain",
					Text:     text,
				},
			},
		}, nil
	})
}

// buildSchemaResource returns a JSON document describing the database schema.
func (s *Server) buildSchemaResource(ctx context.Context) (string, error) {
	type columnMeta struct {
		Name       string `json:"name"`
		Position   any    `json:"position,omitempty"`
		DataType   string `json:"data_type"`
		Constraint string `json:"constraint,omitempty"`
		FKTable    string `json:"fk_table,omitempty"`
		FKColumn   string `json:"fk_column,omitempty"`
	}
	type tableMeta struct {
		Tenant  string       `json:"tenant"`
		Name    string       `json:"name"`
		Rows    any          `json:"rows"`
		Columns any          `json:"columns"`
		IsTemp  bool         `json:"is_temp,omitempty"`
		Cols    []columnMeta `json:"column_definitions"`
	}
	type viewMeta struct {
		Schema string `json:"schema,omitempty"`
		Name   string `json:"name"`
	}
	type schema struct {
		Tenant  string      `json:"tenant"`
		Tables  []tableMeta `json:"tables"`
		Views   []viewMeta  `json:"views"`
		Summary string      `json:"summary"`
	}

	out := schema{Tenant: s.store.Tenant}

	// Fetch tables.
	tableRows, _, _, err := s.queryRows(ctx, `SELECT tenant, name, columns, rows, is_temp FROM sys.tables ORDER BY tenant, name`)
	if err == nil {
		colRows, _, _, _ := s.queryRows(ctx, `SELECT tenant, table_name, name, position, data_type, constraint, fk_table, fk_column FROM sys.columns ORDER BY tenant, table_name, position`)
		colMap := map[string][]columnMeta{}
		for _, r := range colRows {
			tname := nullStr(r["table_name"])
			colMap[tname] = append(colMap[tname], columnMeta{
				Name:       nullStr(r["name"]),
				Position:   r["position"],
				DataType:   nullStr(r["data_type"]),
				Constraint: nullStr(r["constraint"]),
				FKTable:    nullStr(r["fk_table"]),
				FKColumn:   nullStr(r["fk_column"]),
			})
		}
		for _, r := range tableRows {
			name := nullStr(r["name"])
			out.Tables = append(out.Tables, tableMeta{
				Tenant:  nullStr(r["tenant"]),
				Name:    name,
				Rows:    r["rows"],
				Columns: r["columns"],
				IsTemp:  nullBool(r["is_temp"]),
				Cols:    colMap[name],
			})
		}
	}

	// Fetch views.
	viewRows, _, _, _ := s.queryRows(ctx, `SELECT schema, name FROM sys.views ORDER BY schema, name`)
	for _, r := range viewRows {
		out.Views = append(out.Views, viewMeta{
			Schema: nullStr(r["schema"]),
			Name:   nullStr(r["name"]),
		})
	}

	// Build summary.
	var parts []string
	parts = append(parts, fmt.Sprintf("%d table(s)", len(out.Tables)))
	parts = append(parts, fmt.Sprintf("%d view(s)", len(out.Views)))
	out.Summary = strings.Join(parts, ", ") + fmt.Sprintf("; tenant: %s", s.store.Tenant)

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ─── dependency injection helper ─────────────────────────────────────────────

// NewWithStore is an alias for New, kept for explicit naming in tests.
func NewWithStore(store *tinysqldb.Store) *Server {
	return New(store)
}
