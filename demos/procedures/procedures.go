// Package procedures registers reusable stored procedures used by the
// native and WebAssembly demos.
package procedures

import (
	"fmt"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// Register installs the demo procedures in tinySQL's process-local registry.
// Calling Register again replaces the same names and resets their statistics.
func Register() error {
	registrations := []struct {
		name    string
		options tinysql.StoredProcedureOptions
		handler tinysql.StoredProcedureFunc
	}{
		{
			name: "demo_table_summary",
			options: tinysql.StoredProcedureOptions{
				Description: "List tables, columns and row counts for the current tenant",
				ReadOnly:    true,
				Parameters:  []tinysql.StoredProcedureParameter{},
			},
			handler: func(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
				return ctx.ExecuteSQL("SELECT name, columns, rows FROM sys.tables ORDER BY name")
			},
		},
		{
			name: "demo_runtime_status",
			options: tinysql.StoredProcedureOptions{
				Description: "Return selected runtime and database health indicators",
				ReadOnly:    true,
				Parameters:  []tinysql.StoredProcedureParameter{},
			},
			handler: func(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
				return ctx.ExecuteSQL("SELECT key, value FROM sys.status WHERE key IN ('go_version', 'goroutines', 'tables', 'tenants') ORDER BY key")
			},
		},
		{
			name: "demo_geo_distance",
			options: tinysql.StoredProcedureOptions{
				Description: "Calculate air-line distance and bearing between two coordinates",
				ReadOnly:    true,
				Parameters: []tinysql.StoredProcedureParameter{
					{Name: "lat1", Description: "origin latitude", Required: true},
					{Name: "lon1", Description: "origin longitude", Required: true},
					{Name: "lat2", Description: "destination latitude", Required: true},
					{Name: "lon2", Description: "destination longitude", Required: true},
				},
			},
			handler: func(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
				return ctx.ExecuteSQLArgs(`SELECT GEO_DISTANCE(?, ?, ?, ?) AS meters,
                           GEO_BEARING(?, ?, ?, ?) AS bearing_degrees`,
					args[0], args[1], args[2], args[3], args[0], args[1], args[2], args[3])
			},
		},
		{
			name: "demo_find_functions",
			options: tinysql.StoredProcedureOptions{
				Description: "Search the SQL function catalog by a LIKE pattern",
				ReadOnly:    true,
				Parameters: []tinysql.StoredProcedureParameter{
					{Name: "pattern", Description: "optional SQL LIKE pattern", Required: false},
				},
			},
			handler: func(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
				pattern := "%"
				if len(args) == 1 {
					pattern = fmt.Sprint(args[0])
				}
				return ctx.ExecuteSQLArgs("SELECT name, function_type, language FROM sys.functions WHERE name LIKE ? ORDER BY name", pattern)
			},
		},
		{
			name: "demo_log_event",
			options: tinysql.StoredProcedureOptions{
				Description: "Atomically create and append to a procedure event log",
				Atomic:      true,
				Parameters: []tinysql.StoredProcedureParameter{
					{Name: "category", Required: true},
					{Name: "message", Required: true},
				},
			},
			handler: demoLogEvent,
		},
		{
			name: "demo_release_features",
			options: tinysql.StoredProcedureOptions{
				Description: "Show the feature areas covered by the browser build",
				ReadOnly:    true,
				Parameters:  []tinysql.StoredProcedureParameter{},
			},
			handler: func(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
				return ctx.ExecuteSQL(`
					SELECT 'Geo imports' AS area, 'GeoJSON, KML, OSM XML, routing graph, Shapefile ZIP, MBTiles' AS feature, 'partly direct in browser; binary formats in Go/CLI/server' AS wasm_status
					UNION ALL SELECT 'Spatial SQL', 'ST_MakePoint, ST_X, ST_Y, GEO_DISTANCE, GEO_DWITHIN', 'direct'
					UNION ALL SELECT 'Search/RAG', 'HYBRID_SEARCH, wildcard FTS, VEC_SEARCH, RAG_SEARCH, RAG_CONTEXT', 'direct'
					UNION ALL SELECT 'Analytics SQL', 'CTEs, views, materialized views, PIVOT, window functions, RETURNING, EXPLAIN', 'direct'
					UNION ALL SELECT 'Introspection', 'sys.tables, sys.columns, sys.functions, sys.procedures, PRAGMA compatibility', 'direct'
					UNION ALL SELECT 'Operations', 'RBAC, audit logs, WAL/storage, tinysqld, MCP server, tinyORM', 'core/server-side; showcased as metadata and SQL recipes'
					ORDER BY area
				`)
			},
		},
	}

	for _, registration := range registrations {
		if err := tinysql.RegisterStoredProcedureWithOptions(registration.name, registration.options, registration.handler); err != nil {
			return fmt.Errorf("register %s: %w", registration.name, err)
		}
	}
	return nil
}

func demoLogEvent(ctx tinysql.ProcedureContext, args []any) (*tinysql.ResultSet, error) {
	tables, err := ctx.ExecuteSQLArgs("SELECT name FROM sys.tables WHERE name = ?", "demo_procedure_log")
	if err != nil {
		return nil, err
	}
	if len(tables.Rows) == 0 {
		if _, err := ctx.ExecuteSQL(`CREATE TABLE demo_procedure_log (
            category TEXT,
            message TEXT,
            created_at DATETIME
        )`); err != nil {
			return nil, err
		}
	}
	if _, err := ctx.ExecuteSQLArgs(
		"INSERT INTO demo_procedure_log VALUES (?, ?, CURRENT_TIMESTAMP())",
		args[0], args[1],
	); err != nil {
		return nil, err
	}
	return ctx.ExecuteSQL("SELECT category, message, created_at FROM demo_procedure_log ORDER BY created_at DESC LIMIT 20")
}
