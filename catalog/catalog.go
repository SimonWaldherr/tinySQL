// Package catalog exposes stable schema and agent-context helpers for tinySQL
// databases without requiring callers to query internal sys.* tables directly.
package catalog

import (
	"context"
	"fmt"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// AgentContextConfig controls the prompt-sized database profile.
type AgentContextConfig = tinysql.AgentContextConfig

// Object describes a table, view, job, trigger, or other catalog object.
type Object struct {
	Schema string
	Name   string
	Type   string
	Status string
}

// Column describes a catalog column.
type Column struct {
	Tenant     string
	TableName  string
	Name       string
	Position   string
	DataType   string
	Constraint string
	FKTable    string
	FKColumn   string
}

// DefaultAgentContextConfig returns a bounded agent-context configuration.
func DefaultAgentContextConfig() AgentContextConfig {
	return tinysql.DefaultAgentContextConfig()
}

// BuildAgentContext returns a compact prompt-ready database profile.
func BuildAgentContext(ctx context.Context, db *tinysql.DB, tenant string, cfg AgentContextConfig) (string, error) {
	return tinysql.BuildAgentContext(ctx, db, tenant, cfg)
}

// ListObjects returns unified object metadata from sys.objects.
func ListObjects(ctx context.Context, db *tinysql.DB, tenant string) ([]Object, error) {
	rows, err := queryRows(ctx, db, tenant, "SELECT schema, name, object_type, status FROM sys.objects ORDER BY schema, name")
	if err != nil {
		return nil, err
	}
	out := make([]Object, 0, len(rows))
	for _, row := range rows {
		out = append(out, Object{
			Schema: rowString(row, "schema"),
			Name:   rowString(row, "name"),
			Type:   rowString(row, "object_type"),
			Status: rowString(row, "status"),
		})
	}
	return out, nil
}

// ListColumns returns table/view column metadata from sys.columns.
func ListColumns(ctx context.Context, db *tinysql.DB, tenant string) ([]Column, error) {
	rows, err := queryRows(ctx, db, tenant, "SELECT tenant, table_name, name, position, data_type, constraint, fk_table, fk_column FROM sys.columns ORDER BY tenant, table_name, position")
	if err != nil {
		return nil, err
	}
	out := make([]Column, 0, len(rows))
	for _, row := range rows {
		out = append(out, Column{
			Tenant:     rowString(row, "tenant"),
			TableName:  rowString(row, "table_name"),
			Name:       rowString(row, "name"),
			Position:   rowString(row, "position"),
			DataType:   rowString(row, "data_type"),
			Constraint: rowString(row, "constraint"),
			FKTable:    rowString(row, "fk_table"),
			FKColumn:   rowString(row, "fk_column"),
		})
	}
	return out, nil
}

func queryRows(ctx context.Context, db *tinysql.DB, tenant, sql string) ([]tinysql.Row, error) {
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", sql, err)
	}
	rs, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return rs.Rows, nil
}

func rowString(row tinysql.Row, name string) string {
	if v, ok := tinysql.GetVal(row, name); ok && v != nil {
		return strings.TrimSpace(fmt.Sprint(v))
	}
	return ""
}
