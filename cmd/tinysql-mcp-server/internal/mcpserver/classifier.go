// Package mcpserver implements an MCP server for the tinySQL database.
// It exposes tools, resources, and prompts that allow an MCP host to inspect,
// query, mutate, and contextualize a tinySQL database over stdio.
package mcpserver

import (
	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/sqlutil"
)

// stmtKind identifies the broad classification of a SQL statement.
type stmtKind int

const (
	kindUnknown     stmtKind = iota
	kindSelect               // SELECT, WITH … SELECT
	kindInsert               // INSERT
	kindUpdate               // UPDATE
	kindDelete               // DELETE
	kindCreate               // CREATE TABLE, CREATE VIEW, CREATE INDEX, …
	kindCreateTable          // CREATE TABLE only
	kindDrop                 // DROP TABLE, DROP VIEW, …
	kindAlter                // ALTER TABLE, …
	kindOther                // EXPLAIN, PRAGMA, SHOW, …
)

// classifySQL admits a single, fully parsed statement. Check its opening
// keywords as well as its AST: the engine accepts a bare table name as a
// Select, while database/sql handles transaction commands before parsing.
func classifySQL(src string) stmtKind {
	parser := tinysql.NewParser(src)
	first, second := parser.LeadingKeywords()
	stmt, err := parser.ParseStatement()
	if err != nil {
		return kindUnknown
	}

	switch sqlutil.AnalyzeStatement(stmt).Kind {
	case sqlutil.KindSelect:
		if first == "SELECT" || first == "WITH" {
			return kindSelect
		}
	case sqlutil.KindInsert:
		return kindInsert
	case sqlutil.KindUpdate:
		return kindUpdate
	case sqlutil.KindDelete:
		return kindDelete
	case sqlutil.KindCreateTable:
		if first == "CREATE" && second == "TABLE" {
			return kindCreateTable
		}
	}

	// Keep diagnostic labels for statements that none of the SQL tools admit.
	switch first {
	case "CREATE":
		return kindCreate
	case "DROP":
		return kindDrop
	case "ALTER":
		return kindAlter
	case "EXPLAIN", "PRAGMA", "ANALYZE", "CALL":
		return kindOther
	default:
		return kindUnknown
	}
}

// isReadOnly returns true when kind represents a read-only operation.
func (k stmtKind) isReadOnly() bool {
	return k == kindSelect
}

// isMutating returns true when kind represents a write operation.
func (k stmtKind) isMutating() bool {
	switch k {
	case kindInsert, kindUpdate, kindDelete, kindCreate, kindCreateTable, kindDrop, kindAlter:
		return true
	}
	return false
}

// isCreateTable returns true when kind is exactly CREATE TABLE.
func (k stmtKind) isCreateTable() bool {
	return k == kindCreateTable
}
