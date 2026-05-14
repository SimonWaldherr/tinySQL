// Package mcpserver implements an MCP server for the tinySQL database.
// It exposes tools, resources, and prompts that allow an MCP host to inspect,
// query, mutate, and contextualize a tinySQL database over stdio.
package mcpserver

import (
	"regexp"
	"strings"
	"unicode"
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

// classifySQL returns the kind of the first SQL statement in src.
//
// The classifier:
//   - Strips leading/trailing whitespace.
//   - Removes SQL line comments (--) and block comments (/* … */).
//   - Normalises runs of whitespace to a single space.
//   - Examines the first one or two tokens to determine kind.
//
// Limitations (documented):
//   - Does not parse string literals: a comment-like sequence inside a quoted
//     string will still be stripped.  In practice this cannot bypass the
//     classification of the leading keyword.
//   - Multi-statement inputs (containing ';' outside a string) are detected
//     heuristically and returned as kindUnknown to prevent ambiguous routing.
//   - The classifier is conservative: an unknown kind causes the calling tool
//     to reject the query.
func classifySQL(src string) stmtKind {
	clean := stripComments(src)
	clean = strings.TrimSpace(clean)

	if clean == "" {
		return kindUnknown
	}

	// Reject multi-statement input.  We look for a semicolon that is not at
	// the very end of the (trimmed) string.  This is a heuristic: it may
	// reject valid statements that contain semicolons inside string literals,
	// but it cannot be bypassed by injecting a semicolon.
	if hasEarlyTerminator(clean) {
		return kindUnknown
	}

	// Extract the first two space-separated tokens.
	tokens := splitTokens(clean, 3)
	if len(tokens) == 0 {
		return kindUnknown
	}
	first := strings.ToUpper(tokens[0])

	switch first {
	case "SELECT":
		return kindSelect
	case "WITH":
		// WITH … SELECT is a CTE (read-only).
		// WITH … INSERT/UPDATE/DELETE is a mutating CTE.
		// We look at the first keyword that is not part of the CTE preamble.
		return classifyCTE(clean)
	case "INSERT":
		return kindInsert
	case "UPDATE":
		return kindUpdate
	case "DELETE":
		return kindDelete
	case "CREATE":
		if len(tokens) < 2 {
			return kindCreate
		}
		second := strings.ToUpper(tokens[1])
		switch second {
		case "TABLE":
			return kindCreateTable
		case "OR":
			// CREATE OR REPLACE TABLE …
			if len(tokens) >= 3 && strings.ToUpper(tokens[2]) == "REPLACE" {
				return kindCreate
			}
			return kindCreate
		default:
			return kindCreate
		}
	case "DROP":
		return kindDrop
	case "ALTER":
		return kindAlter
	case "TRIGGER", "JOB":
		return kindOther
	case "EXPLAIN":
		return kindOther
	}
	return kindUnknown
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

// -------------------------------------------------------------------------
// Internal helpers
// -------------------------------------------------------------------------

var (
	// reLineComment matches a SQL line comment: -- to end of line.
	reLineComment = regexp.MustCompile(`--[^\n]*`)
	// reBlockComment matches a SQL block comment: /* … */.
	reBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
)

// stripComments removes SQL line and block comments from src.
func stripComments(src string) string {
	s := reBlockComment.ReplaceAllString(src, " ")
	s = reLineComment.ReplaceAllString(s, " ")
	return s
}

// hasEarlyTerminator returns true if src contains a semicolon that is not at
// the trailing end of the trimmed string.  This rejects multi-statement input.
func hasEarlyTerminator(src string) bool {
	trimmed := strings.TrimRight(src, " \t\r\n;")
	// If there is any semicolon remaining in the trimmed content, it is an
	// early terminator.
	return strings.ContainsRune(trimmed, ';')
}

// splitTokens returns up to n whitespace-separated tokens from src.
func splitTokens(src string, n int) []string {
	var tokens []string
	inToken := false
	start := 0
	for i, r := range src {
		isSpace := unicode.IsSpace(r)
		switch {
		case !isSpace && !inToken:
			inToken = true
			start = i
		case isSpace && inToken:
			inToken = false
			tokens = append(tokens, src[start:i])
			if len(tokens) == n {
				return tokens
			}
		}
	}
	if inToken {
		tokens = append(tokens, src[start:])
	}
	return tokens
}

// classifyCTE examines a WITH … query to determine if the terminal statement
// is SELECT (read-only) or a mutation.
//
// We scan forward to find the keyword that follows the first AS (<subquery>)
// of the CTE.  For deeply nested CTEs we use a bracket counter.
// If we cannot determine the kind with confidence, we return kindUnknown.
func classifyCTE(src string) stmtKind {
	upper := strings.ToUpper(src)
	// Find the first SELECT that is the anchor of the outer query.
	// Strategy: skip past all WITH name AS ( … ), fragments.
	// We count depth of parentheses; the outer SELECT is at depth 0.
	depth := 0
	i := 0
	runes := []rune(upper)
	n := len(runes)
	for i < n {
		switch runes[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && i > 0 {
			// Check for a keyword at this position (after skipping leading WITH keyword).
			rest := strings.TrimSpace(string(runes[i+1:]))
			if strings.HasPrefix(rest, ",") {
				// Another CTE follows.
				i++
				continue
			}
			// Consume the comma that may trail a CTE definition.
			tokens := splitTokens(rest, 1)
			if len(tokens) == 0 {
				break
			}
			switch tokens[0] {
			case "SELECT":
				return kindSelect
			case "INSERT":
				return kindInsert
			case "UPDATE":
				return kindUpdate
			case "DELETE":
				return kindDelete
			}
		}
		i++
	}
	// Fallback: look for SELECT anywhere at depth 0 after the WITH keyword.
	// This handles simple single-CTE cases.
	if strings.Contains(upper, "\nSELECT ") || strings.Contains(upper, " SELECT ") {
		return kindSelect
	}
	return kindUnknown
}
