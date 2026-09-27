package engine

import "strings"

// SplitStatements splits a SQL script into its statements with the engine's
// own lexer, so semicolons inside string and BLOB literals, quoted identifiers
// and comments never end a statement. CREATE TRIGGER bodies stay intact:
// semicolons between BEGIN and its matching END (including CASE ... END
// expressions inside the body) belong to the trigger.
//
// Statements are returned without their terminating semicolon and without
// surrounding whitespace. Leading comments before a statement are dropped;
// empty statements are skipped. Splitting never validates syntax: each
// returned statement is parsed and reported individually by its executor.
func SplitStatements(script string) []string {
	lx := lexer{s: script}
	var (
		statements []string
		start      = -1 // byte offset of the current statement's first token
		tokens     int  // tokens seen in the current statement
		create     bool // statement starts with CREATE
		trigger    bool // statement is CREATE [...] TRIGGER
		depth      int  // BEGIN/CASE nesting inside a trigger body
	)
	for {
		tok := lx.nextToken()
		if tok.Typ == tEOF {
			break
		}
		isSemicolon := tok.Typ == tSymbol && tok.Val == ";"
		if start < 0 {
			if isSemicolon {
				continue
			}
			start = tok.Pos
		}
		if isSemicolon && depth == 0 {
			if statement := strings.TrimSpace(script[start:tok.Pos]); statement != "" {
				statements = append(statements, statement)
			}
			start, tokens, create, trigger = -1, 0, false, false
			continue
		}
		tokens++
		if tok.Typ != tKeyword {
			continue
		}
		switch {
		case tokens == 1:
			create = tok.Val == "CREATE"
		case create && !trigger && tokens <= 5 && tok.Val == "TRIGGER":
			// CREATE [OR REPLACE] [TEMP] TRIGGER
			trigger = true
		case trigger:
			switch tok.Val {
			case "BEGIN":
				depth++
			case "CASE":
				if depth > 0 {
					depth++
				}
			case "END":
				if depth > 0 {
					depth--
				}
			}
		}
	}
	if start >= 0 {
		if statement := strings.TrimSpace(script[start:]); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}
