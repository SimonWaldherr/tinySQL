package engine

import "strings"

// SplitStatements splits a SQL script into its statements with the engine's
// own lexer, so semicolons inside string and BLOB literals, quoted identifiers
// and comments never end a statement. CREATE TRIGGER bodies stay intact:
// semicolons between BEGIN and its matching END (including CASE ... END
// expressions inside the body) belong to the trigger.
// An unquoted GO on its own line is also a script separator. It may be
// followed by a line comment.
//
// Statements are returned without their terminating semicolon and without
// surrounding whitespace. Leading comments before a statement are dropped;
// empty statements are skipped. Splitting never validates syntax: each
// returned statement is parsed and reported individually by its executor.
func SplitStatements(script string) []string {
	statements, _ := scanStatements(script, true)
	return statements
}

// StatementComplete reports whether script ends with a complete statement:
// its last statement ends with a semicolon or a GO line, outside string
// literals, quoted identifiers, comments and CREATE TRIGGER bodies. It is the
// equivalent of SQLite's sqlite3_complete and lets interactive shells decide
// when buffered input is ready to execute. Syntax is not validated.
func StatementComplete(script string) bool {
	_, complete := scanStatements(script, false)
	return complete
}

// scanStatements optionally collects SQL text and reports whether the input
// ends after at least one complete statement.
func scanStatements(script string, collect bool) ([]string, bool) {
	lx := lexer{s: script}
	var (
		statements []string
		start      = -1 // byte offset of the current statement's first token
		tokens     int  // tokens seen in the current statement
		create     bool // statement starts with CREATE
		trigger    bool // statement is CREATE [...] TRIGGER
		depth      int  // BEGIN/CASE nesting inside a trigger body
		parens     int
		finished   bool
	)
	for {
		tok := lx.nextToken()
		if tok.Typ == tEOF {
			break
		}
		isSemicolon := tok.Typ == tSymbol && tok.Val == ";"
		isGo := depth == 0 && parens == 0 && tok.Typ == tIdent && lx.pos-tok.Pos == 2 &&
			strings.EqualFold(tok.Val, "GO") && isGOBatchLine(script, tok.Pos, lx.pos)
		if depth == 0 && (isSemicolon || isGo) {
			if start >= 0 {
				finished = true
				if collect {
					statements = append(statements, strings.TrimSpace(script[start:tok.Pos]))
				}
			}
			start, tokens, parens, create, trigger = -1, 0, 0, false, false
			continue
		}
		if start < 0 {
			start = tok.Pos
		}
		if tok.Typ == tSymbol {
			switch tok.Val {
			case "(":
				parens++
			case ")":
				parens = max(0, parens-1)
			}
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
	if collect && start >= 0 {
		if statement := strings.TrimSpace(script[start:]); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements, finished && start < 0
}

func isGOBatchLine(script string, start, end int) bool {
	lineStart := strings.LastIndexAny(script[:start], "\r\n") + 1
	if strings.TrimSpace(script[lineStart:start]) != "" {
		return false
	}
	after := script[end:]
	if newline := strings.IndexAny(after, "\r\n"); newline >= 0 {
		after = after[:newline]
	}
	after = strings.TrimSpace(after)
	return after == "" || strings.HasPrefix(after, "--")
}
