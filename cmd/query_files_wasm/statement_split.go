package main

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// splitStatements splits SQL on statement separators without treating
// semicolons inside quoted strings, quoted identifiers, or comments as
// separators. A standalone GO line also ends a statement. Keeping this
// separate from the WASM bridge makes the editor's
// multi-statement behavior directly testable on every Go target.
func splitStatements(raw string) []string {
	const (
		normal = iota
		singleQuote
		doubleQuote
		lineComment
		blockComment
	)

	state := normal
	var statements []string
	start := 0
	// Only CREATE [OR REPLACE] TRIGGER can contain statement separators in
	// its BEGIN/END body. Other BEGIN statements remain separate statements.
	const (
		prefixStart = iota
		prefixCreate
		prefixOr
		prefixReplace
		prefixTrigger
		prefixOther
	)
	prefix, triggerDepth, parens := prefixStart, 0, 0

	appendStatement := func(end int) {
		if statement := strings.TrimSpace(raw[start:end]); statement != "" && hasSQLContent(statement) {
			// Copy only the finished statement, so retaining one result does not
			// keep the entire input script alive.
			statements = append(statements, strings.Clone(statement))
		}
	}

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		next := byte(0)
		if i+1 < len(raw) {
			next = raw[i+1]
		}

		switch state {
		case normal:
			if triggerDepth == 0 && parens == 0 && (ch == 'G' || ch == 'g') && (next == 'O' || next == 'o') {
				if end := goBatchLineEnd(raw, i); end >= 0 {
					appendStatement(i)
					start, i, prefix = end, end-1, prefixStart
					continue
				}
			}
			switch {
			case ch == '\'':
				if prefix != prefixTrigger {
					prefix = prefixOther
				}
				state = singleQuote
			case ch == '"':
				if prefix != prefixTrigger {
					prefix = prefixOther
				}
				state = doubleQuote
			case ch == '-' && next == '-':
				state = lineComment
				i++
				continue
			case ch == '/' && next == '*':
				state = blockComment
				i++
				continue
			case ch == ';':
				if triggerDepth > 0 {
					continue
				}
				appendStatement(i)
				start = i + 1
				prefix, parens = prefixStart, 0
				continue
			default:
				if ch == '(' {
					parens++
				} else if ch == ')' && parens > 0 {
					parens--
				}
				// Once an ordinary statement is identified, no other tokens
				// need inspecting until its terminating semicolon.
				if prefix == prefixOther {
					continue
				}
				if ch >= utf8.RuneSelf {
					r, size := utf8.DecodeRuneInString(raw[i:])
					if unicode.IsSpace(r) {
						i += size - 1
						continue
					}
				}
				if isSQLIdentifierStart(raw[i:]) {
					end := sqlIdentifierEnd(raw, i)
					word := raw[i:end]
					i = end - 1
					switch prefix {
					case prefixStart:
						if strings.EqualFold(word, "CREATE") {
							prefix = prefixCreate
						} else {
							prefix = prefixOther
						}
					case prefixCreate:
						if strings.EqualFold(word, "TRIGGER") {
							prefix = prefixTrigger
						} else if strings.EqualFold(word, "OR") {
							prefix = prefixOr
						} else {
							prefix = prefixOther
						}
					case prefixOr:
						if strings.EqualFold(word, "REPLACE") {
							prefix = prefixReplace
						} else {
							prefix = prefixOther
						}
					case prefixReplace:
						if strings.EqualFold(word, "TRIGGER") {
							prefix = prefixTrigger
						} else {
							prefix = prefixOther
						}
					case prefixTrigger:
						switch {
						case strings.EqualFold(word, "BEGIN"):
							triggerDepth++
						case strings.EqualFold(word, "CASE") && triggerDepth > 0:
							triggerDepth++
						case strings.EqualFold(word, "END") && triggerDepth > 0:
							triggerDepth--
						}
					}
				} else if prefix != prefixTrigger && !strings.ContainsRune(" \t\r\n\v\f", rune(ch)) {
					prefix = prefixOther
				}
			}
		case singleQuote:
			if ch == '\'' {
				if next == '\'' { // Escaped SQL string quote.
					i++
					continue
				}
				state = normal
			}
		case doubleQuote:
			if ch == '"' {
				if next == '"' { // Escaped quoted-identifier quote.
					i++
					continue
				}
				state = normal
			}
		case lineComment:
			if ch == '\n' || ch == '\r' {
				state = normal
			}
		case blockComment:
			if ch == '*' && next == '/' {
				i++
				state = normal
				continue
			}
		}
	}
	appendStatement(len(raw))
	return statements
}

func goBatchLineEnd(raw string, start int) int {
	lineStart := strings.LastIndexAny(raw[:start], "\r\n") + 1
	if strings.TrimSpace(raw[lineStart:start]) != "" {
		return -1
	}
	end := len(raw)
	if newline := strings.IndexAny(raw[start+2:], "\r\n"); newline >= 0 {
		end = start + 2 + newline
	}
	trailing := strings.TrimSpace(raw[start+2 : end])
	if trailing == "" || strings.HasPrefix(trailing, "--") {
		return end
	}
	return -1
}

func isSQLIdentifierStart(text string) bool {
	ch := text[0]
	if ch < utf8.RuneSelf {
		return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
	}
	r, _ := utf8.DecodeRuneInString(text)
	return unicode.IsLetter(r)
}

func sqlIdentifierEnd(text string, start int) int {
	for i := start; i < len(text); {
		ch := text[i]
		if ch < utf8.RuneSelf {
			// The engine lexes qualified names like NEW.case as one identifier.
			if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '.' {
				i++
				continue
			}
			return i
		}
		r, size := utf8.DecodeRuneInString(text[i:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return i
		}
		i += size
	}
	return len(text)
}

// Comments may appear after a final semicolon or between statements. They
// must remain attached to real SQL, but a segment containing only comments
// is not a statement and should not be sent to the parser on its own.
func hasSQLContent(statement string) bool {
	for {
		statement = strings.TrimSpace(statement)
		switch {
		case statement == "":
			return false
		case strings.HasPrefix(statement, "--"):
			end := strings.IndexAny(statement, "\r\n")
			if end < 0 {
				return false
			}
			statement = statement[end+1:]
		case strings.HasPrefix(statement, "/*"):
			end := strings.Index(statement[2:], "*/")
			if end < 0 {
				// Preserve the malformed segment for the parser to reject;
				// dropping it would turn an invalid script into success.
				return true
			}
			statement = statement[end+4:]
		default:
			return true
		}
	}
}
