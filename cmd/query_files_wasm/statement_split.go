package main

import "strings"

// splitStatements splits SQL on statement separators without treating
// semicolons inside quoted strings, quoted identifiers, or comments as
// separators. Keeping this separate from the WASM bridge makes the editor's
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
	var current strings.Builder

	appendStatement := func() {
		if statement := strings.TrimSpace(current.String()); statement != "" {
			statements = append(statements, statement)
		}
		current.Reset()
	}

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		next := byte(0)
		if i+1 < len(raw) {
			next = raw[i+1]
		}

		switch state {
		case normal:
			switch {
			case ch == '\'':
				state = singleQuote
			case ch == '"':
				state = doubleQuote
			case ch == '-' && next == '-':
				state = lineComment
				current.WriteByte(ch)
				i++
				current.WriteByte(next)
				continue
			case ch == '/' && next == '*':
				state = blockComment
				current.WriteByte(ch)
				i++
				current.WriteByte(next)
				continue
			case ch == ';':
				appendStatement()
				continue
			}
		case singleQuote:
			if ch == '\'' {
				if next == '\'' { // Escaped SQL string quote.
					current.WriteByte(ch)
					i++
					current.WriteByte(next)
					continue
				}
				state = normal
			}
		case doubleQuote:
			if ch == '"' {
				if next == '"' { // Escaped quoted-identifier quote.
					current.WriteByte(ch)
					i++
					current.WriteByte(next)
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
				current.WriteByte(ch)
				i++
				current.WriteByte(next)
				state = normal
				continue
			}
		}

		current.WriteByte(ch)
	}
	appendStatement()
	return statements
}
