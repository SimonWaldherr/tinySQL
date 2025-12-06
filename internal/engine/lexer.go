// Package engine contains the SQL lexer used by the parser.
//
// What: A minimal, whitespace- and comment-aware tokenizer that recognizes
// identifiers, keywords, numeric and string literals, and symbols.
// How: Single-pass rune-based scanner supporting -- and /* */ comments,
// uppercasing keywords, and preserving identifier case. Keywords are a fixed
// allow-list tailored to tinySQL features.
// Why: A compact lexer reduces parser complexity and keeps error messages
// local and actionable without external dependencies.
package engine

import (
	"strings"
	"unicode"
)

type tokenType int

const (
	tEOF tokenType = iota
	tIdent
	tNumber
	tString
	tSymbol
	tKeyword
)

type token struct {
	Typ tokenType
	Val string
	Pos int
}

type lexer struct {
	s   string
	pos int
}

func newLexer(s string) *lexer { return &lexer{s: s} }

func (lx *lexer) peek() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	return rune(lx.s[lx.pos])
}
func (lx *lexer) peekN(n int) rune {
	p := lx.pos + n
	if p >= len(lx.s) {
		return 0
	}
	return rune(lx.s[p])
}
func (lx *lexer) next() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	r := rune(lx.s[lx.pos])
	lx.pos++
	return r
}
func (lx *lexer) skipWS() {
	for {
		if lx.pos >= len(lx.s) {
			return
		}
		r := rune(lx.s[lx.pos])
		if unicode.IsSpace(r) {
			lx.pos++
			continue
		}
		// -- Kommentar
		if r == '-' && lx.peekN(1) == '-' {
			lx.pos += 2
			for lx.pos < len(lx.s) && lx.s[lx.pos] != '\n' {
				lx.pos++
			}
			continue
		}
		// /* block */
		if r == '/' && lx.peekN(1) == '*' {
			lx.pos += 2
			for lx.pos < len(lx.s) {
				if lx.s[lx.pos] == '*' && lx.peekN(1) == '/' {
					lx.pos += 2
					break
				}
				lx.pos++
			}
			continue
		}
		return
	}
}

func (lx *lexer) nextToken() token {
	lx.skipWS()
	start := lx.pos
	if start >= len(lx.s) {
		return token{Typ: tEOF, Pos: start}
	}
	r := lx.peek()

	// Dispatch to specific tokenizers based on first character
	if r == '\'' {
		return lx.tokenizeString(start)
	}
	// double-quoted identifiers (SQL-style) -> treat as identifier preserving case
	if r == '"' {
		return lx.tokenizeQuotedIdent(start)
	}
	if unicode.IsDigit(r) {
		return lx.tokenizeNumber(start)
	}
	if unicode.IsLetter(r) || r == '_' {
		return lx.tokenizeIdentOrKeyword(start)
	}
	return lx.tokenizeSymbol(start)
}

// Helper: tokenize string literals
func (lx *lexer) tokenizeString(start int) token {
	lx.next() // consume opening quote
	var val strings.Builder
	for lx.pos < len(lx.s) {
		ch := lx.next()
		if ch == '\'' {
			if lx.peek() == '\'' {
				lx.next()
				val.WriteRune('\'')
				continue
			}
			break
		}
		val.WriteRune(ch)
	}
	return token{Typ: tString, Val: val.String(), Pos: start}
}

// tokenizeQuotedIdent handles SQL-style double-quoted identifiers.
// It preserves case and allows embedded double-quotes escaped by doubling ("").
func (lx *lexer) tokenizeQuotedIdent(start int) token {
	lx.next() // consume opening double-quote
	var val strings.Builder
	for lx.pos < len(lx.s) {
		ch := lx.next()
		if ch == '"' {
			if lx.peek() == '"' {
				lx.next()
				val.WriteRune('"')
				continue
			}
			break
		}
		val.WriteRune(ch)
	}
	// Return as identifier token (preserve original casing)
	return token{Typ: tIdent, Val: val.String(), Pos: start}
}

// Helper: tokenize numeric literals
func (lx *lexer) tokenizeNumber(start int) token {
	var val strings.Builder
	dot := false
	for lx.pos < len(lx.s) {
		ch := lx.peek()
		if unicode.IsDigit(ch) || (!dot && ch == '.') {
			if ch == '.' {
				dot = true
			}
			val.WriteRune(ch)
			lx.pos++
		} else {
			break
		}
	}
	return token{Typ: tNumber, Val: val.String(), Pos: start}
}

// Helper: tokenize identifiers and keywords
func (lx *lexer) tokenizeIdentOrKeyword(start int) token {
	var val strings.Builder
	for lx.pos < len(lx.s) {
		ch := lx.peek()
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
			val.WriteRune(ch)
			lx.pos++
		} else {
			break
		}
	}
	up := upper(val.String())
	if isKeyword(up) {
		return token{Typ: tKeyword, Val: up, Pos: start}
	}
	return token{Typ: tIdent, Val: val.String(), Pos: start}
}

// Helper: tokenize symbols and operators
func (lx *lexer) tokenizeSymbol(start int) token {
	r := lx.peek()
	switch r {
	case '(', ')', ',', '*', '+', '-', '/', '.', ';', '?':
		lx.next()
		return token{Typ: tSymbol, Val: string(r), Pos: start}
	case '=', '<', '>', '!':
		a := lx.next()
		b := lx.peek()
		if (a == '<' && (b == '=' || b == '>')) || (a == '>' && b == '=') || (a == '!' && b == '=') {
			lx.next()
			return token{Typ: tSymbol, Val: string(a) + string(b), Pos: start}
		}
		return token{Typ: tSymbol, Val: string(a), Pos: start}
	default:
		lx.next()
		return token{Typ: tSymbol, Val: string(r), Pos: start}
	}
}

func upper(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if 'a' <= r && r <= 'z' {
			out = append(out, r-32)
		} else {
			out = append(out, r)
		}
	}
	return string(out)
}

func isKeyword(up string) bool {
	switch up {
	case "RECURSIVE":
		return true
	case "SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
		"ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
		"CASE", "WHEN", "THEN", "ELSE", "END",
		"JOIN", "LEFT", "RIGHT", "OUTER", "ON", "AS",
		"UNION", "ALL", "EXCEPT", "INTERSECT", "WITH",
		"CREATE", "TABLE", "TEMP", "DROP", "ALTER", "ADD", "COLUMN",
		"INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE",
		"INDEX", "VIEW", "REPLACE", "IF", "EXISTS",
		"INT", "INT8", "INT16", "INT32", "INT64",
		"UINT", "UINT8", "UINT16", "UINT32", "UINT64",
		"FLOAT32", "FLOAT64", "FLOAT", "DOUBLE",
		"STRING", "TEXT", "RUNE", "BYTE",
		"BOOL", "BOOLEAN",
		"TIME", "DATE", "DATETIME", "TIMESTAMP", "DURATION",
		"JSON", "JSONB", "MAP", "SLICE", "ARRAY",
		"COMPLEX64", "COMPLEX128", "COMPLEX",
		"POINTER", "PTR", "INTERFACE",
		"PRIMARY", "FOREIGN", "KEY", "REFERENCES", "UNIQUE",
		"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "LIKE", "ESCAPE",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN",
		"COALESCE", "NULLIF", "NVL", "IFNULL", "NOW", "CURRENT_TIME", "CURRENT_DATE",
		"JSON_GET", "JSON_SET", "JSON_EXTRACT", "DATEDIFF",
		"LTRIM", "RTRIM", "TRIM", "REGEXP", "ISNULL",
		"BASE64", "BASE64_DECODE",
		"UPPER", "LOWER", "CONCAT", "CONCAT_WS", "LENGTH", "SUBSTRING", "SUBSTR",
		"MD5", "SHA1", "SHA256", "SHA512",
		"INSTR", "LOCATE", "POSITION", "REVERSE", "REPEAT", "PRINTF", "FORMAT",
		"CHAR_LENGTH", "LPAD", "RPAD",
		"ABS", "ROUND", "FLOOR", "CEIL", "CEILING",
		"MOD", "POWER", "POW", "SQRT", "LOG", "LN", "LOG10", "LOG2", "EXP",
		"SIGN", "TRUNCATE", "TRUNC", "PI",
		"SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2",
		"DEGREES", "RADIANS",
		"GREATEST", "LEAST", "IIF",
		"STRFTIME", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
		"DAYOFWEEK", "DAYOFYEAR", "WEEKOFYEAR", "QUARTER",
		"DATE_ADD", "DATE_SUB", "DATEADD", "DATESUB",
		"RANDOM", "RAND", "CAST",
		"SPACE", "ASCII", "CHAR", "CHR", "INITCAP", "SPLIT_PART", "SOUNDEX",
		"QUOTE", "HEX", "UNHEX",
		"UUID", "TYPEOF", "VERSION",
		"IN_PERIOD", "EXTRACT", "DATE_TRUNC", "EOMONTH", "ADD_MONTHS",
		"REGEXP_MATCH", "REGEXP_EXTRACT", "REGEXP_REPLACE",
		"SPLIT", "FIRST", "LAST", "ARRAY_LENGTH", "ARRAY_CONTAINS", "IN_ARRAY",
		"ARRAY_JOIN", "ARRAY_DISTINCT", "ARRAY_SORT",
		"ROW_NUMBER", "LAG", "LEAD", "MOVING_SUM", "MOVING_AVG",
		"MIN_BY", "MAX_BY", "ARG_MIN", "ARG_MAX", "FIRST_VALUE", "LAST_VALUE",
		"OVER", "PARTITION", "ROWS", "RANGE", "BETWEEN", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW":
		return true
	default:
		return false
	}
}
