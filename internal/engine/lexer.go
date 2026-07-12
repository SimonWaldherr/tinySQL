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
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenType int

const (
	tEOF tokenType = iota
	tIdent
	tNumber
	tString
	tBlob
	tSymbol
	tKeyword
)

type token struct {
	Typ tokenType
	Val string
	Pos int
	Err string
}

type lexer struct {
	s   string
	pos int
}

func newLexer(s string) *lexer { return &lexer{s: s} }

// peek returns the rune at the current position, decoding UTF-8 so that
// multi-byte characters in string literals and identifiers survive lexing
// (a byte-wise cast turned 'héllo' into 'hÃ©llo').
func (lx *lexer) peek() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(lx.s[lx.pos:])
	return r
}

// peekN looks ahead n BYTES. It is only used for ASCII lookahead
// (comment markers, operators), where byte offsets equal rune offsets.
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
	r, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
	lx.pos += w
	return r
}
func (lx *lexer) skipWS() {
	for {
		if lx.pos >= len(lx.s) {
			return
		}
		r, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
		if unicode.IsSpace(r) {
			lx.pos += w
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
	// SQL binary literals use X'0123abcd' (case-insensitive). Keep this
	// distinct from text literals so a BLOB can never silently become UTF-8
	// text while crossing the parser or database/sql boundary.
	if (r == 'x' || r == 'X') && lx.pos+1 < len(lx.s) && lx.s[lx.pos+1] == '\'' {
		return lx.tokenizeBlob(start)
	}
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

// tokenizeBlob reads a SQL X'hex' literal and validates it eagerly. The
// parser receives decoded raw bytes in token.Val. Decode failures travel in
// token.Err, never as a sentinel in the payload itself.
func (lx *lexer) tokenizeBlob(start int) token {
	lx.pos += 2 // X and opening quote
	begin := lx.pos
	for lx.pos < len(lx.s) && lx.s[lx.pos] != '\'' {
		lx.pos++
	}
	hexText := lx.s[begin:lx.pos]
	if lx.pos < len(lx.s) {
		lx.pos++
	}
	decoded, err := hex.DecodeString(hexText)
	if err != nil {
		return token{Typ: tBlob, Pos: start, Err: err.Error()}
	}
	return token{Typ: tBlob, Val: string(decoded), Pos: start}
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
		ch, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
			val.WriteRune(ch)
			lx.pos += w
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
	// fast path: skip allocation if no lowercase ASCII present
	hasLower := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			hasLower = true
			break
		}
	}
	if !hasLower {
		return s
	}
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

func isKeyword(up string) bool {
	switch up {
	case "RECURSIVE":
		return true
	case "SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
		"ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
		"CASE", "WHEN", "THEN", "ELSE", "END",
		"JOIN", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER", "ON", "AS",
		"UNION", "ALL", "EXCEPT", "INTERSECT", "WITH",
		"CREATE", "TABLE", "TEMP", "DROP", "ALTER", "ADD", "COLUMN",
		"INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE", "RETURNING",
		"CALL",
		"MATERIALIZED", "REFRESH", "STALE", "AFTER", "EVERY", "DAILY", "AT",
		"TIMEZONE", "DATA", "NO", "DEMAND", "INVALIDATE", "CHANGE", "HOURS", "MINUTES",
		"SECONDS", "DAYS", "MILLISECOND", "MILLISECONDS", "MS",
		"CONCURRENTLY",
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
		"PRIMARY", "FOREIGN", "KEY", "REFERENCES", "UNIQUE", "DEFAULT", "CASCADE", "RESTRICT", "ACTION",
		"USER", "ROLE", "GRANT", "REVOKE", "PASSWORD", "ENABLE", "DISABLE",
		"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "LIKE", "ESCAPE",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN",
		"COALESCE", "NULLIF", "NVL", "IFNULL", "NOW", "CURRENT_TIME", "CURRENT_DATE",
		"JSON_GET", "JSON_SET", "JSON_EXTRACT", "DATEDIFF",
		"LTRIM", "RTRIM", "TRIM", "REGEXP", "ISNULL", "ROW_TO_TEXT",
		"ILIKE", "RLIKE", "GLOB", "SIMILAR", "TO",
		"LEVENSHTEIN", "EDIT_DISTANCE",
		"CONTAINS", "STARTS_WITH", "ENDS_WITH",
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
		"ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "MOVING_SUM", "MOVING_AVG",
		"MIN_BY", "MAX_BY", "ARG_MIN", "ARG_MAX", "FIRST_VALUE", "LAST_VALUE",
		"OVER", "PARTITION", "ROWS", "RANGE", "BETWEEN", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW",
		// Vector / embedding types and functions
		"VECTOR", "EMBEDDING",
		"VEC_FROM_JSON", "VEC_TO_JSON", "VEC_DIM", "VEC_NORM", "VEC_NORMALIZE",
		"VEC_ADD", "VEC_SUB", "VEC_MUL", "VEC_SCALE",
		"VEC_DOT", "VEC_DOT_PRODUCT", "VEC_INNER_PRODUCT",
		"VEC_COSINE_SIMILARITY", "VEC_COSINE_DISTANCE",
		"VEC_L2_DISTANCE", "VEC_EUCLIDEAN_DISTANCE",
		"VEC_MANHATTAN_DISTANCE", "VEC_L1_DISTANCE",
		"VEC_DISTANCE",
		"VEC_SLICE", "VEC_CONCAT", "VEC_QUANTIZE", "VEC_RANDOM", "VEC_AVG",
		"VEC_SEARCH", "VEC_TOP_K",
		// Extra data types
		"YAML", "URL", "HASH", "BITMAP",
		// Extra type functions
		"YAML_PARSE", "YAML_GET",
		"URL_PARSE", "URL_ENCODE", "URL_DECODE",
		"BITMAP_NEW", "BITMAP_SET", "BITMAP_GET", "BITMAP_COUNT", "BITMAP_OR", "BITMAP_AND",
		// Trigger keywords ("FOR" was previously missing here, meaning
		// "FOR EACH ROW" only matched when the user typed "FOR" in that
		// exact uppercase form — tIdent tokens preserve original case.)
		"TRIGGER", "EACH", "BEFORE", "INSTEAD", "OF", "NEW", "OLD", "FOR",
		// Statement wrappers and block delimiters
		"EXPLAIN", "BEGIN",
		// FTS keywords
		"VIRTUAL", "USING", "MATCH",
		"FTS_MATCH", "FTS_RANK", "FTS_SNIPPET", "BM25",
		// PIVOT keyword
		"PIVOT",
		// SQL:2008 OFFSET ... FETCH {FIRST|NEXT} ... {ROW|ROWS} ONLY
		"FETCH", "NEXT", "ONLY":
		return true
	case "PRAGMA":
		return true
	default:
		return false
	}
}
