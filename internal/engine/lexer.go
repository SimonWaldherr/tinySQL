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
	// scratch backs upperInto's uppercased copy of an identifier/keyword
	// candidate, reused across every call for this lexer (one lexer per
	// parsed statement) instead of allocating fresh on each one. See
	// upperInto.
	scratch []byte
}

func newLexer(s string) *lexer { return &lexer{s: s} }

// peek returns the rune at the current position, decoding UTF-8 so that
// multi-byte characters in string literals and identifiers survive lexing
// (a byte-wise cast turned 'héllo' into 'hÃ©llo'). ASCII (the overwhelming
// majority of SQL source -- keywords, identifiers, operators) short-circuits
// the decode: an ASCII byte's rune value is the byte itself by definition,
// so this skips utf8.DecodeRuneInString's function-call and branch overhead
// for that case without changing its result.
func (lx *lexer) peek() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	if c := lx.s[lx.pos]; c < utf8.RuneSelf {
		return rune(c)
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
	if c := lx.s[lx.pos]; c < utf8.RuneSelf {
		lx.pos++
		return rune(c)
	}
	r, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
	lx.pos += w
	return r
}

// skipWS skips whitespace and comments. The ASCII branch below matches
// exactly the code points unicode.IsSpace treats as whitespace in the ASCII
// range (space, tab, LF, VT, FF, CR) -- verified against the stdlib table,
// not assumed -- so this changes performance, not behavior; anything outside
// ASCII (a Unicode space separator) falls back to the general decode+
// IsSpace path exactly as before.
func (lx *lexer) skipWS() {
	for {
		if lx.pos >= len(lx.s) {
			return
		}
		if c := lx.s[lx.pos]; c < utf8.RuneSelf {
			switch c {
			case ' ', '\t', '\n', '\r', '\v', '\f':
				lx.pos++
				continue
			case '-':
				if lx.peekN(1) == '-' {
					lx.pos += 2
					for lx.pos < len(lx.s) && lx.s[lx.pos] != '\n' {
						lx.pos++
					}
					continue
				}
				return
			case '/':
				if lx.peekN(1) == '*' {
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
			default:
				return
			}
		}
		r, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
		if unicode.IsSpace(r) {
			lx.pos += w
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
	// Fast path: most string literals contain no embedded '' escape, so scan
	// ahead byte-wise for the closing quote first. A single-byte ASCII quote
	// can never occur as a continuation byte of a multi-byte UTF-8 sequence,
	// so this scan is safe regardless of the literal's content, and slicing
	// the original string is both zero-copy and skips the decode/re-encode
	// strings.Builder.WriteRune would otherwise do for every byte. The slow
	// path below decodes every byte through utf8.DecodeRuneInString, which
	// silently replaces malformed UTF-8 with U+FFFD -- so a plain slice is
	// only equivalent when the content is valid UTF-8; malformed content
	// must still fall back to the slow path to get the same sanitization.
	contentStart := lx.pos
	for i := contentStart; i < len(lx.s); i++ {
		if lx.s[i] != '\'' {
			continue
		}
		if i+1 < len(lx.s) && lx.s[i+1] == '\'' {
			break // escaped quote present -- fall back to the slow path
		}
		if content := lx.s[contentStart:i]; utf8.ValidString(content) {
			lx.pos = i + 1
			return token{Typ: tString, Val: content, Pos: start}
		}
		break // malformed UTF-8 -- fall back to the slow path to sanitize it
	}
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
	// Same zero-copy fast path as tokenizeString, for the common case of no
	// embedded "" escape and valid UTF-8 content (see tokenizeString for why
	// malformed UTF-8 must still fall back to the sanitizing slow path).
	contentStart := lx.pos
	for i := contentStart; i < len(lx.s); i++ {
		if lx.s[i] != '"' {
			continue
		}
		if i+1 < len(lx.s) && lx.s[i+1] == '"' {
			break // escaped quote present -- fall back to the slow path
		}
		if content := lx.s[contentStart:i]; utf8.ValidString(content) {
			lx.pos = i + 1
			return token{Typ: tIdent, Val: content, Pos: start}
		}
		break // malformed UTF-8 -- fall back to the slow path to sanitize it
	}
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

// Helper: tokenize numeric literals. The token is a contiguous run in the
// source, so it is returned as a zero-copy substring rather than accumulated
// into a strings.Builder (digits and '.' are single-byte, so lx.pos++ is safe).
func (lx *lexer) tokenizeNumber(start int) token {
	dot := false
	for lx.pos < len(lx.s) {
		if ch := lx.s[lx.pos]; ch < utf8.RuneSelf {
			if (ch >= '0' && ch <= '9') || (!dot && ch == '.') {
				if ch == '.' {
					dot = true
				}
				lx.pos++
				continue
			}
			break
		}
		ch, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
		if unicode.IsDigit(ch) || (!dot && ch == '.') {
			if ch == '.' {
				dot = true
			}
			lx.pos += w
		} else {
			break
		}
	}
	// Accept SQL/scientific notation (for example 1.25e-3). Only consume the
	// exponent marker when it is followed by at least one digit; otherwise an
	// identifier beginning with `e` remains a separate token and produces the
	// normal parser diagnostic.
	exponent := lx.pos
	if lx.pos < len(lx.s) && (lx.s[lx.pos] == 'e' || lx.s[lx.pos] == 'E') {
		lx.pos++
		if lx.pos < len(lx.s) && (lx.s[lx.pos] == '+' || lx.s[lx.pos] == '-') {
			lx.pos++
		}
		digits := lx.pos
		for lx.pos < len(lx.s) && unicode.IsDigit(lx.peek()) {
			lx.next()
		}
		if lx.pos == digits {
			lx.pos = exponent
		}
	}
	return token{Typ: tNumber, Val: lx.s[start:lx.pos], Pos: start}
}

// Helper: tokenize identifiers and keywords. Like tokenizeNumber, the token is
// a contiguous source run returned as a zero-copy substring. The ASCII branch
// (letters, digits, '_', '.') covers every character SQL identifiers and
// tinySQL's keyword list actually use; it is checked first because it is the
// overwhelming majority of input and needs neither a UTF-8 decode nor a
// unicode-table lookup, only a byte-range test. Anything outside ASCII falls
// back to the general unicode.IsLetter/IsDigit path so a multi-byte
// identifier (e.g. 'héllo') still lexes correctly -- the rune width w is
// exactly what keeps that case correct.
func (lx *lexer) tokenizeIdentOrKeyword(start int) token {
	hasDot := false
	for lx.pos < len(lx.s) {
		if c := lx.s[lx.pos]; c < utf8.RuneSelf {
			if isIdentASCIIByte(c) {
				hasDot = hasDot || c == '.'
				lx.pos++
				continue
			}
			break
		}
		ch, w := utf8.DecodeRuneInString(lx.s[lx.pos:])
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) {
			lx.pos += w
		} else {
			break
		}
	}
	val := lx.s[start:lx.pos]
	// A dotted token is necessarily a qualified identifier: no keyword in
	// the grammar contains '.', so avoid allocating an uppercased copy merely
	// to reject it from isKeyword. Qualified references dominate joins and
	// projections, making this a hot-path allocation reduction.
	if hasDot {
		return token{Typ: tIdent, Val: val, Pos: start}
	}
	if up := lx.upperInto(val); isKeyword(up) {
		return token{Typ: tKeyword, Val: up, Pos: start}
	}
	return token{Typ: tIdent, Val: val, Pos: start}
}

// isIdentASCIIByte reports whether an ASCII byte can continue an identifier
// or keyword: a letter, digit, underscore, or the '.' that lets a qualified
// name (t.col) lex as one token. Matches
// unicode.IsLetter(rune(c))||unicode.IsDigit(rune(c))||c=='_'||c=='.'
// exactly for every c < utf8.RuneSelf.
func isIdentASCIIByte(c byte) bool {
	return c == '_' || c == '.' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// asciiSingleChar holds the 128 one-byte ASCII strings, indexed by byte
// value, built once at package init. tokenizeSymbol returns a SLICE of this
// table (asciiSingleChar[c:c+1]) for its single-character tokens instead of
// converting a rune or byte to a string: slicing a string is a zero-copy
// view into existing backing memory, so it allocates nothing, where EVERY
// form of value-to-string conversion does. This was verified directly,
// against an earlier, mistaken version of this comment: string(rune) and
// string(byte(r)) both allocate one object per call (measured ~23ns/1
// alloc each, no difference between them) -- there is no free single-byte
// conversion in Go for a value not known at compile time. Slicing a
// pre-built table is what actually avoids the allocation.
var asciiSingleChar = buildASCIISingleCharTable()

func buildASCIISingleCharTable() string {
	b := make([]byte, utf8.RuneSelf)
	for i := range b {
		b[i] = byte(i)
	}
	return string(b)
}

// Helper: tokenize symbols and operators. Every case here is a specific
// ASCII literal, so its result is either a slice of asciiSingleChar or (for
// the two-character operators <=, <>, >=, != and ||) a string constant --
// both zero-allocation.
// The default branch can still see a non-ASCII rune (any character
// nextToken() didn't already route elsewhere), so it keeps the original,
// rune-safe conversion, which does allocate but only for that rare input.
func (lx *lexer) tokenizeSymbol(start int) token {
	r := lx.peek()
	switch r {
	case '(', ')', ',', '*', '+', '-', '/', '.', ';', '?':
		lx.next()
		return token{Typ: tSymbol, Val: asciiSingleChar[r : r+1], Pos: start}
	case '|':
		// "||" is SQL's string concatenation operator. A lone '|' stays a
		// single-character symbol, exactly as the default branch produced
		// before this case existed, so that "a | b" keeps failing with the
		// parser's unexpected-token error instead of being silently read as a
		// concatenation (or as a bitwise OR, which tinySQL does not have).
		lx.next()
		if lx.peek() == '|' {
			lx.next()
			return token{Typ: tSymbol, Val: "||", Pos: start}
		}
		return token{Typ: tSymbol, Val: asciiSingleChar[r : r+1], Pos: start}
	case '=', '<', '>', '!':
		a := lx.next()
		b := lx.peek()
		switch {
		case a == '<' && b == '=':
			lx.next()
			return token{Typ: tSymbol, Val: "<=", Pos: start}
		case a == '<' && b == '>':
			lx.next()
			return token{Typ: tSymbol, Val: "<>", Pos: start}
		case a == '>' && b == '=':
			lx.next()
			return token{Typ: tSymbol, Val: ">=", Pos: start}
		case a == '!' && b == '=':
			lx.next()
			return token{Typ: tSymbol, Val: "!=", Pos: start}
		}
		return token{Typ: tSymbol, Val: asciiSingleChar[a : a+1], Pos: start}
	default:
		lx.next()
		if r < utf8.RuneSelf {
			return token{Typ: tSymbol, Val: asciiSingleChar[r : r+1], Pos: start}
		}
		return token{Typ: tSymbol, Val: string(r), Pos: start}
	}
}

// upper returns s uppercased (ASCII only, which is all tinySQL's keyword
// list and column-type-name parsing ever need to match against), allocating
// only when s actually contains a lowercase ASCII byte. Used by the cold,
// once-per-statement call sites in parse_column_type.go and
// parse_statement.go; the per-token hot path in tokenizeIdentOrKeyword uses
// upperInto instead, which reuses a buffer across many calls.
func upper(s string) string {
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

// upperInto is upper's contract (ASCII-only uppercase, no-op fast path)
// implemented for the per-token hot path in tokenizeIdentOrKeyword, which
// calls this once for every identifier and keyword lexed.
//
// The original implementation (upper, above) allocated twice on the
// has-lowercase path: once via []byte(s) for a mutable copy, and again via
// string(b) to hand isKeyword's switch a string. This reuses lx.scratch --
// one buffer per lexer, i.e. per parsed statement, grown only when a longer
// candidate appears than any seen so far -- to fold case into, cutting that
// to the one allocation string(lx.scratch[:len(s)]) must still make (Go
// strings are immutable, so the switch needs its own independent copy;
// nothing shorter than a real allocation can supply that).
func (lx *lexer) upperInto(s string) string {
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
	if cap(lx.scratch) < len(s) {
		lx.scratch = make([]byte, len(s))
	}
	buf := lx.scratch[:len(s)]
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		buf[i] = c
	}
	return string(buf)
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
		"INSERT", "INTO", "VALUES", "CONFLICT", "DO", "NOTHING", "RENAME",
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
		"OVERLAPS", "RANGE_MERGE",
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
		// Spatial geometry type
		"GEOMETRY", "GEOM",
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
		"EXPLAIN", "ANALYZE", "BEGIN",
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
