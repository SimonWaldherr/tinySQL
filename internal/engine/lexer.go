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
	// Reused only for unusually long candidates; ordinary tokens use a
	// stack buffer in canonicalKeyword.
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
					for lx.pos < len(lx.s) && lx.s[lx.pos] != '\n' && lx.s[lx.pos] != '\r' {
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

func (lx *lexer) tokenizeString(start int) token {
	return lx.tokenizeQuoted(start, '\'', tString)
}

func (lx *lexer) tokenizeQuotedIdent(start int) token {
	return lx.tokenizeQuoted(start, '"', tIdent)
}

// tokenizeQuoted scans whole byte spans between ASCII quote delimiters. A
// delimiter cannot split a UTF-8 rune. Count doubled quotes before allocating
// so escaped values need one exactly sized buffer; plain values remain slices
// of the SQL source.
func (lx *lexer) tokenizeQuoted(start int, quote byte, typ tokenType) token {
	begin := start + 1
	end := begin
	escapes := 0
	for {
		i := strings.IndexByte(lx.s[end:], quote)
		if i < 0 {
			end = len(lx.s)
			lx.pos = end
			break
		}
		end += i
		if end+1 < len(lx.s) && lx.s[end+1] == quote {
			escapes++
			end += 2
			continue
		}
		lx.pos = end + 1
		break
	}
	content := lx.s[begin:end]
	if !utf8.ValidString(content) {
		// Keep the historical replacement of each malformed byte with U+FFFD.
		// strings.ToValidUTF8 would collapse consecutive malformed bytes.
		lx.pos = begin
		return lx.tokenizeQuotedRunes(start, quote, typ)
	}
	if escapes == 0 {
		return token{Typ: typ, Val: content, Pos: start}
	}
	var value strings.Builder
	value.Grow(len(content) - escapes)
	for escapes > 0 {
		i := strings.IndexByte(content, quote)
		value.WriteString(content[:i+1])
		content = content[i+2:]
		escapes--
	}
	value.WriteString(content)
	return token{Typ: typ, Val: value.String(), Pos: start}
}

func (lx *lexer) tokenizeQuotedRunes(start int, quote byte, typ tokenType) token {
	var value strings.Builder
	for lx.pos < len(lx.s) {
		ch := lx.next()
		if ch == rune(quote) {
			if lx.peek() == rune(quote) {
				lx.next()
			} else {
				break
			}
		}
		value.WriteRune(ch)
	}
	return token{Typ: typ, Val: value.String(), Pos: start}
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
	// to reject it from keywordValue. Qualified references dominate joins and
	// projections, making this a hot-path allocation reduction.
	if hasDot {
		return token{Typ: tIdent, Val: val, Pos: start}
	}
	if keyword := lx.canonicalKeyword(val); keyword != "" {
		return token{Typ: tKeyword, Val: keyword, Pos: start}
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
// once-per-statement call sites in parse_column_type.go and parse_statement.go.
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

// canonicalKeyword classifies a candidate without allocating an uppercase
// copy. keywordValue returns constant strings and never retains its argument,
// so the temporary string view of the stack buffer does not allocate, even
// when a lowercase keyword matches.
func (lx *lexer) canonicalKeyword(s string) string {
	hasLower := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			hasLower = true
			break
		}
	}
	if !hasLower {
		return keywordValue(s)
	}
	var small [32]byte
	buf := small[:]
	if len(s) > len(buf) {
		// Reuse storage for long identifiers and keep lookup correct if a
		// future keyword exceeds the stack buffer.
		if cap(lx.scratch) < len(s) {
			lx.scratch = make([]byte, len(s))
		}
		buf = lx.scratch
	}
	buf = buf[:len(s)]
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	return keywordValue(string(buf))
}

// keywordValue returns the canonical constant for each keyword. Returning a
// constant, rather than the candidate, keeps case-folded lexer buffers from
// escaping and gives retained tokens immutable values without a copy.
func keywordValue(up string) string {
	switch up {
	case "RECURSIVE":
		return "RECURSIVE"
	case "SELECT":
		return "SELECT"
	case "DISTINCT":
		return "DISTINCT"
	case "FROM":
		return "FROM"
	case "WHERE":
		return "WHERE"
	case "GROUP":
		return "GROUP"
	case "BY":
		return "BY"
	case "HAVING":
		return "HAVING"
	case "ORDER":
		return "ORDER"
	case "ASC":
		return "ASC"
	case "DESC":
		return "DESC"
	case "LIMIT":
		return "LIMIT"
	case "OFFSET":
		return "OFFSET"
	case "CASE":
		return "CASE"
	case "WHEN":
		return "WHEN"
	case "THEN":
		return "THEN"
	case "ELSE":
		return "ELSE"
	case "END":
		return "END"
	case "JOIN":
		return "JOIN"
	case "LEFT":
		return "LEFT"
	case "RIGHT":
		return "RIGHT"
	case "FULL":
		return "FULL"
	case "CROSS":
		return "CROSS"
	case "OUTER":
		return "OUTER"
	case "ON":
		return "ON"
	case "AS":
		return "AS"
	case "UNION":
		return "UNION"
	case "ALL":
		return "ALL"
	case "EXCEPT":
		return "EXCEPT"
	case "INTERSECT":
		return "INTERSECT"
	case "WITH":
		return "WITH"
	case "CREATE":
		return "CREATE"
	case "TABLE":
		return "TABLE"
	case "TEMP":
		return "TEMP"
	case "DROP":
		return "DROP"
	case "ALTER":
		return "ALTER"
	case "ADD":
		return "ADD"
	case "COLUMN":
		return "COLUMN"
	case "INSERT":
		return "INSERT"
	case "INTO":
		return "INTO"
	case "VALUES":
		return "VALUES"
	case "CONFLICT":
		return "CONFLICT"
	case "DO":
		return "DO"
	case "NOTHING":
		return "NOTHING"
	case "RENAME":
		return "RENAME"
	case "UPDATE":
		return "UPDATE"
	case "SET":
		return "SET"
	case "DELETE":
		return "DELETE"
	case "RETURNING":
		return "RETURNING"
	case "CALL":
		return "CALL"
	case "MATERIALIZED":
		return "MATERIALIZED"
	case "REFRESH":
		return "REFRESH"
	case "STALE":
		return "STALE"
	case "AFTER":
		return "AFTER"
	case "EVERY":
		return "EVERY"
	case "DAILY":
		return "DAILY"
	case "AT":
		return "AT"
	case "TIMEZONE":
		return "TIMEZONE"
	case "DATA":
		return "DATA"
	case "NO":
		return "NO"
	case "DEMAND":
		return "DEMAND"
	case "INVALIDATE":
		return "INVALIDATE"
	case "CHANGE":
		return "CHANGE"
	case "HOURS":
		return "HOURS"
	case "MINUTES":
		return "MINUTES"
	case "SECONDS":
		return "SECONDS"
	case "DAYS":
		return "DAYS"
	case "MILLISECOND":
		return "MILLISECOND"
	case "MILLISECONDS":
		return "MILLISECONDS"
	case "MS":
		return "MS"
	case "CONCURRENTLY":
		return "CONCURRENTLY"
	case "INDEX":
		return "INDEX"
	case "VIEW":
		return "VIEW"
	case "REPLACE":
		return "REPLACE"
	case "IF":
		return "IF"
	case "EXISTS":
		return "EXISTS"
	case "INT":
		return "INT"
	case "INT8":
		return "INT8"
	case "INT16":
		return "INT16"
	case "INT32":
		return "INT32"
	case "INT64":
		return "INT64"
	case "UINT":
		return "UINT"
	case "UINT8":
		return "UINT8"
	case "UINT16":
		return "UINT16"
	case "UINT32":
		return "UINT32"
	case "UINT64":
		return "UINT64"
	case "FLOAT32":
		return "FLOAT32"
	case "FLOAT64":
		return "FLOAT64"
	case "FLOAT":
		return "FLOAT"
	case "DOUBLE":
		return "DOUBLE"
	case "STRING":
		return "STRING"
	case "TEXT":
		return "TEXT"
	case "RUNE":
		return "RUNE"
	case "BYTE":
		return "BYTE"
	case "BOOL":
		return "BOOL"
	case "BOOLEAN":
		return "BOOLEAN"
	case "TIME":
		return "TIME"
	case "DATE":
		return "DATE"
	case "DATETIME":
		return "DATETIME"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "DURATION":
		return "DURATION"
	case "JSON":
		return "JSON"
	case "JSONB":
		return "JSONB"
	case "MAP":
		return "MAP"
	case "SLICE":
		return "SLICE"
	case "ARRAY":
		return "ARRAY"
	case "COMPLEX64":
		return "COMPLEX64"
	case "COMPLEX128":
		return "COMPLEX128"
	case "COMPLEX":
		return "COMPLEX"
	case "POINTER":
		return "POINTER"
	case "PTR":
		return "PTR"
	case "INTERFACE":
		return "INTERFACE"
	case "PRIMARY":
		return "PRIMARY"
	case "FOREIGN":
		return "FOREIGN"
	case "KEY":
		return "KEY"
	case "REFERENCES":
		return "REFERENCES"
	case "UNIQUE":
		return "UNIQUE"
	case "DEFAULT":
		return "DEFAULT"
	case "CASCADE":
		return "CASCADE"
	case "RESTRICT":
		return "RESTRICT"
	case "ACTION":
		return "ACTION"
	case "USER":
		return "USER"
	case "ROLE":
		return "ROLE"
	case "GRANT":
		return "GRANT"
	case "REVOKE":
		return "REVOKE"
	case "PASSWORD":
		return "PASSWORD"
	case "ENABLE":
		return "ENABLE"
	case "DISABLE":
		return "DISABLE"
	case "AND":
		return "AND"
	case "OR":
		return "OR"
	case "NOT":
		return "NOT"
	case "IS":
		return "IS"
	case "NULL":
		return "NULL"
	case "TRUE":
		return "TRUE"
	case "FALSE":
		return "FALSE"
	case "IN":
		return "IN"
	case "LIKE":
		return "LIKE"
	case "ESCAPE":
		return "ESCAPE"
	case "COUNT":
		return "COUNT"
	case "SUM":
		return "SUM"
	case "AVG":
		return "AVG"
	case "MIN":
		return "MIN"
	case "MAX":
		return "MAX"
	case "MEDIAN":
		return "MEDIAN"
	case "COALESCE":
		return "COALESCE"
	case "NULLIF":
		return "NULLIF"
	case "NVL":
		return "NVL"
	case "IFNULL":
		return "IFNULL"
	case "NOW":
		return "NOW"
	case "CURRENT_TIME":
		return "CURRENT_TIME"
	case "CURRENT_DATE":
		return "CURRENT_DATE"
	case "JSON_GET":
		return "JSON_GET"
	case "JSON_SET":
		return "JSON_SET"
	case "JSON_EXTRACT":
		return "JSON_EXTRACT"
	case "DATEDIFF":
		return "DATEDIFF"
	case "LTRIM":
		return "LTRIM"
	case "RTRIM":
		return "RTRIM"
	case "TRIM":
		return "TRIM"
	case "REGEXP":
		return "REGEXP"
	case "ISNULL":
		return "ISNULL"
	case "ROW_TO_TEXT":
		return "ROW_TO_TEXT"
	case "ILIKE":
		return "ILIKE"
	case "RLIKE":
		return "RLIKE"
	case "GLOB":
		return "GLOB"
	case "SIMILAR":
		return "SIMILAR"
	case "TO":
		return "TO"
	case "LEVENSHTEIN":
		return "LEVENSHTEIN"
	case "EDIT_DISTANCE":
		return "EDIT_DISTANCE"
	case "CONTAINS":
		return "CONTAINS"
	case "STARTS_WITH":
		return "STARTS_WITH"
	case "ENDS_WITH":
		return "ENDS_WITH"
	case "BASE64":
		return "BASE64"
	case "BASE64_DECODE":
		return "BASE64_DECODE"
	case "UPPER":
		return "UPPER"
	case "LOWER":
		return "LOWER"
	case "CONCAT":
		return "CONCAT"
	case "CONCAT_WS":
		return "CONCAT_WS"
	case "LENGTH":
		return "LENGTH"
	case "SUBSTRING":
		return "SUBSTRING"
	case "SUBSTR":
		return "SUBSTR"
	case "MD5":
		return "MD5"
	case "SHA1":
		return "SHA1"
	case "SHA256":
		return "SHA256"
	case "SHA512":
		return "SHA512"
	case "INSTR":
		return "INSTR"
	case "LOCATE":
		return "LOCATE"
	case "POSITION":
		return "POSITION"
	case "REVERSE":
		return "REVERSE"
	case "REPEAT":
		return "REPEAT"
	case "PRINTF":
		return "PRINTF"
	case "FORMAT":
		return "FORMAT"
	case "CHAR_LENGTH":
		return "CHAR_LENGTH"
	case "LPAD":
		return "LPAD"
	case "RPAD":
		return "RPAD"
	case "ABS":
		return "ABS"
	case "ROUND":
		return "ROUND"
	case "FLOOR":
		return "FLOOR"
	case "CEIL":
		return "CEIL"
	case "CEILING":
		return "CEILING"
	case "MOD":
		return "MOD"
	case "POWER":
		return "POWER"
	case "POW":
		return "POW"
	case "SQRT":
		return "SQRT"
	case "LOG":
		return "LOG"
	case "LN":
		return "LN"
	case "LOG10":
		return "LOG10"
	case "LOG2":
		return "LOG2"
	case "EXP":
		return "EXP"
	case "SIGN":
		return "SIGN"
	case "TRUNCATE":
		return "TRUNCATE"
	case "TRUNC":
		return "TRUNC"
	case "PI":
		return "PI"
	case "SIN":
		return "SIN"
	case "COS":
		return "COS"
	case "TAN":
		return "TAN"
	case "ASIN":
		return "ASIN"
	case "ACOS":
		return "ACOS"
	case "ATAN":
		return "ATAN"
	case "ATAN2":
		return "ATAN2"
	case "DEGREES":
		return "DEGREES"
	case "RADIANS":
		return "RADIANS"
	case "GREATEST":
		return "GREATEST"
	case "LEAST":
		return "LEAST"
	case "IIF":
		return "IIF"
	case "STRFTIME":
		return "STRFTIME"
	case "YEAR":
		return "YEAR"
	case "MONTH":
		return "MONTH"
	case "DAY":
		return "DAY"
	case "HOUR":
		return "HOUR"
	case "MINUTE":
		return "MINUTE"
	case "SECOND":
		return "SECOND"
	case "DAYOFWEEK":
		return "DAYOFWEEK"
	case "DAYOFYEAR":
		return "DAYOFYEAR"
	case "WEEKOFYEAR":
		return "WEEKOFYEAR"
	case "QUARTER":
		return "QUARTER"
	case "DATE_ADD":
		return "DATE_ADD"
	case "DATE_SUB":
		return "DATE_SUB"
	case "DATEADD":
		return "DATEADD"
	case "DATESUB":
		return "DATESUB"
	case "OVERLAPS":
		return "OVERLAPS"
	case "RANGE_MERGE":
		return "RANGE_MERGE"
	case "RANDOM":
		return "RANDOM"
	case "RAND":
		return "RAND"
	case "CAST":
		return "CAST"
	case "SPACE":
		return "SPACE"
	case "ASCII":
		return "ASCII"
	case "CHAR":
		return "CHAR"
	case "CHR":
		return "CHR"
	case "INITCAP":
		return "INITCAP"
	case "SPLIT_PART":
		return "SPLIT_PART"
	case "SOUNDEX":
		return "SOUNDEX"
	case "QUOTE":
		return "QUOTE"
	case "HEX":
		return "HEX"
	case "UNHEX":
		return "UNHEX"
	case "UUID":
		return "UUID"
	case "TYPEOF":
		return "TYPEOF"
	case "VERSION":
		return "VERSION"
	case "IN_PERIOD":
		return "IN_PERIOD"
	case "EXTRACT":
		return "EXTRACT"
	case "DATE_TRUNC":
		return "DATE_TRUNC"
	case "EOMONTH":
		return "EOMONTH"
	case "ADD_MONTHS":
		return "ADD_MONTHS"
	case "REGEXP_MATCH":
		return "REGEXP_MATCH"
	case "REGEXP_EXTRACT":
		return "REGEXP_EXTRACT"
	case "REGEXP_REPLACE":
		return "REGEXP_REPLACE"
	case "SPLIT":
		return "SPLIT"
	case "FIRST":
		return "FIRST"
	case "LAST":
		return "LAST"
	case "ARRAY_LENGTH":
		return "ARRAY_LENGTH"
	case "ARRAY_CONTAINS":
		return "ARRAY_CONTAINS"
	case "IN_ARRAY":
		return "IN_ARRAY"
	case "ARRAY_JOIN":
		return "ARRAY_JOIN"
	case "ARRAY_DISTINCT":
		return "ARRAY_DISTINCT"
	case "ARRAY_SORT":
		return "ARRAY_SORT"
	case "ROW_NUMBER":
		return "ROW_NUMBER"
	case "RANK":
		return "RANK"
	case "DENSE_RANK":
		return "DENSE_RANK"
	case "LAG":
		return "LAG"
	case "LEAD":
		return "LEAD"
	case "MOVING_SUM":
		return "MOVING_SUM"
	case "MOVING_AVG":
		return "MOVING_AVG"
	case "MIN_BY":
		return "MIN_BY"
	case "MAX_BY":
		return "MAX_BY"
	case "ARG_MIN":
		return "ARG_MIN"
	case "ARG_MAX":
		return "ARG_MAX"
	case "FIRST_VALUE":
		return "FIRST_VALUE"
	case "LAST_VALUE":
		return "LAST_VALUE"
	case "OVER":
		return "OVER"
	case "PARTITION":
		return "PARTITION"
	case "ROWS":
		return "ROWS"
	case "RANGE":
		return "RANGE"
	case "BETWEEN":
		return "BETWEEN"
	case "UNBOUNDED":
		return "UNBOUNDED"
	case "PRECEDING":
		return "PRECEDING"
	case "FOLLOWING":
		return "FOLLOWING"
	case "CURRENT":
		return "CURRENT"
	case "ROW":
		return "ROW"
	// Vector / embedding types and functions
	case "VECTOR":
		return "VECTOR"
	case "EMBEDDING":
		return "EMBEDDING"
	// Spatial geometry type
	case "GEOMETRY":
		return "GEOMETRY"
	case "GEOM":
		return "GEOM"
	case "VEC_FROM_JSON":
		return "VEC_FROM_JSON"
	case "VEC_TO_JSON":
		return "VEC_TO_JSON"
	case "VEC_DIM":
		return "VEC_DIM"
	case "VEC_NORM":
		return "VEC_NORM"
	case "VEC_NORMALIZE":
		return "VEC_NORMALIZE"
	case "VEC_ADD":
		return "VEC_ADD"
	case "VEC_SUB":
		return "VEC_SUB"
	case "VEC_MUL":
		return "VEC_MUL"
	case "VEC_SCALE":
		return "VEC_SCALE"
	case "VEC_DOT":
		return "VEC_DOT"
	case "VEC_DOT_PRODUCT":
		return "VEC_DOT_PRODUCT"
	case "VEC_INNER_PRODUCT":
		return "VEC_INNER_PRODUCT"
	case "VEC_COSINE_SIMILARITY":
		return "VEC_COSINE_SIMILARITY"
	case "VEC_COSINE_DISTANCE":
		return "VEC_COSINE_DISTANCE"
	case "VEC_L2_DISTANCE":
		return "VEC_L2_DISTANCE"
	case "VEC_EUCLIDEAN_DISTANCE":
		return "VEC_EUCLIDEAN_DISTANCE"
	case "VEC_MANHATTAN_DISTANCE":
		return "VEC_MANHATTAN_DISTANCE"
	case "VEC_L1_DISTANCE":
		return "VEC_L1_DISTANCE"
	case "VEC_DISTANCE":
		return "VEC_DISTANCE"
	case "VEC_SLICE":
		return "VEC_SLICE"
	case "VEC_CONCAT":
		return "VEC_CONCAT"
	case "VEC_QUANTIZE":
		return "VEC_QUANTIZE"
	case "VEC_RANDOM":
		return "VEC_RANDOM"
	case "VEC_AVG":
		return "VEC_AVG"
	case "VEC_SEARCH":
		return "VEC_SEARCH"
	case "VEC_TOP_K":
		return "VEC_TOP_K"
	// Extra data types
	case "YAML":
		return "YAML"
	case "URL":
		return "URL"
	case "HASH":
		return "HASH"
	case "BITMAP":
		return "BITMAP"
	// Extra type functions
	case "YAML_PARSE":
		return "YAML_PARSE"
	case "YAML_GET":
		return "YAML_GET"
	case "URL_PARSE":
		return "URL_PARSE"
	case "URL_ENCODE":
		return "URL_ENCODE"
	case "URL_DECODE":
		return "URL_DECODE"
	case "BITMAP_NEW":
		return "BITMAP_NEW"
	case "BITMAP_SET":
		return "BITMAP_SET"
	case "BITMAP_GET":
		return "BITMAP_GET"
	case "BITMAP_COUNT":
		return "BITMAP_COUNT"
	case "BITMAP_OR":
		return "BITMAP_OR"
	case "BITMAP_AND":
		return "BITMAP_AND"
	// Trigger keywords ("FOR" was previously missing here, meaning
	// "FOR EACH ROW" only matched when the user typed "FOR" in that
	// exact uppercase form — tIdent tokens preserve original case.)
	case "TRIGGER":
		return "TRIGGER"
	case "EACH":
		return "EACH"
	case "BEFORE":
		return "BEFORE"
	case "INSTEAD":
		return "INSTEAD"
	case "OF":
		return "OF"
	case "NEW":
		return "NEW"
	case "OLD":
		return "OLD"
	case "FOR":
		return "FOR"
	// Statement wrappers and block delimiters
	case "EXPLAIN":
		return "EXPLAIN"
	case "ANALYZE":
		return "ANALYZE"
	case "BEGIN":
		return "BEGIN"
	// FTS keywords
	case "VIRTUAL":
		return "VIRTUAL"
	case "USING":
		return "USING"
	case "MATCH":
		return "MATCH"
	case "FTS_MATCH":
		return "FTS_MATCH"
	case "FTS_RANK":
		return "FTS_RANK"
	case "FTS_SNIPPET":
		return "FTS_SNIPPET"
	case "BM25":
		return "BM25"
	// PIVOT keyword
	case "PIVOT":
		return "PIVOT"
	// SQL:2008 OFFSET ... FETCH {FIRST|NEXT} ... {ROW|ROWS} ONLY
	case "FETCH":
		return "FETCH"
	case "NEXT":
		return "NEXT"
	case "ONLY":
		return "ONLY"
	case "PRAGMA":
		return "PRAGMA"
	default:
		return ""
	}
}
