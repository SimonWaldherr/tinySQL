package engine

import "unicode"

type TokenType int

const (
	tEOF TokenType = iota
	tIdent
	tNumber
	tString
	tSymbol
	tKeyword
)

type Token struct {
	Typ TokenType
	Val string
	Pos int
}

type Lexer struct {
	s   string
	pos int
}

func newLexer(s string) *Lexer { return &Lexer{s: s} }

func (lx *Lexer) peek() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	return rune(lx.s[lx.pos])
}
func (lx *Lexer) peekN(n int) rune {
	p := lx.pos + n
	if p >= len(lx.s) {
		return 0
	}
	return rune(lx.s[p])
}
func (lx *Lexer) next() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	r := rune(lx.s[lx.pos])
	lx.pos++
	return r
}
func (lx *Lexer) skipWS() {
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

func (lx *Lexer) Next() Token {
	lx.skipWS()
	start := lx.pos
	if start >= len(lx.s) {
		return Token{Typ: tEOF, Pos: start}
	}
	r := lx.peek()
	// string
	if r == '\'' {
		lx.next()
		val := ""
		for lx.pos < len(lx.s) {
			ch := lx.next()
			if ch == '\'' {
				if lx.peek() == '\'' {
					lx.next()
					val += "'"
					continue
				}
				break
			}
			val += string(ch)
		}
		return Token{Typ: tString, Val: val, Pos: start}
	}
	// number
	if unicode.IsDigit(r) {
		val := ""
		dot := false
		for lx.pos < len(lx.s) {
			ch := lx.peek()
			if unicode.IsDigit(ch) || (!dot && ch == '.') {
				if ch == '.' {
					dot = true
				}
				val += string(ch)
				lx.pos++
			} else {
				break
			}
		}
		return Token{Typ: tNumber, Val: val, Pos: start}
	}
	// ident/keyword
	if unicode.IsLetter(r) || r == '_' {
		val := ""
		for lx.pos < len(lx.s) {
			ch := lx.peek()
			if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
				val += string(ch)
				lx.pos++
			} else {
				break
			}
		}
		up := upper(val)
		if isKeyword(up) {
			return Token{Typ: tKeyword, Val: up, Pos: start}
		}
		return Token{Typ: tIdent, Val: val, Pos: start}
	}
	// symbol
	switch r {
	case '(', ')', ',', '*', '+', '-', '/', '.', ';', '?':
		lx.next()
		return Token{Typ: tSymbol, Val: string(r), Pos: start}
	case '=', '<', '>', '!':
		a := lx.next()
		b := lx.peek()
		if (a == '<' && (b == '=' || b == '>')) || (a == '>' && b == '=') || (a == '!' && b == '=') {
			lx.next()
			return Token{Typ: tSymbol, Val: string(a) + string(b), Pos: start}
		}
		return Token{Typ: tSymbol, Val: string(a), Pos: start}
	default:
		lx.next()
		return Token{Typ: tSymbol, Val: string(r), Pos: start}
	}
}

func upper(s string) string {
	out := make([]rune, len(s))
	for i, r := range s {
		if 'a' <= r && r <= 'z' {
			out[i] = r - 32
		} else {
			out[i] = r
		}
	}
	return string(out)
}

func isKeyword(up string) bool {
	switch up {
	case "SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
		"ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
		"JOIN", "LEFT", "RIGHT", "OUTER", "ON", "AS",
		"CREATE", "TABLE", "TEMP", "DROP",
		"INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE",
		"INT", "FLOAT", "TEXT", "BOOL", "JSON",
		"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE",
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"COALESCE", "NULLIF",
		"JSON_GET":
		return true
	default:
		return false
	}
}
