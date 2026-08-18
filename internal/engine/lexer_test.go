// Direct lexer tests, focused on the ASCII fast paths added to
// peek/next/skipWS/tokenizeIdentOrKeyword/tokenizeSymbol/upperInto: every one
// of them must produce byte-identical results to the general Unicode-aware
// path they short-circuit, for both ASCII and non-ASCII input. The package's
// own header comment on lexer.go documents a past bug in exactly this shape
// ("a byte-wise cast turned 'héllo' into 'hÃ©llo'"), which is why this file
// exercises multi-byte identifiers specifically, not just string-literal
// content (already covered by string_ops_test.go).
package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func lexAll(t *testing.T, sql string) []token {
	t.Helper()
	lx := newLexer(sql)
	var toks []token
	for {
		tok := lx.nextToken()
		if tok.Typ == tEOF {
			return toks
		}
		toks = append(toks, tok)
	}
}

// TestLexerUnicodeIdentifier pins that a multi-byte UTF-8 identifier lexes
// as a single, correctly-decoded token -- not corrupted by the ASCII fast
// path added to tokenizeIdentOrKeyword's scan loop, which must fall back to
// full UTF-8 decoding the moment it sees a non-ASCII byte.
func TestLexerUnicodeIdentifier(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"héllo", "héllo"},
		{"café_table", "café_table"},
		{"日本語", "日本語"},
		{"naïve_col", "naïve_col"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			toks := lexAll(t, tc.sql)
			if len(toks) != 1 {
				t.Fatalf("lexAll(%q) produced %d tokens, want 1: %+v", tc.sql, len(toks), toks)
			}
			if toks[0].Typ != tIdent {
				t.Fatalf("lexAll(%q) token type = %v, want tIdent", tc.sql, toks[0].Typ)
			}
			if toks[0].Val != tc.want {
				t.Fatalf("lexAll(%q) = %q, want %q", tc.sql, toks[0].Val, tc.want)
			}
		})
	}
}

// TestLexerUnicodeIdentifierInQuery exercises the same thing inside a real
// statement, through the parser, end to end.
func TestLexerUnicodeIdentifierInQuery(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE café (id INT, naïve_col TEXT)`)
	execSQL(t, db, `INSERT INTO café VALUES (1, 'héllo')`)
	rs := execSQL(t, db, `SELECT naïve_col FROM café WHERE id = 1`)
	if len(rs.Rows) != 1 || rs.Rows[0]["naïve_col"] != "héllo" {
		t.Fatalf("unicode identifier round-trip failed: %+v", rs.Rows)
	}
}

// TestLexerSymbols pins tokenizeSymbol's rewritten allocation strategy
// (string(byte(r)) / string constants instead of string(rune)) against every
// single- and double-character operator it recognizes, plus the boundary
// cases immediately next to them (e.g. '<' alone vs '<=' vs '<>').
func TestLexerSymbols(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"( ) , * + - / . ; ?", []string{"(", ")", ",", "*", "+", "-", "/", ".", ";", "?"}},
		{"= < > !", []string{"=", "<", ">", "!"}},
		{"<= <> >= !=", []string{"<=", "<>", ">=", "!="}},
		{"<==", []string{"<=", "="}},     // <= then a trailing =
		{"<>=", []string{"<>", "="}},     // <> then a trailing =
		{"! =", []string{"!", "="}},      // ! and = as separate tokens (whitespace between)
		{"a<b", []string{"a", "<", "b"}}, // '<' alone, not swallowed into '<='
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			toks := lexAll(t, tc.sql)
			if len(toks) != len(tc.want) {
				t.Fatalf("lexAll(%q) produced %d tokens, want %d: %+v", tc.sql, len(toks), len(tc.want), toks)
			}
			for i, w := range tc.want {
				if toks[i].Val != w {
					t.Fatalf("lexAll(%q) token %d = %q, want %q (all: %+v)", tc.sql, i, toks[i].Val, w, toks)
				}
			}
		})
	}
}

// TestLexerStringLiteral pins tokenizeString's zero-copy fast path (plain
// literal, no embedded quote) against its strings.Builder-based fallback
// (an embedded '' escape forces the slow path), including a literal that
// contains multi-byte UTF-8 content to confirm the byte-wise quote scan
// doesn't misfire on continuation bytes.
func TestLexerStringLiteral(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{`'plain'`, "plain"},
		{`''`, ""},
		{`'it''s'`, "it's"},
		{`'''quoted'''`, "'quoted'"},
		{`'héllo wörld'`, "héllo wörld"},
		{`'a''b''c'`, "a'b'c"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			toks := lexAll(t, tc.sql)
			if len(toks) != 1 || toks[0].Typ != tString {
				t.Fatalf("lexAll(%q) = %+v, want a single tString token", tc.sql, toks)
			}
			if toks[0].Val != tc.want {
				t.Fatalf("lexAll(%q) = %q, want %q", tc.sql, toks[0].Val, tc.want)
			}
		})
	}
}

// TestLexerQuotedIdentifier mirrors TestLexerStringLiteral for double-quoted
// identifiers and their "" escape.
func TestLexerQuotedIdentifier(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{`"plain"`, "plain"},
		{`"it""s"`, `it"s`},
		{`"café"`, "café"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			toks := lexAll(t, tc.sql)
			if len(toks) != 1 || toks[0].Typ != tIdent {
				t.Fatalf("lexAll(%q) = %+v, want a single tIdent token", tc.sql, toks)
			}
			if toks[0].Val != tc.want {
				t.Fatalf("lexAll(%q) = %q, want %q", tc.sql, toks[0].Val, tc.want)
			}
		})
	}
}

// TestLexerStringLiteralMalformedUTF8 pins a bug an adversarial review found
// in tokenizeString/tokenizeQuotedIdent's zero-copy fast path: the pre-existing
// slow path (strings.Builder + next(), which decodes via utf8.DecodeRuneInString)
// silently normalizes any invalid UTF-8 byte to the U+FFFD replacement
// character, so the lexer always produced valid UTF-8 token values. The first
// version of the fast path bypassed that entirely and returned malformed
// bytes verbatim whenever the literal had no doubled-quote escape (the common
// case). The fix validates the fast-path slice with utf8.ValidString before
// returning it, falling back to the sanitizing slow path otherwise -- these
// cases pin that both paths agree on malformed input.
func TestLexerStringLiteralMalformedUTF8(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"lone continuation byte", "'\x80'", "�"},
		{"invalid lead byte C0", "'\xc0'", "�"},
		{"invalid lead byte FF", "'\xff'", "�"},
		{"embedded mid-string", "'ab\x80cd'", "ab�cd"},
		{"truncated multi-byte sequence", "'a\xe2\x82'", "a��"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			toks := lexAll(t, tc.sql)
			if len(toks) != 1 || toks[0].Typ != tString {
				t.Fatalf("lexAll(%q) = %+v, want a single tString token", tc.sql, toks)
			}
			if toks[0].Val != tc.want {
				t.Fatalf("lexAll(%q) = %q (bytes %x), want %q (bytes %x)", tc.sql, toks[0].Val, toks[0].Val, tc.want, tc.want)
			}
		})
	}
}

// TestLexerQuotedIdentifierMalformedUTF8 mirrors
// TestLexerStringLiteralMalformedUTF8 for double-quoted identifiers.
func TestLexerQuotedIdentifierMalformedUTF8(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"lone continuation byte", "\"\x80\"", "�"},
		{"invalid lead byte FD", "\"\xfd\"", "�"},
		{"embedded mid-identifier", "\"a\xe9llo\"", "a�llo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			toks := lexAll(t, tc.sql)
			if len(toks) != 1 || toks[0].Typ != tIdent {
				t.Fatalf("lexAll(%q) = %+v, want a single tIdent token", tc.sql, toks)
			}
			if toks[0].Val != tc.want {
				t.Fatalf("lexAll(%q) = %q (bytes %x), want %q (bytes %x)", tc.sql, toks[0].Val, toks[0].Val, tc.want, tc.want)
			}
		})
	}
}

// TestLexerWhitespaceAndComments pins skipWS's ASCII fast path against every
// whitespace character unicode.IsSpace recognizes in the ASCII range, plus
// both comment forms, in combination.
func TestLexerWhitespaceAndComments(t *testing.T) {
	sql := "SELECT\t1,\n2,\r3 -- trailing comment\n, /* block\ncomment */ 4"
	toks := lexAll(t, sql)
	want := []string{"SELECT", "1", ",", "2", ",", "3", ",", "4"}
	if len(toks) != len(want) {
		t.Fatalf("lexAll produced %d tokens, want %d: %+v", len(toks), len(want), toks)
	}
	for i, w := range want {
		if toks[i].Val != w {
			t.Fatalf("token %d = %q, want %q (all: %+v)", i, toks[i].Val, w, toks)
		}
	}
}

// TestLexerKeywordCaseInsensitivity pins upperInto's scratch-buffer reuse
// across MANY tokens in one lexer -- the whole point of the optimization --
// by lexing many differently-cased keywords and identifiers back to back
// and checking each is classified correctly, including identifiers longer
// than an earlier keyword (forcing the scratch buffer to grow mid-statement)
// and shorter ones after (checking the grown buffer's stale tail bytes don't
// leak into a shorter token's result).
func TestLexerKeywordCaseInsensitivity(t *testing.T) {
	sql := "select Select SELECT sElEcT from short_er_identifier_name_that_is_quite_long a select"
	toks := lexAll(t, sql)
	wantTypes := []tokenType{tKeyword, tKeyword, tKeyword, tKeyword, tKeyword, tIdent, tIdent, tKeyword}
	wantVals := []string{"SELECT", "SELECT", "SELECT", "SELECT", "FROM", "short_er_identifier_name_that_is_quite_long", "a", "SELECT"}
	if len(toks) != len(wantTypes) {
		t.Fatalf("produced %d tokens, want %d: %+v", len(toks), len(wantTypes), toks)
	}
	for i := range wantTypes {
		if toks[i].Typ != wantTypes[i] || toks[i].Val != wantVals[i] {
			t.Fatalf("token %d = {%v %q}, want {%v %q}", i, toks[i].Typ, toks[i].Val, wantTypes[i], wantVals[i])
		}
	}
}
