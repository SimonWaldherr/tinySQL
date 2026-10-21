package engine

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkLexerQuotedValues(b *testing.B) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"Short", `'payload text here'`},
		{"Plain", `'` + strings.Repeat("payload text here ", 64) + `'`},
		{"Escaped", `'` + strings.Repeat("customer''s payload text here ", 64) + `'`},
		{"Unicode", `'` + strings.Repeat("Grüße aus 日本 '' Grüße ", 64) + `'`},
		{"QuotedIdentifier", `"` + strings.Repeat(`column""name_`, 32) + `"`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.sql)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				lx := newLexer(tc.sql)
				if tok := lx.nextToken(); tok.Val == "" || lx.pos != len(tc.sql) {
					b.Fatal(tok, lx.pos)
				}
			}
		})
	}
}

func BenchmarkParseEscapedInsert(b *testing.B) {
	query := "INSERT INTO events (id, payload) VALUES (42, '" +
		strings.Repeat("customer''s payload text here ", 64) + "')"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := NewParser(query).ParseStatement(); err != nil {
			b.Fatal(err)
		}
	}
}

// This reference decodes one rune at a time, preserving the lexer's handling
// of malformed UTF-8 and a missing closing quote.
func quotedTokenReference(s string, start int, quote byte, typ tokenType) (token, int) {
	pos := start + 1
	var value strings.Builder
	for pos < len(s) {
		r, width := utf8.DecodeRuneInString(s[pos:])
		pos += width
		if r == rune(quote) {
			if pos < len(s) && s[pos] == quote {
				pos++
			} else {
				break
			}
		}
		value.WriteRune(r)
	}
	return token{Typ: typ, Val: value.String(), Pos: start}, pos
}

func TestLexerQuotedTokenBoundaries(t *testing.T) {
	for _, quote := range []byte{'\'', '"'} {
		for _, content := range []string{
			"", "plain", "héllo 日本語", "\x00", "\x80\xff", "a\xe2\x82",
			string(quote), strings.Repeat(string(quote), 2), strings.Repeat(string(quote), 3),
			"a" + string(quote) + "\x80" + string(quote) + "b",
		} {
			for _, suffix := range []string{"", string(quote), string(quote) + ", next"} {
				sql := "  " + string(quote) + content + suffix
				typ := tString
				if quote == '"' {
					typ = tIdent
				}
				want, end := quotedTokenReference(sql, 2, quote, typ)
				lx := newLexer(sql)
				if got := lx.nextToken(); got != want || lx.pos != end {
					t.Fatalf("SQL %q: got %+v at %d, want %+v at %d", sql, got, lx.pos, want, end)
				}
			}
		}
	}
}

func TestLexerQuotedValuesSurviveLaterTokens(t *testing.T) {
	lx := newLexer(`'Grüße ''日本''' "customer""name" 'another''value'`)
	first, second := lx.nextToken(), lx.nextToken()
	for lx.nextToken().Typ != tEOF {
	}
	if first.Val != "Grüße '日本'" || second.Val != `customer"name` {
		t.Fatalf("later tokens changed retained values: %+v, %+v", first, second)
	}
}

func TestLexerQuotedSQLRoundTrip(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE quoted_values ("user""名" TEXT, tail TEXT)`)
	execSQL(t, db, `INSERT INTO quoted_values VALUES ('Grüße ''日本''', 'next')`)
	rs := execSQL(t, db, `SELECT "user""名" AS value, tail FROM quoted_values`)
	if len(rs.Rows) != 1 || rs.Rows[0]["value"] != "Grüße '日本'" || rs.Rows[0]["tail"] != "next" {
		t.Fatalf("quoted values failed to round trip: %+v", rs.Rows)
	}
}

func FuzzLexerQuotedToken(f *testing.F) {
	for _, s := range []string{"", "it's", "a''b", `a""b`, "日本語", "\x80\xff", "\xe2'\x82", "\x00'foo"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, content string) {
		for _, quote := range []byte{'\'', '"'} {
			typ := tString
			if quote == '"' {
				typ = tIdent
			}
			sql := string(quote) + content
			want, end := quotedTokenReference(sql, 0, quote, typ)
			lx := newLexer(sql)
			if got := lx.nextToken(); got != want || lx.pos != end {
				t.Fatalf("SQL %q: got %+v at %d, want %+v at %d", sql, got, lx.pos, want, end)
			}
		}
	})
}
