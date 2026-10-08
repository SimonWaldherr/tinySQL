package engine

import (
	"strings"
	"testing"
)

func TestLexerLongCandidatesPreserveKeywordValues(t *testing.T) {
	short := strings.Repeat("a", 40)
	long := strings.Repeat("B", 80) + "c"
	tokens := lexAll(t, "select "+short+" fRoM "+long+" where "+short+" aNd TrUe")
	want := []token{
		{Typ: tKeyword, Val: "SELECT"},
		{Typ: tIdent, Val: short},
		{Typ: tKeyword, Val: "FROM"},
		{Typ: tIdent, Val: long},
		{Typ: tKeyword, Val: "WHERE"},
		{Typ: tIdent, Val: short},
		{Typ: tKeyword, Val: "AND"},
		{Typ: tKeyword, Val: "TRUE"},
	}
	if len(tokens) != len(want) {
		t.Fatalf("got %d tokens, want %d", len(tokens), len(want))
	}
	for i, tok := range tokens {
		if tok.Typ != want[i].Typ || tok.Val != want[i].Val {
			t.Errorf("token %d = {%v %q}, want {%v %q}", i, tok.Typ, tok.Val, want[i].Typ, want[i].Val)
		}
	}
}

func TestParseInsertValueRowsWithDifferentWidths(t *testing.T) {
	// Parsing retains each row independently even when its width differs from
	// the preceding row. Execution performs the column-count validation.
	parsed, err := NewParser("INSERT INTO events VALUES (1, 2, 3), (4), (5, 6, 7, 8), (9, 10)").ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	rows := parsed.(*Insert).Rows
	want := [][]int{{1, 2, 3}, {4}, {5, 6, 7, 8}, {9, 10}}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for i, row := range rows {
		if len(row) != len(want[i]) {
			t.Fatalf("row %d has %d values, want %d", i, len(row), len(want[i]))
		}
		for j, value := range row {
			if got := value.(*Literal).Val; got != want[i][j] {
				t.Errorf("row %d column %d = %v, want %d", i, j, got, want[i][j])
			}
		}
	}
}

func BenchmarkLexerKeywordCasing(b *testing.B) {
	sql := "SELECT account_identifier, user_identifier, organization_identifier FROM customer_accounts WHERE account_identifier = 42 AND enabled = TRUE"
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"Mixed", sql},
		{"Upper", strings.ToUpper(sql)},
		{"Lower", strings.ToLower(sql)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				lx := newLexer(tc.sql)
				for lx.nextToken().Typ != tEOF {
				}
			}
		})
	}
}
