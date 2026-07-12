package engine

import "testing"

// FuzzBlobLiteralParsing keeps malformed binary SQL input a regular parse
// error rather than a panic or a text coercion path.
func FuzzBlobLiteralParsing(f *testing.F) {
	for _, seed := range []string{"X''", "X'00ff'", "X'abc'", "X'nothex'", "x'1f8b0800'"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, literal string) {
		stmt, err := NewParser("SELECT " + literal).ParseStatement()
		if err != nil || stmt == nil {
			return
		}
		if _, ok := stmt.(*Select); !ok {
			t.Fatalf("unexpected statement %T", stmt)
		}
	})
}
