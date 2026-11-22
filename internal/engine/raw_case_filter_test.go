package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawCaseComparisonMatchesInterpreter(t *testing.T) {
	values := []any{nil, "", "abc", "AbC", "ABC", "İ", "ı", "K", "ſ", "Σ", "σ", "ς", "é", "É", "x\xffz", "x\ufffdz", 123, false, []byte("AbC")}
	targets := []string{"", "abc", "ABC", "AbC", "i", "I", "k", "K", "s", "S", "σ", "ς", "Σ", "é", "É", "x\xffz", "x\ufffdz", "X\ufffdZ", "123", "false", "FALSE"}
	column := &VarRef{Name: "text"}
	columns := map[string]int{"text": 0}
	for _, name := range []string{"LOWER", "UPPER"} {
		for _, op := range []string{"=", "!=", "<>"} {
			for _, target := range targets {
				for _, reversed := range []bool{false, true} {
					var left Expr = &FuncCall{Name: name, Args: []Expr{column}}
					var right Expr = &Literal{Val: target}
					if reversed {
						left, right = right, left
					}
					expr := &Binary{Op: op, Left: left, Right: right}
					filter := buildRawFilter(columns, expr)
					for _, value := range values {
						got, err := filter([]any{value})
						want, evalErr := evalExpr(ExecEnv{}, expr, Row{"text": value})
						if err != nil || evalErr != nil || got != (toTri(want) == tvTrue) {
							t.Fatalf("%s(%q) %s %q (reversed=%t): got %t, %v; want %v, %v", name, value, op, target, reversed, got, err, want, evalErr)
						}
					}
				}
			}
		}
	}
}

func BenchmarkCaseComparisonFilter(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("entries", []storage.Column{{Name: "note", Type: storage.TextType}}, false)
	for i := range 20_000 {
		table.Rows = append(table.Rows, []any{fmt.Sprintf("Note number %d at branch", i)})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, predicate string }{
		{"lower", "LOWER(note) = 'note number 12001 at branch'"},
		{"upper", "'NOTE NUMBER 12001 AT BRANCH' = UPPER(note)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runBench(b, db, "SELECT COUNT(*) AS n FROM entries WHERE "+tc.predicate)
		})
	}
}
