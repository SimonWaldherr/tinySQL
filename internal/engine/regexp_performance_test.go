package engine

import (
	"context"
	"regexp"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkRegexpMatchFunctionScan(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE REGEXP_MATCH(note, 'number [0-9]{3}0')`)
}

func BenchmarkRegexpProjection(b *testing.B) {
	db := setupPerfTable(b, 20_000)
	b.Cleanup(func() { _ = db.Close() })
	for _, tc := range []struct{ name, expr string }{
		{"MatchClass", `REGEXP_MATCH(note, 'number [0-9]{3}0')`},
		{"ExtractLiteral", `REGEXP_EXTRACT(note, 'lorem')`},
		{"ExtractClass", `REGEXP_EXTRACT(note, '[0-9]+')`},
		{"ExtractFixed", `REGEXP_EXTRACT(note, 'number [0-9]{3}0')`},
		{"ReplaceLiteral", `REGEXP_REPLACE(note, 'lorem', 'word')`},
		{"ReplaceFixed", `REGEXP_REPLACE(note, 'number [0-9]{3}0', 'number #')`},
		{"ReplaceCaptures", `REGEXP_REPLACE(note, 'number ([0-9]+)', 'id=$1')`},
		{"ReplaceAbsent", `REGEXP_REPLACE(note, 'missing [0-9]+', 'word')`},
	} {
		b.Run(tc.name, func(b *testing.B) { runBench(b, db, "SELECT "+tc.expr+" FROM t") })
	}
}

func TestRegexpMatchRawFilter(t *testing.T) {
	for _, pattern := range []string{"", "abc", "^a.*z$", "(?i)ä", "[0-9]+", "a|bc", "�"} {
		re := regexp.MustCompile(pattern)
		filter := buildRawFilterRegexpMatch(map[string]int{"v": 0}, &FuncCall{Name: "REGEXP_MATCH", Args: []Expr{&VarRef{Name: "v"}, &Literal{Val: pattern}}})
		if filter == nil {
			t.Fatalf("no filter for %q", pattern)
		}
		for _, value := range []any{nil, "", "abc", "abz", "Ä", "x42", "\xff", 123, []byte("abc")} {
			got, err := filter([]any{value})
			want := value != nil && re.MatchString(valueText(value))
			if err != nil || got != want {
				t.Fatalf("pattern=%q value=%#v got=%v err=%v want=%v", pattern, value, got, err, want)
			}
		}
	}
	for _, literal := range []*Literal{{Val: "["}, {Val: "abc", Parameter: true}, {Val: nil}} {
		if buildRawFilterRegexpMatch(map[string]int{"v": 0}, &FuncCall{Name: "REGEXP_MATCH", Args: []Expr{&VarRef{Name: "v"}, literal}}) != nil {
			t.Fatalf("must fall back for %#v", literal)
		}
	}
	if buildRawFilterRegexp(map[string]int{"v": 0}, &RegexpExpr{Expr: &VarRef{Name: "v"}, Pattern: &Literal{Val: "a", Parameter: true}}) != nil {
		t.Fatal("bound infix pattern must remain dynamic")
	}
}

func TestRegexpMatchFilterSQL(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE regex_data (v TEXT)`)
	execSQL(t, db, `INSERT INTO regex_data VALUES ('abc'), ('xyz'), (NULL)`)
	for query, want := range map[string]int{
		`SELECT v FROM regex_data WHERE REGEXP_MATCH(v, '^a')`:     1,
		`SELECT v FROM regex_data WHERE NOT REGEXP_MATCH(v, '^a')`: 2,
		`SELECT v FROM regex_data WHERE REGEXP_MATCH(v, '')`:       2,
	} {
		if got := len(execSQL(t, db, query).Rows); got != want {
			t.Fatalf("%s: got %d want %d", query, got, want)
		}
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`SELECT v FROM regex_data WHERE REGEXP_MATCH(v, '[')`)); err == nil {
		t.Fatal("invalid pattern must fail")
	}
	execSQL(t, db, `CREATE TABLE regex_null (v TEXT)`)
	execSQL(t, db, `INSERT INTO regex_null VALUES (NULL)`)
	if got := len(execSQL(t, db, `SELECT v FROM regex_null WHERE REGEXP_MATCH(v, '[')`).Rows); got != 0 {
		t.Fatal("NULL input must short circuit invalid pattern")
	}
}

// Compare identical predicates without query planning or result materialization.
func BenchmarkRegexpMatchFilter(b *testing.B) {
	cols := map[string]int{"v": 0}
	expr := &FuncCall{Name: "REGEXP_MATCH", Args: []Expr{&VarRef{Name: "v"}, &Literal{Val: "number [0-9]{3}0"}}}
	rows := [][]any{{"note number 1230 lorem ipsum"}, {"note number 1231 lorem ipsum"}, {nil}}
	for _, specialized := range []bool{false, true} {
		name := "generic"
		filter := buildRawExprFilter(cols, expr)
		if specialized {
			name = "bound"
			filter = buildRawFilterRegexpMatch(cols, expr)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				got, err := filter(rows[i%len(rows)])
				if err != nil || got != (i%len(rows) == 0) {
					b.Fatal(got, err)
				}
			}
		})
	}
}
