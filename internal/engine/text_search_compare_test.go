package engine

import (
	"reflect"
	"testing"
)

type textSearchVariant struct{ name, predicate string }

var textSearchComparisons = []struct {
	name     string
	variants []textSearchVariant
}{
	{"contains", []textSearchVariant{
		{"like", `body LIKE '%timeout%'`},
		{"regexp", `body REGEXP 'timeout'`},
		{"contains", `CONTAINS(body, 'timeout')`},
		{"instr", `INSTR(body, 'timeout') > 0`},
		{"position", `POSITION('timeout' IN body) > 0`},
		{"locate", `LOCATE('timeout', body) > 0`},
		{"glob", `body GLOB '*timeout*'`},
	}},
	{"prefix", []textSearchVariant{
		{"like", `body LIKE 'ERROR\_%'`},
		{"starts_with", `STARTS_WITH(body, 'ERROR_')`},
		{"instr", `INSTR(body, 'ERROR_') = 1`},
		{"left", `LEFT(body, 6) = 'ERROR_'`},
		{"glob", `body GLOB 'ERROR_*'`},
	}},
	{"suffix", []textSearchVariant{
		{"like", `body LIKE '%timeout'`},
		{"ends_with", `ENDS_WITH(body, 'timeout')`},
		{"right", `RIGHT(body, 7) = 'timeout'`},
		{"glob", `body GLOB '*timeout'`},
	}},
	// CONTAINS_ANY/ALL deliberately ignore case, unlike plain CONTAINS.
	{"any_insensitive", []textSearchVariant{
		{"ilike", `body ILIKE '%error%' OR body ILIKE '%timeout%'`},
		{"contains_any", `CONTAINS_ANY(body, 'error', 'timeout')`},
	}},
	{"all_insensitive", []textSearchVariant{
		{"ilike", `body ILIKE '%error%' AND body ILIKE '%timeout%'`},
		{"contains_all", `CONTAINS_ALL(body, 'error', 'timeout')`},
	}},
}

// These equivalences apply to positive WHERE filters over TEXT/NULL values.
// They do not imply identical scalar NULL results or negation semantics.
func TestTextSearchAlternativeFilters(t *testing.T) {
	db := regexpComparisonDB(t, 1000)
	table, err := db.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"", "ERROR_", "timeout", "ERROR_timeout", "error_TIMEOUT", "ERROR_é🙂\ntimeout", "ERROR_timeout\n", "ERROR_timeout detail", "ERROR_\xfftimeout", "K漢timeout", "time_out", "%timeout%"} {
		table.Rows = append(table.Rows, []any{len(table.Rows), body})
	}
	table.Version++
	for _, tc := range textSearchComparisons {
		want := execSQL(t, db, `SELECT id FROM events WHERE `+tc.variants[0].predicate)
		for _, variant := range tc.variants[1:] {
			got := execSQL(t, db, `SELECT id FROM events WHERE `+variant.predicate)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s/%s selects different rows", tc.name, variant.name)
			}
		}
	}
}

func BenchmarkTextSearchAlternatives(b *testing.B) {
	db := regexpComparisonDB(b, 50000)
	for _, tc := range textSearchComparisons {
		want, err := Execute(b.Context(), db, "default", mustParse(`SELECT COUNT(*) AS hits FROM events WHERE `+tc.variants[0].predicate))
		if err != nil {
			b.Fatal(err)
		}
		for _, variant := range tc.variants {
			b.Run(tc.name+"/"+variant.name, func(b *testing.B) {
				stmt := mustParse(`SELECT COUNT(*) AS hits FROM events WHERE ` + variant.predicate)
				got, err := Execute(b.Context(), db, "default", stmt)
				if err != nil || !reflect.DeepEqual(got, want) {
					b.Fatalf("comparison mismatch: %+v, want %+v, err=%v", got, want, err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
