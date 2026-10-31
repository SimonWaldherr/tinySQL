package engine

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Execute the reference's independent semantics examples against explicit
// expected results. Later sections intentionally include FILE/HTTP examples.
func TestFunctionReferenceSemanticsExamples(t *testing.T) {
	source, err := os.ReadFile("../../docs/reference/functions.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(source), "-- CHECKED SEMANTICS EXAMPLES")
	if !ok {
		t.Fatal("checked reference section missing")
	}
	section, _, ok = strings.Cut(section, "-- END CHECKED SEMANTICS EXAMPLES")
	if !ok {
		t.Fatal("checked reference section is unterminated")
	}
	want := [][][]any{
		{{5, 7}}, {{1, 0}}, {{0, 23}}, {{true, false, true, true}}, {{"ok", false}},
		{{"text", " text ", nil}}, {{"alpha", 1}, {"beta", 2}, {"", 3}, {"", 4}},
		{{2, 2}}, {{"A", nil}}, {{"9007199254740993"}}, {{float64(1), "Ada"}, {float64(2), "Lin"}},
	}
	queries := SplitStatements(section)
	if len(queries) != len(want) {
		t.Fatalf("%d reference statements, want %d", len(queries), len(want))
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for i, query := range queries {
		rs := execSQL(t, db, query)
		actual := make([][]any, len(rs.Rows))
		for r, row := range rs.Rows {
			for _, col := range rs.Cols {
				actual[r] = append(actual[r], row[col])
			}
		}
		if !reflect.DeepEqual(actual, want[i]) {
			t.Errorf("example %d: got %#v, want %#v\n%s", i+1, actual, want[i], query)
		}
	}
}

func TestFunctionReferenceStatementsParse(t *testing.T) {
	source, err := os.ReadFile("../../docs/reference/functions.sql")
	if err != nil {
		t.Fatal(err)
	}
	queries := SplitStatements(string(source))
	if len(queries) == 0 {
		t.Fatal("empty function reference")
	}
	for i, query := range queries {
		if _, err := NewParser(query).ParseStatement(); err != nil {
			t.Errorf("reference statement %d: %v\n%s", i+1, err, query)
		}
	}
}

func TestFunctionReferenceJSONColumnExamples(t *testing.T) {
	source, err := os.ReadFile("../../docs/reference/functions.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(source), "-- JSON FUNCTIONS\n")
	if !ok {
		t.Fatal("JSON reference section missing")
	}
	_, section, ok = strings.Cut(section, "\n\n") // Skip the closing heading rule.
	if !ok {
		t.Fatal("JSON reference section has no body")
	}
	section, _, _ = strings.Cut(section, "-- ============================================================")
	queries := SplitStatements(section)
	if len(queries) != 6 {
		t.Fatalf("got %d JSON reference statements, want 6", len(queries))
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for i, query := range queries {
		rs := execSQL(t, db, query)
		if i == 2 && !reflect.DeepEqual(rs.Rows, []Row{{"json_name": "John", "nested_value": "Jane"}}) {
			t.Errorf("JSON extraction examples: %v", rs.Rows)
		}
		if i == 4 && !reflect.DeepEqual(rs.Rows, []Row{{"updated_sku": "B"}}) {
			t.Errorf("JSON update example: %v", rs.Rows)
		}
	}
}
