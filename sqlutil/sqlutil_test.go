package sqlutil

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func TestAnalyzeClassifiesCommonStatements(t *testing.T) {
	tests := []struct {
		sql  string
		kind StatementKind
		ro   bool
		rp   bool
	}{
		{"SELECT * FROM users", KindSelect, true, true},
		{"EXPLAIN SELECT * FROM users", KindExplain, true, true},
		{"INSERT INTO users VALUES (1)", KindInsert, false, false},
		{"CREATE VIEW v AS SELECT 1", KindCreateView, false, false},
	}
	for _, tc := range tests {
		got, err := Analyze(tc.sql)
		if err != nil {
			t.Fatalf("Analyze(%q): %v", tc.sql, err)
		}
		if got.Kind != tc.kind || got.ReadOnly != tc.ro || got.ResultProducing != tc.rp {
			t.Fatalf("Analyze(%q) = %#v", tc.sql, got)
		}
	}
}

func TestIsResultProducingHasDialectFallback(t *testing.T) {
	if !IsResultProducing("SHOW TABLES") {
		t.Fatal("SHOW TABLES should be treated as result-producing for external dialects")
	}
}

func TestAnalyzeReturningAndExplainAnalyze(t *testing.T) {
	for _, tc := range []struct {
		query                      string
		kind                       StatementKind
		readOnly, mutation, result bool
	}{
		{"INSERT INTO users VALUES (1) RETURNING *", KindInsert, false, true, true},
		{"UPDATE users SET id = 2 RETURNING id", KindUpdate, false, true, true},
		{"DELETE FROM users RETURNING id", KindDelete, false, true, true},
		{"EXPLAIN DELETE FROM users", KindExplain, true, false, true},
		{"EXPLAIN ANALYZE DELETE FROM users", KindExplain, false, true, true},
		{"EXPLAIN ANALYZE SELECT 1", KindExplain, true, false, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, err := Analyze(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != tc.kind || got.ReadOnly != tc.readOnly || got.Mutation != tc.mutation || got.ResultProducing != tc.result {
				t.Fatalf("Analyze = %#v", got)
			}
			if IsResultProducing(tc.query) != tc.result || IsReadOnly(tc.query) != tc.readOnly {
				t.Fatal("helper classification differs from Analyze")
			}
		})
	}
}

func TestAnalyzeRejectsMultipleStatements(t *testing.T) {
	for _, query := range []string{"SELECT 1; DELETE FROM users", "SELECT ';'; UPDATE users SET id = 2", "SELECT 1; -- comment\rDELETE FROM users"} {
		if result, err := Analyze(query); err == nil || result.Kind != KindUnknown {
			t.Errorf("Analyze(%q) = %#v, %v; want multiple statement error", query, result, err)
		}
		if IsReadOnly(query) || IsResultProducing(query) {
			t.Errorf("multiple statements classified as a single statement: %q", query)
		}
	}
	for _, query := range []string{"SELECT ';'", "SELECT 1; -- harmless trailing comment"} {
		if !IsReadOnly(query) || !IsResultProducing(query) {
			t.Errorf("single statement rejected: %q", query)
		}
	}
}

func TestAnalysisHelpersCoverMutationsAndUnknownStatements(t *testing.T) {
	for _, tc := range []struct {
		stmt engine.Statement
		kind StatementKind
		job  bool
	}{
		{&engine.Update{Table: "users"}, KindUpdate, false},
		{&engine.Delete{Table: "users"}, KindDelete, false},
		{&engine.CreateIndex{Name: "users_id", Table: "users"}, KindCreateIndex, false},
		{&engine.DropIndex{Name: "users_id", Table: "users"}, KindDropIndex, false},
		{&engine.CreateJob{Name: "nightly"}, KindCreateJob, true},
		{nil, KindUnknown, false},
	} {
		got := AnalyzeStatement(tc.stmt)
		if got.Kind != tc.kind || got.Job != tc.job {
			t.Fatalf("AnalyzeStatement(%T) = %#v", tc.stmt, got)
		}
	}
	if !IsReadOnly("SELECT 1") || IsReadOnly("UPDATE users SET name = 'Ada'") {
		t.Fatal("IsReadOnly classification is incorrect")
	}
	if IsResultProducing("UPDATE users SET name = 'Ada'") || IsResultProducing("not valid SQL") {
		t.Fatal("IsResultProducing classification is incorrect")
	}
}
