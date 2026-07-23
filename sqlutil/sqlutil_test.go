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
