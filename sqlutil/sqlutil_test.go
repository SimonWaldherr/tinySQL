package sqlutil

import "testing"

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
