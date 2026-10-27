package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestSplitStatementsKeepsQuotedAndCommentSemicolons(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "string literals",
			sql:  "INSERT INTO notes VALUES ('a; b'); SELECT 'it''s; fine';",
			want: []string{"INSERT INTO notes VALUES ('a; b')", "SELECT 'it''s; fine'"},
		},
		{
			name: "quoted identifiers",
			sql:  "SELECT \"cost; center\" FROM \"sales; 2026\"; SELECT 2",
			want: []string{"SELECT \"cost; center\" FROM \"sales; 2026\"", "SELECT 2"},
		},
		{
			name: "comments",
			sql:  "-- setup; still a comment\nCREATE TABLE demo (id INT); /* separator; in comment */ INSERT INTO demo VALUES (1);",
			want: []string{"-- setup; still a comment\nCREATE TABLE demo (id INT)", "/* separator; in comment */ INSERT INTO demo VALUES (1)"},
		},
		{
			name: "empty and unicode segments",
			sql:  " ;\n SELECT 'München;東京' ;;\r SELECT \"a\"\";b\";   ",
			want: []string{"SELECT 'München;東京'", "SELECT \"a\"\";b\""},
		},
		{
			name: "unterminated comment is retained for diagnosis",
			sql:  "SELECT 1; /* open; comment",
			want: []string{"SELECT 1", "/* open; comment"},
		},
		{
			name: "carriage return ends line comment",
			sql:  "-- note; comment\rSELECT 'x';SELECT 'unterminated;",
			want: []string{"-- note; comment\rSELECT 'x'", "SELECT 'unterminated;"},
		},
		{
			name: "empty script",
			sql:  " \t;;\n",
			want: nil,
		},
		{
			name: "comments between statements and trailing comments",
			sql:  "SELECT 1; /* only */ ; -- first\n /* hint */ SELECT 'München'; /* last */ -- end",
			want: []string{"SELECT 1", "-- first\n /* hint */ SELECT 'München'"},
		},
		{
			name: "comments only with unicode whitespace",
			sql:  "\u2003/* first */\n-- second\n; /* closed */",
			want: nil,
		},
		{
			name: "unterminated comment after closed comments",
			sql:  "SELECT 1; /* closed */ -- note\n /* open",
			want: []string{"SELECT 1", "/* closed */ -- note\n /* open"},
		},
		{
			name: "trigger body, nested CASE and following statements",
			sql:  "-- trigger\nCREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE c SET n = CASE WHEN NEW.id > 0 THEN 1 ELSE 0 END; INSERT INTO log VALUES ('BEGIN; END'); /* END; */ END; SELECT 1; SELECT 2",
			want: []string{"-- trigger\nCREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE c SET n = CASE WHEN NEW.id > 0 THEN 1 ELSE 0 END; INSERT INTO log VALUES ('BEGIN; END'); /* END; */ END", "SELECT 1", "SELECT 2"},
		},
		{
			name: "ordinary transaction is not a trigger",
			sql:  "BEGIN; SELECT 1; COMMIT;",
			want: []string{"BEGIN", "SELECT 1", "COMMIT"},
		},
		{
			name: "invalid trigger prefix does not hide separators",
			sql:  "CREATE OR ALTER TRIGGER t BEGIN SELECT 1; SELECT 2;",
			want: []string{"CREATE OR ALTER TRIGGER t BEGIN SELECT 1", "SELECT 2"},
		},
		{
			name: "create view with CASE is not a trigger",
			sql:  "CREATE OR REPLACE VIEW v AS SELECT CASE WHEN TRUE THEN 1 ELSE 0 END; SELECT 2",
			want: []string{"CREATE OR REPLACE VIEW v AS SELECT CASE WHEN TRUE THEN 1 ELSE 0 END", "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitStatements(tt.sql); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitStatements() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func BenchmarkSplitStatements(b *testing.B) {
	// A realistic multiline script with quotes and comments; parsing does not
	// execute the SQL. Vary statement count as short editor queries are common.
	for _, tc := range []struct {
		name  string
		count int
	}{{"single", 1}, {"fifty", 50}} {
		script := strings.Repeat("-- semicolon; in a comment\nINSERT INTO notes VALUES ('hello; world', 'it''s fine');\n", tc.count)
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(script)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				splitStatements(script)
			}
		})
	}
}
