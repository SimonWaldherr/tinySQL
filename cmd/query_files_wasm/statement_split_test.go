package main

import (
	"reflect"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitStatements(tt.sql); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitStatements() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
