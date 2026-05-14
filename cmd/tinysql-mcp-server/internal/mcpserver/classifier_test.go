package mcpserver

import (
	"testing"
)

// TestClassifySQL verifies the statement classifier against a comprehensive set
// of SQL snippets, including edge cases for comment injection and multi-statement
// inputs.
func TestClassifySQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want stmtKind
	}{
		// ── reads ────────────────────────────────────────────────────────
		{"bare select", "SELECT * FROM users", kindSelect},
		{"select with whitespace", "  \t SELECT 1 ", kindSelect},
		{"select uppercase", "SELECT id FROM t WHERE id = 1", kindSelect},
		{"select lowercase", "select * from t", kindSelect},
		{"cte select", "WITH cte AS (SELECT 1) SELECT * FROM cte", kindSelect},

		// ── mutations ─────────────────────────────────────────────────────
		{"insert", "INSERT INTO t VALUES (1)", kindInsert},
		{"update", "UPDATE t SET x = 1 WHERE id = 2", kindUpdate},
		{"delete", "DELETE FROM t WHERE id = 1", kindDelete},

		// ── DDL ───────────────────────────────────────────────────────────
		{"create table", "CREATE TABLE foo (id INT)", kindCreateTable},
		{"create table case", "create table Bar (x TEXT)", kindCreateTable},
		{"create view", "CREATE VIEW v AS SELECT 1", kindCreate},
		{"create index", "CREATE INDEX i ON t(col)", kindCreate},
		{"drop table", "DROP TABLE foo", kindDrop},
		{"drop view", "DROP VIEW foo", kindDrop},
		{"alter table", "ALTER TABLE t ADD COLUMN x INT", kindAlter},

		// ── comment stripping ─────────────────────────────────────────────
		{"leading line comment then select",
			"-- this is a comment\nSELECT 1", kindSelect},
		{"block comment before select",
			"/* comment */ SELECT * FROM t", kindSelect},
		{"block comment before insert",
			"/* evil */ INSERT INTO t VALUES(1)", kindInsert},
		{"nested comment with keyword",
			"/* SELECT */ INSERT INTO t VALUES(1)", kindInsert},
		{"line comment with insert then select",
			"-- INSERT INTO t VALUES(1)\nSELECT 1", kindSelect},
		{"block comment with drop keyword",
			"/* DROP TABLE t; */ SELECT 1", kindSelect},

		// ── multi-statement rejection ─────────────────────────────────────
		{"two statements", "SELECT 1; SELECT 2", kindUnknown},
		{"insert then select", "INSERT INTO t VALUES(1); SELECT * FROM t", kindUnknown},
		{"trailing semicolon only", "SELECT 1;", kindSelect}, // trailing ; is fine
		{"embedded semicolon attack", "SELECT 1; DROP TABLE t", kindUnknown},

		// ── edge cases ────────────────────────────────────────────────────
		{"empty string", "", kindUnknown},
		{"only whitespace", "   \t  ", kindUnknown},
		{"only comment", "-- just a comment", kindUnknown},
		{"only block comment", "/* nothing */", kindUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifySQL(tc.sql)
			if got != tc.want {
				t.Errorf("classifySQL(%q) = %v (%d), want %v (%d)",
					tc.sql, describeKind(got), got, describeKind(tc.want), tc.want)
			}
		})
	}
}

// TestIsReadOnly verifies the read-only predicate.
func TestIsReadOnly(t *testing.T) {
	if !kindSelect.isReadOnly() {
		t.Error("kindSelect should be read-only")
	}
	for _, k := range []stmtKind{kindInsert, kindUpdate, kindDelete, kindCreate, kindCreateTable, kindDrop, kindAlter} {
		if k.isReadOnly() {
			t.Errorf("%v should not be read-only", describeKind(k))
		}
	}
}

// TestIsMutating verifies the mutation predicate.
func TestIsMutating(t *testing.T) {
	for _, k := range []stmtKind{kindInsert, kindUpdate, kindDelete, kindCreate, kindCreateTable, kindDrop, kindAlter} {
		if !k.isMutating() {
			t.Errorf("%v should be mutating", describeKind(k))
		}
	}
	for _, k := range []stmtKind{kindSelect, kindOther, kindUnknown} {
		if k.isMutating() {
			t.Errorf("%v should not be mutating", describeKind(k))
		}
	}
}

// TestIsCreateTable verifies the CREATE TABLE predicate.
func TestIsCreateTable(t *testing.T) {
	if !kindCreateTable.isCreateTable() {
		t.Error("kindCreateTable.isCreateTable() should be true")
	}
	if kindCreate.isCreateTable() {
		t.Error("kindCreate.isCreateTable() should be false (CREATE VIEW etc.)")
	}
}

// TestStripComments verifies comment removal.
func TestStripComments(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // we only check that the SQL keyword survives
	}{
		{"line comment removed", "-- DROP TABLE t\nSELECT 1", "SELECT 1"},
		{"block comment removed", "/* DROP TABLE t */ SELECT 1", " SELECT 1"},
		{"multiline block comment", "/*\nDROP TABLE t\n*/ SELECT 1", " SELECT 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stripped := stripComments(tc.input)
			if !contains(stripped, "SELECT") {
				t.Errorf("stripComments(%q) = %q; expected SELECT to survive", tc.input, stripped)
			}
			if contains(stripped, "DROP") {
				t.Errorf("stripComments(%q) = %q; DROP should have been removed", tc.input, stripped)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
