package engine

import (
	"reflect"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name   string
		script string
		want   []string
	}{
		{"empty", " ;; \n", nil},
		{"simple", "SELECT 1; SELECT 2", []string{"SELECT 1", "SELECT 2"}},
		{"trailing semicolon", "SELECT 1;\n", []string{"SELECT 1"}},
		{"string", "INSERT INTO t VALUES ('a;b'); SELECT 'it''s;'", []string{"INSERT INTO t VALUES ('a;b')", "SELECT 'it''s;'"}},
		{"identifier and blob", `SELECT "a;b", X'3b' FROM t; SELECT 2`, []string{`SELECT "a;b", X'3b' FROM t`, "SELECT 2"}},
		{"comments", "-- lead; comment\nSELECT 1 /* ; */; -- tail;\nSELECT 2", []string{"SELECT 1 /* ; */", "SELECT 2"}},
		{"transaction", "BEGIN; INSERT INTO t VALUES (1); COMMIT;", []string{"BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"}},
		{
			"trigger body",
			"CREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE c SET n = CASE WHEN NEW.id > 0 THEN 1 ELSE 0 END; DELETE FROM d; END; SELECT 1",
			[]string{"CREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE c SET n = CASE WHEN NEW.id > 0 THEN 1 ELSE 0 END; DELETE FROM d; END", "SELECT 1"},
		},
		{
			"temp trigger",
			"create temp trigger x after insert on t for each row begin insert into a values (1); end;select 2",
			[]string{"create temp trigger x after insert on t for each row begin insert into a values (1); end", "select 2"},
		},
		{"case outside trigger", "SELECT CASE WHEN 1 THEN 2 END; SELECT 3", []string{"SELECT CASE WHEN 1 THEN 2 END", "SELECT 3"}},
		{"unicode", "SELECT 'Grüße 🦊;'; SELECT 'ok'", []string{"SELECT 'Grüße 🦊;'", "SELECT 'ok'"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SplitStatements(tc.script); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SplitStatements(%q)\n got %#v\nwant %#v", tc.script, got, tc.want)
			}
		})
	}
}

func TestSplitStatementsParseEachTriggerStatement(t *testing.T) {
	script := `CREATE TABLE src (id INT); CREATE TABLE audit (id INT);
CREATE TRIGGER copy AFTER INSERT ON src FOR EACH ROW BEGIN
  INSERT INTO audit VALUES (CASE WHEN NEW.id > 0 THEN NEW.id ELSE 0 END);
END;`
	statements := SplitStatements(script)
	if len(statements) != 3 {
		t.Fatalf("got %d statements: %#v", len(statements), statements)
	}
	for _, statement := range statements {
		if _, err := NewParser(statement).ParseStatement(); err != nil {
			t.Fatalf("%q: %v", statement, err)
		}
	}
}

func TestStatementComplete(t *testing.T) {
	for script, want := range map[string]bool{
		"":                    false,
		"   ":                 false,
		"SELECT 1":            false,
		"SELECT 1;":           true,
		"SELECT 1; -- done":   true,
		"SELECT 1;\nSELECT 2": false,
		"SELECT 'a;":          false,
		"SELECT 'a;';":        true,
		`SELECT "x;`:          false,
		"-- don't\nSELECT 1;": true,
		"/* ; */":             false,
		"CREATE TRIGGER t AFTER INSERT ON x BEGIN INSERT INTO y VALUES (1);":      false,
		"CREATE TRIGGER t AFTER INSERT ON x BEGIN INSERT INTO y VALUES (1); END":  false,
		"CREATE TRIGGER t AFTER INSERT ON x BEGIN INSERT INTO y VALUES (1); END;": true,
		"BEGIN;": true,
	} {
		if got := StatementComplete(script); got != want {
			t.Errorf("StatementComplete(%q) = %v, want %v", script, got, want)
		}
	}
}

func FuzzSplitStatements(f *testing.F) {
	for _, seed := range []string{"SELECT 1; SELECT 2", "'a;b';", "CREATE TRIGGER t BEGIN x; END;", "/* ; */ --;\n;"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, script string) {
		for _, statement := range SplitStatements(script) {
			if statement == "" {
				t.Fatal("empty statement")
			}
		}
	})
}
