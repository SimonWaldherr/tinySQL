package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestParseStatementRejectsTrailingTokens(t *testing.T) {
	for _, sql := range []string{
		`SELECT 1 WHERE 1 = 1 unexpected_token`,
		`INSERT INTO t VALUES (1) unexpected_token`,
		`UPDATE t SET id = 2 unexpected_token`,
		`DELETE FROM t WHERE id = 1 unexpected_token`,
		`CREATE TABLE t (id INT) unexpected_token`,
	} {
		if _, err := NewParser(sql).ParseStatement(); err == nil {
			t.Errorf("ParseStatement(%q) succeeded with trailing tokens", sql)
		}
	}
}

func TestTrailingGarbageCannotExecuteDMLPrefix(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT)`)); err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1)`)); err != nil {
		t.Fatal(err)
	}
	if _, err := NewParser(`DELETE FROM t WHERE id = 1 unexpected_token`).ParseStatement(); err == nil {
		t.Fatal("malformed DELETE unexpectedly parsed")
	}
	rs := execSQL(t, db, `SELECT id FROM t`)
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 1 {
		t.Fatalf("malformed DELETE changed rows: %#v", rs.Rows)
	}
}

func TestParserCompletionKeepsMultiStatementTriggerBodiesWorking(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	stmt, err := NewParser(`
		CREATE TRIGGER trig AFTER INSERT ON source FOR EACH ROW BEGIN
			INSERT INTO audit VALUES (NEW.id);
			INSERT INTO audit VALUES (NEW.id + 1);
		END
	`).ParseStatement()
	if err != nil {
		t.Fatalf("parse trigger: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE source (id INT)`)); err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE audit (id INT)`)); err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", stmt); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO source VALUES (4)`)); err != nil {
		t.Fatalf("fire trigger: %v", err)
	}
	rs := execSQL(t, db, `SELECT id FROM audit ORDER BY id`)
	if len(rs.Rows) != 2 || rs.Rows[0]["id"] != 4 || rs.Rows[1]["id"] != 5 {
		t.Fatalf("trigger rows = %#v", rs.Rows)
	}
}

func TestParserRejectsUnterminatedTriggerBodyAndRepeatedTerminators(t *testing.T) {
	if _, err := NewParser(`CREATE TRIGGER trig AFTER INSERT ON t BEGIN INSERT INTO x VALUES (1);`).ParseStatement(); err == nil {
		t.Fatal("unterminated trigger body unexpectedly parsed")
	}
	if _, err := NewParser(`REFRESH MATERIALIZED VIEW m;;`).ParseStatement(); err == nil {
		t.Fatal("repeated statement terminators unexpectedly parsed")
	}
}
