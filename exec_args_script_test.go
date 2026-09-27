package tinysql_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/sqlutil"
)

func ExampleExecSQLArgs() {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()

	if _, err := tsql.ExecSQL(ctx, db, "default", "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		panic(err)
	}
	// Values are bound as literals; quotes in user input cannot change the SQL.
	if _, err := tsql.ExecSQLArgs(ctx, db, "default", "INSERT INTO users VALUES (?, ?)", 1, "O'Hara"); err != nil {
		panic(err)
	}
	rs, err := tsql.ExecSQLArgs(ctx, db, "default", "SELECT name FROM users WHERE id = $1", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(rs.Rows[0]["name"])
	// Output: O'Hara
}

func ExampleExecScript() {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()

	rs, err := tsql.ExecScript(ctx, db, "default", `
		-- Schema and seed data in one call.
		CREATE TABLE notes (id INT, body TEXT);
		INSERT INTO notes VALUES (1, 'semicolons; inside literals are fine');
		SELECT COUNT(*) AS n FROM notes;`)
	if err != nil {
		panic(err)
	}
	fmt.Println(rs.Rows[0]["n"])
	// Output: 1
}

func TestExecSQLArgsTypes(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()
	if _, err := tsql.ExecSQL(ctx, db, "default", "CREATE TABLE v (i INT, f FLOAT, s TEXT, b BOOL, raw BLOB, n TEXT, ts TEXT)"); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if _, err := tsql.ExecSQLArgs(ctx, db, "default", "INSERT INTO v VALUES (?, ?, ?, ?, ?, ?, ?)",
		int32(-7), 2.5, "a'b -- ? ;", true, []byte{0, 1, 255}, nil, when); err != nil {
		t.Fatal(err)
	}
	rs, err := tsql.ExecSQLArgs(ctx, db, "default", `SELECT * FROM v WHERE s = ? AND "i" = :2`, "a'b -- ? ;", -7)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows: %#v", rs.Rows)
	}
	row := rs.Rows[0]
	if fmt.Sprint(row["i"]) != "-7" || row["s"] != "a'b -- ? ;" || row["b"] != true || row["n"] != nil {
		t.Fatalf("unexpected row: %#v", row)
	}
	if raw, ok := row["raw"].([]byte); !ok || string(raw) != "\x00\x01\xff" {
		t.Fatalf("blob: %#v", row["raw"])
	}
	if row["ts"] != "2026-09-27T12:00:00Z" {
		t.Fatalf("time: %#v", row["ts"])
	}
	for _, bad := range []struct {
		sql  string
		args []any
	}{{"SELECT ?", nil}, {"SELECT 1", []any{1}}, {"SELECT $2", []any{1}}} {
		if _, err := tsql.ExecSQLArgs(ctx, db, "default", bad.sql, bad.args...); err == nil {
			t.Errorf("%q with %d args succeeded", bad.sql, len(bad.args))
		}
	}
}

func TestExecScriptStopsAtFailingStatement(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()
	_, err := tsql.ExecScript(ctx, db, "default", `
		CREATE TABLE t (id INT);
		INSERT INTO t VALUES (1);
		INSERT INTO missing VALUES (2);
		INSERT INTO t VALUES (3);`)
	if err == nil || !strings.Contains(err.Error(), "statement 3") {
		t.Fatalf("error %v, want statement 3", err)
	}
	rs, err := tsql.ExecSQL(ctx, db, "default", "SELECT COUNT(*) AS n FROM t")
	if err != nil || fmt.Sprint(rs.Rows[0]["n"]) != "1" {
		t.Fatalf("applied prefix: %#v, %v", rs, err)
	}
	if rs, err := tsql.ExecScript(ctx, db, "default", " ; -- nothing\n"); err != nil || rs != nil {
		t.Fatalf("empty script: %#v, %v", rs, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tsql.ExecScript(cancelled, db, "default", "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled script: %v", err)
	}
}

func TestExecScriptKeepsTriggerBodies(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()
	script := `
CREATE TABLE src (id INT);
CREATE TABLE audit (id INT, kind TEXT);
CREATE TRIGGER copy AFTER INSERT ON src FOR EACH ROW BEGIN
	INSERT INTO audit VALUES (NEW.id, CASE WHEN NEW.id > 1 THEN 'big' ELSE 'small' END);
	INSERT INTO audit VALUES (NEW.id, 'copy;');
END;
INSERT INTO src VALUES (2);
SELECT COUNT(*) AS n FROM audit;`
	if got := len(sqlutil.SplitStatements(script)); got != 5 {
		t.Fatalf("split into %d statements", got)
	}
	rs, err := tsql.ExecScript(ctx, db, "default", script)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rs.Rows[0]["n"]) != "2" {
		t.Fatalf("trigger rows: %#v", rs.Rows)
	}
}
