//go:build js && wasm

package main

import (
	"context"
	"fmt"
	"strings"
	"syscall/js"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestExecuteMultiKeepsEarlierWritesAfterError(t *testing.T) {
	oldDB, oldCache, oldResult := db, queryCache, lastResult
	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)
	t.Cleanup(func() {
		db, queryCache, lastResult = oldDB, oldCache, oldResult
		lastResultPager.reset()
	})
	result := executeMultiRequest(js.Undefined(), []js.Value{js.ValueOf(
		"CREATE TABLE saved (id INT); INSERT INTO saved VALUES (7); INSERT INTO missing VALUES (9)",
	)}).(map[string]interface{})
	if result["success"] != false || !strings.Contains(result["error"].(string), "Statement 3") {
		t.Fatalf("unexpected batch result: %#v", result)
	}
	stmt, err := tinysql.ParseSQL("SELECT id FROM saved")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tinysql.Execute(context.Background(), db, defaultTenant, stmt)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("earlier write missing after failed batch: %#v, %v", rows, err)
	}
	if value, _ := tinysql.GetVal(rows.Rows[0], "id"); value != int64(7) && value != 7 {
		t.Fatalf("row id = %#v, want 7", value)
	}
}

func TestExecuteMultiAllowsTrailingComments(t *testing.T) {
	oldDB, oldCache, oldResult := db, queryCache, lastResult
	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)
	t.Cleanup(func() {
		db, queryCache, lastResult = oldDB, oldCache, oldResult
		lastResultPager.reset()
	})
	for _, script := range []string{
		"SELECT 1; SELECT 2; -- trailing comment",
		"SELECT 1; SELECT 2; /* trailing; comment */",
		"SELECT 1; -- CR-only comment\rSELECT 2",
	} {
		result := executeMultiRequest(js.Undefined(), []js.Value{js.ValueOf(script)}).(map[string]interface{})
		if result["success"] != true || result["statementsRun"] != 2 {
			t.Errorf("script %q: %#v", script, result)
		}
	}
	result := executeMultiRequest(js.Undefined(), []js.Value{js.ValueOf(
		"SELECT 1; SELECT 2; /* unterminated",
	)}).(map[string]interface{})
	if result["success"] != false {
		t.Fatalf("unterminated block comment should fail, got %#v", result)
	}
}

func TestExecuteMultiTriggerWithCaseAndFollowingStatements(t *testing.T) {
	oldDB, oldCache, oldResult := db, queryCache, lastResult
	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)
	t.Cleanup(func() {
		db, queryCache, lastResult = oldDB, oldCache, oldResult
		lastResultPager.reset()
	})
	script := `CREATE TABLE source (id INT, "case" INT); CREATE TABLE audit (id INT, note TEXT);
	CREATE OR REPLACE TRIGGER copy AFTER INSERT ON source BEGIN
	  INSERT INTO audit VALUES (CASE WHEN NEW.case > 0 THEN NEW.id ELSE 0 END, 'BEGIN; END');
	  /* END; */ INSERT INTO audit VALUES (NEW.id + 1, 'ok');
	END;
	INSERT INTO source VALUES (7, 1); SELECT id FROM audit ORDER BY id; -- done`
	// The engine treats Unicode whitespace as a token boundary too.
	script = strings.Replace(script, "BEGIN\n\t  INSERT", "BEGIN\u2003INSERT", 1)
	result := executeMultiRequest(js.Undefined(), []js.Value{js.ValueOf(script)}).(map[string]interface{})
	if result["success"] != true || result["statementsRun"] != 5 || result["totalRows"] != 2 {
		t.Fatalf("unexpected trigger batch result: %#v", result)
	}
	stmt, err := tinysql.ParseSQL("SELECT id FROM audit ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	got, err := tinysql.Execute(context.Background(), db, defaultTenant, stmt)
	if err != nil || len(got.Rows) != 2 {
		t.Fatalf("trigger results = %#v, %v", got, err)
	}
	for i, want := range []int64{7, 8} {
		if value, _ := tinysql.GetVal(got.Rows[i], "id"); fmt.Sprint(value) != fmt.Sprint(want) {
			t.Errorf("row %d = %#v, want %d", i, value, want)
		}
	}
}
