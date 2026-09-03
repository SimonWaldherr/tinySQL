package tinysql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestExportTableJSONPublicAPI(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	for _, sql := range []string{
		`CREATE TABLE events (id INT, name TEXT)`,
		`INSERT INTO events VALUES (1, 'launch')`,
	} {
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := tinysql.ExportTableJSON(ctx, &out, db, "default", "events", tinysql.ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"launch"`) {
		t.Fatalf("JSON = %s", out.String())
	}
}

func TestExportTableJSONRestoresJSONColumns(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	for _, sql := range []string{
		`CREATE TABLE payloads (id INT, meta JSON)`,
		`INSERT INTO payloads VALUES (1, '{"source":"api"}')`,
	} {
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := tinysql.ExportTableJSON(ctx, &out, db, "default", "payloads", tinysql.ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	meta, ok := rows[0]["meta"].(map[string]any)
	if !ok || meta["source"] != "api" {
		t.Fatalf("JSON column = %#v, want object", rows[0]["meta"])
	}
}
