package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

// newTestApp creates a fully isolated App for testing. Each call uses a unique
// storage DB. The driver's empty DSN is the intentional embedding path that
// reuses the DB installed by SetDefaultDB; named mem:// DSNs own a separate DB.
func newTestApp(t *testing.T) *App {
	t.Helper()

	nativeDB := tinysql.NewDB()
	tsqldriver.SetDefaultDB(nativeDB)
	tenant := "default"

	sqlDB, err := sql.Open("tinysql", "")
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	// Force a single connection to avoid pool-reuse across SetDefaultDB calls.
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(0)
	t.Cleanup(func() { sqlDB.Close() })

	tpl, err := parseTemplates()
	if err != nil {
		t.Fatalf("parse templates: %v", err)
	}
	return newApp(nativeDB, sqlDB, tenant, tpl)
}

func TestIndexRedirectsToFirstTable(t *testing.T) {
	app := newTestApp(t)

	// Create a table so the index redirects to it.
	if _, err := app.sqlDB.Exec("CREATE TABLE items (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	app.indexHandler(w, req)

	if w.Code != http.StatusSeeOther {
		t.Errorf("expected 303, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	if !strings.Contains(loc, "items") {
		t.Errorf("expected redirect to items table, got %q", loc)
	}
}

func TestIndexNoTables(t *testing.T) {
	app := newTestApp(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	app.indexHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "No tables yet") {
		t.Errorf("expected empty-state message, got:\n%s", w.Body.String())
	}
}

func TestTableViewHandler(t *testing.T) {
	app := newTestApp(t)
	if _, err := app.sqlDB.Exec("CREATE TABLE people (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := app.sqlDB.Exec("INSERT INTO people (id, name) VALUES (1, 'Alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/t/people", nil)
	req.SetPathValue("table", "people")
	w := httptest.NewRecorder()
	app.tableViewHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "Alice") {
		t.Errorf("expected Alice in table view, got:\n%s", body)
	}
}

func TestCreateTableHandler(t *testing.T) {
	app := newTestApp(t)

	form := url.Values{}
	form.Set("table_name", "products")
	form.Add("col_name", "title")
	form.Add("col_type", "TEXT")
	form.Add("col_name", "price")
	form.Add("col_type", "REAL")

	req := httptest.NewRequest(http.MethodPost, "/create-table",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	app.createTableHandler(w, req)

	if w.Code != http.StatusSeeOther {
		t.Errorf("expected 303, got %d; body: %s", w.Code, w.Body.String())
	}

	// Verify table was created.
	tables := app.tableNames()
	found := false
	for _, n := range tables {
		if n == "products" {
			found = true
		}
	}
	if !found {
		t.Errorf("table 'products' not found in %v", tables)
	}
}

func TestRecordCRUD(t *testing.T) {
	app := newTestApp(t)
	if _, err := app.sqlDB.Exec("CREATE TABLE notes (id INT, body TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ctx := context.Background()

	// Insert
	meta, err := app.tableMeta(ctx, "notes")
	if err != nil {
		t.Fatalf("tableMeta: %v", err)
	}
	if err := app.insertRecord(ctx, "notes", map[string]string{"body": "hello"}, meta.Columns); err != nil {
		t.Fatalf("insertRecord: %v", err)
	}

	// Read back
	cols, row, err := app.getRecord(ctx, "notes", "1")
	if err != nil {
		t.Fatalf("getRecord: %v", err)
	}
	vals := make(map[string]string, len(cols))
	for i, c := range cols {
		vals[c.Name] = row[i]
	}
	if vals["body"] != "hello" {
		t.Errorf("expected body=hello, got %q", vals["body"])
	}

	// Update
	if err := app.updateRecord(ctx, "notes", "1", map[string]string{"body": "world"}, meta.Columns); err != nil {
		t.Fatalf("updateRecord: %v", err)
	}
	cols2, row2, err := app.getRecord(ctx, "notes", "1")
	if err != nil {
		t.Fatalf("getRecord after update: %v", err)
	}
	vals2 := make(map[string]string, len(cols2))
	for i, c := range cols2 {
		vals2[c.Name] = row2[i]
	}
	if vals2["body"] != "world" {
		t.Errorf("expected body=world after update, got %q", vals2["body"])
	}

	// Delete
	if err := app.deleteRecord(ctx, "notes", "1"); err != nil {
		t.Fatalf("deleteRecord: %v", err)
	}
	meta2, _ := app.tableMeta(ctx, "notes")
	if meta2.RowCount != 0 {
		t.Errorf("expected 0 rows after delete, got %d", meta2.RowCount)
	}
}

func TestQueryEditor(t *testing.T) {
	app := newTestApp(t)

	// GET query editor
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	w := httptest.NewRecorder()
	app.queryEditorHandler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "SQL Editor") {
		t.Errorf("expected SQL Editor heading")
	}
}

func TestAPIQueryHandler(t *testing.T) {
	app := newTestApp(t)
	if _, err := app.sqlDB.Exec("CREATE TABLE vals (id INT, v INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := app.sqlDB.Exec("INSERT INTO vals (id, v) VALUES (1, 42)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	body := strings.NewReader(`{"sql":"SELECT * FROM vals"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	app.apiQueryHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "42") {
		t.Errorf("expected 42 in response, got: %s", w.Body.String())
	}
}

func TestTableExportCSVAndJSON(t *testing.T) {
	app := newTestApp(t)
	if _, err := app.sqlDB.Exec("CREATE TABLE people (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := app.sqlDB.Exec("INSERT INTO people (id, name) VALUES (1, 'Alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	csvReq := httptest.NewRequest(http.MethodGet, "/t/people/export?format=csv", nil)
	csvReq.SetPathValue("table", "people")
	csvRec := httptest.NewRecorder()
	app.exportTableHandler(csvRec, csvReq)
	if csvRec.Code != http.StatusOK {
		t.Fatalf("expected CSV export 200, got %d; body: %s", csvRec.Code, csvRec.Body.String())
	}
	if got := csvRec.Header().Get("Content-Type"); !strings.Contains(got, "text/csv") {
		t.Errorf("expected text/csv content type, got %q", got)
	}
	if body := csvRec.Body.String(); !strings.Contains(body, "id") ||
		!strings.Contains(body, "name") ||
		!strings.Contains(body, "1") ||
		!strings.Contains(body, "Alice") {
		t.Errorf("unexpected CSV body: %s", body)
	}

	jsonReq := httptest.NewRequest(http.MethodGet, "/t/people/export?format=json", nil)
	jsonReq.SetPathValue("table", "people")
	jsonRec := httptest.NewRecorder()
	app.exportTableHandler(jsonRec, jsonReq)
	if jsonRec.Code != http.StatusOK {
		t.Fatalf("expected JSON export 200, got %d; body: %s", jsonRec.Code, jsonRec.Body.String())
	}
	if got := jsonRec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Errorf("expected application/json content type, got %q", got)
	}
	if body := jsonRec.Body.String(); !strings.Contains(body, `"name":"Alice"`) {
		t.Errorf("unexpected JSON body: %s", body)
	}
}

func TestAPIExportHandler(t *testing.T) {
	app := newTestApp(t)
	if _, err := app.sqlDB.Exec("CREATE TABLE vals (id INT, v INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := app.sqlDB.Exec("INSERT INTO vals (id, v) VALUES (1, 42)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	body := strings.NewReader(`{"sql":"SELECT * FROM vals","format":"json"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/export", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	app.apiExportHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Disposition"); !strings.Contains(got, "query.json") {
		t.Errorf("expected query.json content disposition, got %q", got)
	}
	if !strings.Contains(w.Body.String(), `"v":"42"`) {
		t.Errorf("expected exported value, got: %s", w.Body.String())
	}
}

func TestAPIExportRejectsMutation(t *testing.T) {
	app := newTestApp(t)

	body := strings.NewReader(`{"sql":"CREATE TABLE nope (id INT)","format":"csv"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/export", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	app.apiExportHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "export requires") {
		t.Errorf("expected export requires error, got: %s", w.Body.String())
	}
}

func TestSafeExportFilenameBase(t *testing.T) {
	tests := map[string]string{
		`people`:          "people",
		`bad"name`:        "bad_name",
		" spaced table ":  "spaced_table",
		"reports-2026.v1": "reports-2026.v1",
		"":                "export",
	}

	for input, want := range tests {
		if got := safeExportFilenameBase(input); got != want {
			t.Errorf("safeExportFilenameBase(%q) = %q, want %q", input, got, want)
		}
	}
}
