package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func newTestWebState(t *testing.T) *webState {
	t.Helper()
	db := tinysql.NewDB()
	ctx := context.Background()

	// Create a test table
	stmt, err := tinysql.ParseSQL("CREATE TABLE users (id INT, name TEXT, email TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
		t.Fatal(err)
	}

	stmt, err = tinysql.ParseSQL("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
		t.Fatal(err)
	}

	stmt, err = tinysql.ParseSQL("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
		t.Fatal(err)
	}

	return &webState{db: db, ctx: ctx, tenant: "default"}
}

func TestWebIndex(t *testing.T) {
	state := newTestWebState(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	state.handleIndex(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("expected text/html, got %s", ct)
	}
	if !strings.Contains(w.Body.String(), "tinySQL Migrate") {
		t.Error("expected page to contain 'tinySQL Migrate'")
	}
}

func TestWebQuery(t *testing.T) {
	state := newTestWebState(t)

	body := `{"sql":"SELECT * FROM users ORDER BY id"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	state.handleQuery(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	if resp.RowCount != 2 {
		t.Errorf("expected 2 rows, got %d", resp.RowCount)
	}
	if len(resp.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(resp.Columns))
	}
}

func TestWebQueryError(t *testing.T) {
	state := newTestWebState(t)

	body := `{"sql":"SELECT * FROM nonexistent"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	state.handleQuery(w, req)

	var resp apiResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Error == "" {
		t.Error("expected error for nonexistent table")
	}
}

func TestWebQueryEmptySQL(t *testing.T) {
	state := newTestWebState(t)

	body := `{"sql":""}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	state.handleQuery(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestWebQueryMethodNotAllowed(t *testing.T) {
	state := newTestWebState(t)

	req := httptest.NewRequest(http.MethodGet, "/api/query", nil)
	w := httptest.NewRecorder()
	state.handleQuery(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestWebTables(t *testing.T) {
	state := newTestWebState(t)

	req := httptest.NewRequest(http.MethodGet, "/api/tables", nil)
	w := httptest.NewRecorder()
	state.handleTables(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp apiResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}
	if len(resp.Tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(resp.Tables))
	}
	if resp.Tables[0].Name != "users" {
		t.Errorf("expected table 'users', got '%s'", resp.Tables[0].Name)
	}
}

func TestWebConnections(t *testing.T) {
	state := newTestWebState(t)

	req := httptest.NewRequest(http.MethodGet, "/api/connections", nil)
	w := httptest.NewRecorder()
	state.handleConnections(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp apiResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.Success {
		t.Error("expected success")
	}
}

func TestWebExportJSON(t *testing.T) {
	state := newTestWebState(t)

	body := `{"sql":"SELECT * FROM users","format":"json"}`
	req := httptest.NewRequest(http.MethodPost, "/api/export", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	state.handleAPIExport(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}
}

func TestWebExportCSV(t *testing.T) {
	state := newTestWebState(t)

	body := `{"sql":"SELECT * FROM users","format":"csv"}`
	req := httptest.NewRequest(http.MethodPost, "/api/export", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	state.handleAPIExport(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/csv" {
		t.Errorf("expected text/csv, got %s", ct)
	}
}

func TestWebDDLQuery(t *testing.T) {
	state := newTestWebState(t)

	body := `{"sql":"CREATE TABLE test (id INT, val TEXT)"}`
	req := httptest.NewRequest(http.MethodPost, "/api/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	state.handleQuery(w, req)

	var resp apiResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected success for DDL, got error: %s", resp.Error)
	}
	if resp.Message != "OK" {
		t.Errorf("expected 'OK' message, got '%s'", resp.Message)
	}
}
