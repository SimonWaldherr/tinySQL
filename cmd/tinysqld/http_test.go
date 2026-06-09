package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func newTestDaemon(t *testing.T) *daemon {
	t.Helper()
	inst, err := tinysql.OpenEnterprise(tinysql.StorageConfig{
		Mode: tinysql.ModeDisk,
		Path: filepath.Join(t.TempDir(), "db"),
	}, "default")
	if err != nil {
		t.Fatalf("OpenEnterprise failed: %v", err)
	}
	t.Cleanup(func() {
		if err := inst.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
	return newDaemon(inst, daemonConfig{DefaultTenant: "default"})
}

func TestDaemonHealthReadyStatus(t *testing.T) {
	d := newTestDaemon(t)
	handler := d.routes()

	for _, path := range []string{"/healthz", "/readyz", "/api/status"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body=%s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestDaemonExecAndQuery(t *testing.T) {
	d := newTestDaemon(t)
	handler := d.routes()

	postJSON(t, handler, "/api/exec", map[string]any{"sql": "CREATE TABLE users (id INT, name TEXT)"})
	postJSON(t, handler, "/api/exec", map[string]any{"sql": "INSERT INTO users VALUES (1, 'Ada')"})

	resp := postJSON(t, handler, "/api/query", map[string]any{"sql": "SELECT name FROM users"})
	rows, ok := resp["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("unexpected rows: %#v", resp["rows"])
	}
	row, ok := rows[0].(map[string]any)
	if !ok || row["name"] != "Ada" {
		t.Fatalf("unexpected row: %#v", rows[0])
	}
}

func TestDaemonAuth(t *testing.T) {
	d := newTestDaemon(t)
	d.authToken = "secret"
	handler := d.routes()

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status with auth = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func postJSON(t *testing.T, handler http.Handler, path string, body map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST %s status = %d, body=%s", path, rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}
