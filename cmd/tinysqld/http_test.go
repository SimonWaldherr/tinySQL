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

	status := getJSON(t, handler, "/api/status")
	health, ok := status["health"].(map[string]any)
	if !ok {
		t.Fatalf("missing health object: %#v", status)
	}
	if health["ok"] != true || health["storage_mode"] != "disk" || health["scheduler_running"] != true {
		t.Fatalf("unexpected health object: %#v", health)
	}
}

func TestVectorAnalyticsEndpointIsOptIn(t *testing.T) {
	d := newTestDaemon(t)
	h := d.routes()
	req := httptest.NewRequest(http.MethodGet, "/api/analytics/vector", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled analytics status = %d", rec.Code)
	}
	d.analytics = true
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("enabled analytics status = %d body=%s", rec.Code, rec.Body.String())
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

func TestDaemonSQLStateOnSyntaxError(t *testing.T) {
	d := newTestDaemon(t)
	handler := d.routes()

	resp := postJSONStatus(t, handler, "/api/query", map[string]any{"sql": "SLECT * FROM missing"}, http.StatusBadRequest)
	if resp["success"] != false {
		t.Fatalf("expected failed response, got %#v", resp)
	}
	if resp["sql_state"] != tinysql.SQLStateSyntaxError {
		t.Fatalf("expected syntax SQLSTATE, got %#v", resp)
	}
	if resp["error"] == "" {
		t.Fatalf("expected original error message, got %#v", resp)
	}
}

func TestDaemonCatalogEndpoints(t *testing.T) {
	d := newTestDaemon(t)
	handler := d.routes()

	postJSON(t, handler, "/api/exec", map[string]any{"sql": "CREATE TABLE users (id INT, name TEXT)"})

	tables := getJSON(t, handler, "/api/catalog/tables")
	if rows := responseRows(t, tables); len(rows) == 0 {
		t.Fatalf("expected catalog table rows, got %#v", tables)
	}

	columns := getJSON(t, handler, "/api/catalog/columns")
	rows := responseRows(t, columns)
	foundName := false
	for _, row := range rows {
		if row["table_name"] == "users" && row["name"] == "name" {
			foundName = true
			break
		}
	}
	if !foundName {
		t.Fatalf("expected users.name in catalog columns, got %#v", rows)
	}
}

func TestDaemonJobsEndpointsAndManualRun(t *testing.T) {
	d := newTestDaemon(t)
	handler := d.routes()

	postJSON(t, handler, "/api/exec", map[string]any{"sql": "CREATE TABLE audit_log (id INT, message TEXT)"})
	created := postJSONStatus(t, handler, "/api/jobs", map[string]any{
		"name":          "audit_job",
		"sql":           "INSERT INTO audit_log VALUES (1, 'manual')",
		"schedule_type": "ONCE",
		"run_at":        "2099-01-01T00:00:00Z",
		"enabled":       false,
	}, http.StatusCreated)
	job, ok := created["job"].(map[string]any)
	if !ok || job["name"] != "audit_job" || job["schedule_type"] != "ONCE" {
		t.Fatalf("unexpected create job response: %#v", created)
	}

	bad := postJSONStatus(t, handler, "/api/jobs", map[string]any{
		"name":          "bad_job",
		"sql":           "SELECT 1",
		"schedule_type": "ONCE",
		"run_at":        "not-a-time",
	}, http.StatusBadRequest)
	if bad["success"] != false || bad["error"] == "" {
		t.Fatalf("unexpected invalid job response: %#v", bad)
	}

	jobs := getJSON(t, handler, "/api/jobs")
	rows := responseRows(t, jobs)
	foundJob := false
	for _, row := range rows {
		if row["name"] == "audit_job" {
			foundJob = true
			break
		}
	}
	if !foundJob {
		t.Fatalf("expected audit_job in jobs response, got %#v", rows)
	}

	run := postJSON(t, handler, "/api/jobs/run", map[string]any{"name": "audit_job"})
	if run["status"] != "SUCCEEDED" {
		t.Fatalf("unexpected run response: %#v", run)
	}

	query := postJSON(t, handler, "/api/query", map[string]any{"sql": "SELECT message FROM audit_log"})
	auditRows := responseRows(t, query)
	if len(auditRows) != 1 || auditRows[0]["message"] != "manual" {
		t.Fatalf("unexpected audit rows: %#v", auditRows)
	}

	history := getJSON(t, handler, "/api/job-history")
	historyRows := responseRows(t, history)
	if len(historyRows) == 0 || historyRows[len(historyRows)-1]["job_name"] != "audit_job" {
		t.Fatalf("expected audit_job history, got %#v", historyRows)
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

func getJSON(t *testing.T, handler http.Handler, path string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, body=%s", path, rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func postJSON(t *testing.T, handler http.Handler, path string, body map[string]any) map[string]any {
	return postJSONStatus(t, handler, path, body, http.StatusOK)
}

func postJSONStatus(t *testing.T, handler http.Handler, path string, body map[string]any, wantStatus int) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("POST %s status = %d, want %d, body=%s", path, rec.Code, wantStatus, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func responseRows(t *testing.T, resp map[string]any) []map[string]any {
	t.Helper()
	rawRows, ok := resp["rows"].([]any)
	if !ok {
		t.Fatalf("response rows missing or invalid: %#v", resp)
	}
	rows := make([]map[string]any, 0, len(rawRows))
	for _, raw := range rawRows {
		row, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("invalid row: %#v", raw)
		}
		rows = append(rows, row)
	}
	return rows
}
