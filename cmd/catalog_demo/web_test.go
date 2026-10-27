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

func TestDashboardRunsRegisteredJob(t *testing.T) {
	a, err := newCatalogWebApp(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.StopJobScheduler()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/integrity_check/run", nil)
	req.SetPathValue("name", "integrity_check")
	res := httptest.NewRecorder()
	a.runJob(res, req)
	if res.Code != http.StatusOK || a.runs[0].Rows != 1 {
		t.Fatalf("job response %d / runs %#v", res.Code, a.runs)
	}
}

func TestDashboardJobHistoryFilterAndScheduledName(t *testing.T) {
	a, err := newCatalogWebApp(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.StopJobScheduler()
	job, err := a.catalog.GetJob("integrity_check")
	if err != nil {
		t.Fatal(err)
	}
	executor := &webSchedulerExecutor{app: a}
	if _, err := executor.ExecuteSQL(context.Background(), job.SQLText); err != nil {
		t.Fatal(err)
	}
	if _, err := a.executeSQL(context.Background(), "refresh_event_stats", `SELECT 1`); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/state?job=integrity_check", nil)
	res := httptest.NewRecorder()
	a.handler().ServeHTTP(res, req)
	var state struct {
		Runs []jobRun `json:"runs"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if res.Code != http.StatusOK || len(state.Runs) != 1 || state.Runs[0].Job != "integrity_check" {
		t.Fatalf("filtered state: %d %#v", res.Code, state.Runs)
	}
	if err := a.catalog.RegisterJob(&tinysql.CatalogJob{Name: "duplicate", SQLText: job.SQLText}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ExecuteSQL(context.Background(), job.SQLText); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.runs[0].Job != "scheduled" {
		t.Fatalf("ambiguous SQL attributed to %q", a.runs[0].Job)
	}
}

func TestDashboardStateUsesEmptyRunArray(t *testing.T) {
	a, err := newCatalogWebApp(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.StopJobScheduler()
	req := httptest.NewRequest(http.MethodGet, "/api/state", nil)
	res := httptest.NewRecorder()
	a.state(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"runs":[]`) {
		t.Fatalf("state response: %d %s", res.Code, res.Body.String())
	}
}
