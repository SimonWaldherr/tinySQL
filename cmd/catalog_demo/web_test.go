package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
