package main

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

// TestPageServesItsOwnAssets guards the class of defect that a status-code
// test cannot see: a stylesheet or script the page references but nobody
// serves, and inline content that the application's own Content-Security-Policy
// throws away. See webapp.CheckPage for why this exists.
func TestPageServesItsOwnAssets(t *testing.T) {
	ctx := context.Background()
	db, source, err := openPOIDatabase(ctx, config{})
	if err != nil {
		t.Fatal(err)
	}
	tpl, err := template.ParseFS(webAssets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	a := &offlineApp{db: db, source: source, readOnly: true, tpl: tpl}

	problems, err := webapp.CheckPage(a.handler(), "/", webapp.CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("index page: %s", p)
	}
}

func TestWebStartsWithEmptySearchAndAllPOIs(t *testing.T) {
	db, source, err := openPOIDatabase(context.Background(), config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	tpl, err := template.ParseFS(webAssets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	a := &offlineApp{db: db, source: source, readOnly: true, tpl: tpl}
	page := httptest.NewRecorder()
	a.handler().ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	if page.Code != http.StatusOK {
		t.Fatalf("page: status=%d", page.Code)
	}
	input := regexp.MustCompile(`<input\b[^>]*\bid="query"[^>]*>`).FindString(page.Body.String())
	if input == "" {
		t.Fatal("missing query input")
	}
	if value := regexp.MustCompile(`\bvalue\s*=\s*"([^"]*)"`).FindStringSubmatch(input); len(value) > 0 && value[1] != "" {
		t.Fatalf("query input must start empty, got %q", value[1])
	}

	results := httptest.NewRecorder()
	a.handler().ServeHTTP(results, httptest.NewRequest(http.MethodGet, "/api/search?q=", nil))
	var payload struct {
		Results []poi `json:"results"`
	}
	if results.Code != http.StatusOK || json.Unmarshal(results.Body.Bytes(), &payload) != nil || len(payload.Results) != 5 {
		t.Fatalf("initial search: status=%d body=%s", results.Code, results.Body.String())
	}
}
