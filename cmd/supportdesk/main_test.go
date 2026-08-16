package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

func TestRunShowsSupportDeskWorkflow(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), "password", &out); err != nil {
		t.Fatalf("run support desk: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Open tickets (view): 2 | Audit events (trigger): 3",
		"Knowledge-base coverage (CTE):",
		"Full-text search for \"password\":",
		"Reset your password [Account]",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestWebSearchUsesFTS(t *testing.T) {
	db, err := webapp.Open(context.Background(), "mem://?tenant=supportdesk-web")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &supportDeskApp{db: db}
	if err := a.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=password", nil)
	res := httptest.NewRecorder()
	a.search(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), "Reset your password") {
		t.Fatalf("search response: %d %s", res.Code, res.Body.String())
	}
}

func TestRunRejectsBlankQuery(t *testing.T) {
	if err := run(context.Background(), "  ", &bytes.Buffer{}); err == nil {
		t.Fatal("expected blank query to fail")
	}
}
