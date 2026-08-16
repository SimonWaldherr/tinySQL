package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunJSONIncludesInactivePlaceWhenRequested(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out, "json", true); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), `"Cologne"`) {
		t.Fatalf("JSON output does not include inactive place: %s", out.String())
	}
}

func TestWebPlaceDirectoryCreatesAndListsPlace(t *testing.T) {
	a, err := newPlaceApp(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/places", strings.NewReader(`{"name":"Hamburg","country":"de","lat":53.55,"lon":9.99,"active":true}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	a.createPlace(res, req)
	if res.Code != http.StatusCreated || !strings.Contains(res.Body.String(), "Hamburg") {
		t.Fatalf("create response: %d %s", res.Code, res.Body.String())
	}
}

func TestRunRejectsUnknownFormat(t *testing.T) {
	if err := run(context.Background(), &bytes.Buffer{}, "xml", false); err == nil {
		t.Fatal("expected unsupported-format error")
	}
}
