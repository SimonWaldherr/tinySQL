package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

func TestValidateRejectsDuplicateEmailAndCommitWritesValidImport(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=import-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	bad, issues, err := a.validateCSV(ctx, "bad.csv", "contacts", `[{"column":"email","required":true,"type":"email","unique":true}]`, strings.NewReader("name,email\nAda,ada@example.test\nBea,ada@example.test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if bad.Status != "rejected" || len(issues) != 1 {
		t.Fatalf("bad import = %#v, %#v", bad, issues)
	}
	good, issues, err := a.validateCSV(ctx, "good.csv", "contacts", `[{"column":"email","required":true,"type":"email","unique":true}]`, strings.NewReader("name,email\nAda,ada@example.test\n"))
	if err != nil || len(issues) != 0 {
		t.Fatalf("good import: %#v %v", issues, err)
	}
	if _, err := a.commit(ctx, strconv.FormatInt(good.ID, 10)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contacts").Scan(&n); err != nil || n != 1 {
		t.Fatalf("contacts: %d, %v", n, err)
	}
}

func TestPreviewRowsIncludesMappingsWithoutCommit(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=import-preview")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	var csv strings.Builder
	csv.WriteString("name,age\n")
	for i := 0; i < 25; i++ {
		fmt.Fprintf(&csv, "person%d,%d\n", i, i)
	}
	run, _, err := a.validateCSV(ctx, "people.csv", "contacts", `[{"column":"name","target":"full_name"}]`, strings.NewReader(csv.String()))
	if err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	a.handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/imports/%d/preview", run.ID), nil))
	if res.Code != http.StatusOK {
		t.Fatalf("status %d: %s", res.Code, res.Body.String())
	}
	var rows []previewRow
	if err := json.Unmarshal(res.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 || rows[0].Row != 2 || rows[0].Values["full_name"] != "person0" || rows[19].Row != 21 {
		t.Fatalf("preview: %#v", rows)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contacts").Scan(&count); err == nil {
		t.Fatal("preview created the target table")
	}
	rejected, _, err := a.validateCSV(ctx, "invalid.csv", "contacts", `[{"column":"age","type":"integer"}]`, strings.NewReader("name,age\nAda,nope\n"))
	if err != nil || rejected.Status != "rejected" {
		t.Fatalf("rejected import: %#v, %v", rejected, err)
	}
	res = httptest.NewRecorder()
	a.handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/imports/%d/preview", rejected.ID), nil))
	rows = nil
	if err := json.Unmarshal(res.Body.Bytes(), &rows); res.Code != http.StatusOK || err != nil || len(rows) != 1 || rows[0].Values["age"] != "nope" {
		t.Fatalf("rejected preview: %d %s %v", res.Code, res.Body.String(), err)
	}
	for _, tc := range []struct {
		id     string
		status int
	}{{"bad", 400}, {"99999", 404}} {
		res := httptest.NewRecorder()
		a.handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/imports/"+tc.id+"/preview", nil))
		if res.Code != tc.status {
			t.Fatalf("id %s: status %d", tc.id, res.Code)
		}
	}
}
