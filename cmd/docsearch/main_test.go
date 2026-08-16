package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

func TestIndexAndSearch(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "guide.md"), []byte("# Deployment guide\nTinySQL runs locally and persistently."), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := webapp.Open(context.Background(), "mem://?tenant=docsearch-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db, root: dir}
	if err := a.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	items, err := a.searchDocuments(context.Background(), "persistently")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Path != "guide.md" {
		t.Fatalf("search = %#v", items)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/documents/1", nil)
	req.SetPathValue("id", "1")
	res := httptest.NewRecorder()
	a.getDocument(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("document endpoint status = %d: %s", res.Code, res.Body.String())
	}
}
