package main

import (
	"context"
	"html/template"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

// TestPageServesItsOwnAssets guards the class of defect that a status-code
// test cannot see: a stylesheet or script the page references but nobody
// serves, and inline content that the application's own Content-Security-Policy
// throws away. See webapp.CheckPage for why this exists.
func TestPageServesItsOwnAssets(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=supportdesk-page")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tpl, err := template.ParseFS(supportAssets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	a := &supportDeskApp{db: db, tpl: tpl}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}

	problems, err := webapp.CheckPage(a.handler(), "/", webapp.CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("index page: %s", p)
	}
}
