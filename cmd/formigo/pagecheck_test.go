package main

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

// TestLoginPageServesItsOwnAssets guards the defect this application shipped:
// securityHeaders sends a Content-Security-Policy without 'unsafe-inline'
// while base.html kept the whole stylesheet in an inline <style> block, so the
// navigation gradient, the role badges and the card shadows never rendered.
//
// The login page is the one view an unauthenticated request reaches, which
// makes it the right target: it exercises the templates, the static file
// server and the session middleware together.
//
// cdn.jsdelivr.net is listed as allowed because Bootstrap is still loaded from
// it. For an application that otherwise runs entirely on local data, that is a
// weakness worth seeing here.
func TestLoginPageServesItsOwnAssets(t *testing.T) {
	ctx := context.Background()
	db, d, err := openDB(ctx, "mem://?tenant=formigo-page")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(ctx, db, d); err != nil {
		t.Fatal(err)
	}
	store := NewStore(db, d)
	if err := seedInitialAdmin(ctx, store, "admin", builtinAdminPassword); err != nil {
		t.Fatal(err)
	}
	tpl, err := parseTemplates()
	if err != nil {
		t.Fatal(err)
	}
	auth := NewAuthService(store, false)
	app := NewApp(store, auth, tpl, "admin", builtinAdminPassword)

	problems, err := webapp.CheckPage(app.handler(auth), "/login", webapp.CheckPageOptions{
		AllowExternalAssets: []string{"cdn.jsdelivr.net"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("login page: %s", p)
	}
}
