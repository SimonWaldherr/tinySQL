package main

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

// TestPagesServeTheirOwnAssets guards the defect this application actually
// shipped: the strict Content-Security-Policy in securityHeaders silently
// dropped the inline <style> block and all three inline <script> blocks, so
// the stylesheet never applied and Run, Clear, CSV, JSON, "Add Column" and the
// delete confirmations did nothing. Every existing test still saw a 200.
//
// cdn.jsdelivr.net is listed as allowed because Bootstrap is still loaded from
// it. That is a real weakness for a local-first application — the admin UI
// falls apart without internet access — and this list is where to notice it.
func TestPagesServeTheirOwnAssets(t *testing.T) {
	app := newTestApp(t)
	handler := app.handler()
	opts := webapp.CheckPageOptions{AllowExternalAssets: []string{"cdn.jsdelivr.net"}}

	for _, path := range []string{"/", "/query", "/create-table"} {
		t.Run(path, func(t *testing.T) {
			problems, err := webapp.CheckPage(handler, path, opts)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range problems {
				t.Errorf("%s: %s", path, p)
			}
		})
	}
}
