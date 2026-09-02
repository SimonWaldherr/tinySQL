package webapp

import (
	"embed"
	"net/http"
)

// staticAssets holds the stylesheet shared by the small demo applications.
// Each app embeds its own static/ directory, and go:embed cannot reach out of
// a package directory, so the shared parts live here and are mounted through
// MountShared.
//
//go:embed static/base.css
var staticAssets embed.FS

// MountShared registers the assets that every demo shares under /static/.
// Call it before the app's own "GET /static/" file server: ServeMux prefers
// the more specific pattern, so the app keeps full control of every other
// path below /static/.
func MountShared(mux *http.ServeMux) {
	mux.Handle("GET /static/base.css", http.FileServer(http.FS(staticAssets)))
}
