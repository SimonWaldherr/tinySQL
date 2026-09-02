package webapp

import (
	"net/http"
	"strings"
	"testing"
)

// servePage returns a handler that answers "/" with html, serves the given
// static files, and sets csp when it is not empty.
func servePage(html, csp string, static map[string]string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(html))
	})
	for path, body := range static {
		body := body
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(body))
		})
	}
	if csp == "" {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", csp)
		mux.ServeHTTP(w, r)
	})
}

func kinds(problems []PageProblem) string {
	out := make([]string, 0, len(problems))
	for _, p := range problems {
		out = append(out, p.String())
	}
	return strings.Join(out, " | ")
}

// TestCheckPageFindsInlineBlockedByCSP reproduces the defect that shipped in
// cmd/accessweb: a strict CSP alongside an inline stylesheet, an inline script
// and onclick handlers. The page answered 200, so no existing test noticed,
// but the browser dropped all three.
func TestCheckPageFindsInlineBlockedByCSP(t *testing.T) {
	const csp = "default-src 'self'; style-src 'self'; script-src 'self'"
	html := `<!doctype html><html><head><style>body{margin:0}</style></head>` +
		`<body><button onclick="runQuery()">Run</button>` +
		`<script>function runQuery(){}</script></body></html>`

	problems, err := CheckPage(servePage(html, csp, nil), "/", CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 3 {
		t.Fatalf("want 3 problems, got %d: %s", len(problems), kinds(problems))
	}
	for _, p := range problems {
		if p.Kind != "blocked-inline" {
			t.Errorf("unexpected problem kind %q: %s", p.Kind, p.Detail)
		}
	}
}

// TestCheckPageAllowsInlineWhenCSPPermitsIt keeps the check honest: inline
// content is only a defect when the page's own policy forbids it.
func TestCheckPageAllowsInlineWhenCSPPermitsIt(t *testing.T) {
	html := `<!doctype html><html><head><style>body{margin:0}</style></head>` +
		`<body><script>var x=1;</script></body></html>`

	for name, csp := range map[string]string{
		"no CSP at all":        "",
		"unsafe-inline":        "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'",
		"default-src fallback": "default-src 'self' 'unsafe-inline'",
	} {
		t.Run(name, func(t *testing.T) {
			problems, err := CheckPage(servePage(html, csp, nil), "/", CheckPageOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(problems) != 0 {
				t.Fatalf("want no problems, got: %s", kinds(problems))
			}
		})
	}
}

// TestCheckPageFindsMissingAsset covers the other half: a stylesheet that the
// template references but the file server does not serve.
func TestCheckPageFindsMissingAsset(t *testing.T) {
	html := `<!doctype html><html><head>` +
		`<link rel="stylesheet" href="/static/base.css">` +
		`<link rel="stylesheet" href="/static/app.css">` +
		`</head><body><script src="/static/app.js"></script></body></html>`

	handler := servePage(html, "", map[string]string{
		"/static/app.css": "body{}",
		"/static/app.js":  "// ok",
		// /static/base.css deliberately absent.
	})

	problems, err := CheckPage(handler, "/", CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 1 || problems[0].Kind != "missing-asset" ||
		!strings.Contains(problems[0].Detail, "/static/base.css") {
		t.Fatalf("want one missing-asset for base.css, got: %s", kinds(problems))
	}
}

// TestCheckPageFlagsEmptyAsset guards against a route that exists but returns
// nothing, which looks healthy to a status-code check.
func TestCheckPageFlagsEmptyAsset(t *testing.T) {
	html := `<!doctype html><html><head><link rel="stylesheet" href="/static/app.css"></head><body></body></html>`
	handler := servePage(html, "", map[string]string{"/static/app.css": ""})

	problems, err := CheckPage(handler, "/", CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 1 || problems[0].Kind != "missing-asset" {
		t.Fatalf("want one missing-asset, got: %s", kinds(problems))
	}
}

// TestCheckPageReportsUnexpectedHosts documents the local-first expectation:
// an application that pulls its stylesheet from a CDN stops working offline.
func TestCheckPageReportsUnexpectedHosts(t *testing.T) {
	html := `<!doctype html><html><head>` +
		`<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">` +
		`</head><body></body></html>`
	handler := servePage(html, "", nil)

	problems, err := CheckPage(handler, "/", CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 1 || problems[0].Kind != "external-asset" {
		t.Fatalf("want one external-asset, got: %s", kinds(problems))
	}

	allowed, err := CheckPage(handler, "/", CheckPageOptions{
		AllowExternalAssets: []string{"cdn.jsdelivr.net"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(allowed) != 0 {
		t.Fatalf("host was allowed, want no problems, got: %s", kinds(allowed))
	}
}

// TestCheckPageIgnoresNonStylesheetLinks keeps icons, canonical URLs and
// preconnect hints out of the asset check.
func TestCheckPageIgnoresNonStylesheetLinks(t *testing.T) {
	html := `<!doctype html><html><head>` +
		`<link rel="canonical" href="https://example.com/">` +
		`<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg%3E%3C/svg%3E">` +
		`</head><body></body></html>`

	problems, err := CheckPage(servePage(html, "", nil), "/", CheckPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 0 {
		t.Fatalf("want no problems, got: %s", kinds(problems))
	}
}

// TestCheckPageRejectsNonOKStatus surfaces a broken page as an error rather
// than as an empty problem list.
func TestCheckPageRejectsNonOKStatus(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := CheckPage(handler, "/", CheckPageOptions{}); err == nil {
		t.Fatal("want an error for a 500 response")
	}
}
