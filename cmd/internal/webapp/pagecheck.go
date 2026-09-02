package webapp

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
)

// PageProblem describes one defect found by CheckPage.
type PageProblem struct {
	Kind   string // "missing-asset", "blocked-inline", "external-asset"
	Detail string
}

func (p PageProblem) String() string { return p.Kind + ": " + p.Detail }

// CheckPageOptions tunes CheckPage for a specific application.
type CheckPageOptions struct {
	// AllowExternalAssets lists hosts the page is permitted to load from, for
	// example "cdn.jsdelivr.net". Any other absolute URL is reported. A page
	// that loads nothing remotely — the goal for a local-first application —
	// leaves this empty.
	AllowExternalAssets []string
}

var (
	reLinkHref  = regexp.MustCompile(`(?is)<link\b[^>]*\bhref\s*=\s*["']([^"']+)["'][^>]*>`)
	reScriptSrc = regexp.MustCompile(`(?is)<script\b[^>]*\bsrc\s*=\s*["']([^"']+)["']`)
	// Go's regexp has no lookahead, so the src-less case is decided in
	// hasInlineScript by inspecting the captured attribute list.
	reScriptOpen = regexp.MustCompile(`(?is)<script\b([^>]*)>\s*(\S?)`)
	reInlineCSS  = regexp.MustCompile(`(?is)<style\b[^>]*>\s*\S`)
	reStyleAttr  = regexp.MustCompile(`(?is)<[a-z][^>]*\sstyle\s*=\s*["']`)
	reEventAttr  = regexp.MustCompile(`(?is)<[a-z][^>]*\son(?:click|submit|change|input|load|error|focus|blur|keydown|keyup)\s*=\s*["']`)
	reRelStyle   = regexp.MustCompile(`(?is)\brel\s*=\s*["']?stylesheet`)
	reSrcAttr    = regexp.MustCompile(`(?is)\bsrc\s*=`)
)

// CheckPage fetches path from handler and reports the defects that a plain
// status-code test cannot see.
//
// It exists because two applications in this repository shipped a
// Content-Security-Policy without 'unsafe-inline' while keeping their entire
// stylesheet — and, in one case, every interactive handler — in inline <style>
// and <script> blocks. The browser dropped all of it, so the page rendered
// unstyled and its buttons did nothing, while every Go test still saw a
// healthy 200. The checks below are exactly the ones that would have caught
// that on the first day:
//
//   - every same-origin stylesheet and script the page references is actually
//     served,
//   - no inline style/script content or on*= handler survives a CSP that
//     forbids it, and
//   - no asset is loaded from an unexpected host.
func CheckPage(handler http.Handler, path string, opts CheckPageOptions) ([]PageProblem, error) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d, want 200", path, res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", path, err)
	}
	html := string(body)

	var problems []PageProblem
	add := func(kind, format string, args ...any) {
		problems = append(problems, PageProblem{Kind: kind, Detail: fmt.Sprintf(format, args...)})
	}

	// ── Inline content the page's own CSP would drop ────────────────────────
	csp := res.Header.Get("Content-Security-Policy")
	inlineStyleOK := cspAllowsInline(csp, "style-src")
	inlineScriptOK := cspAllowsInline(csp, "script-src")

	if !inlineStyleOK {
		if reInlineCSS.MatchString(html) {
			add("blocked-inline", "<style> block, but style-src in the response CSP has no 'unsafe-inline'; move it to a served .css file")
		}
		if reStyleAttr.MatchString(html) {
			add("blocked-inline", `style="" attribute, but style-src in the response CSP has no 'unsafe-inline'; move it to a class`)
		}
	}
	if !inlineScriptOK {
		if hasInlineScript(html) {
			add("blocked-inline", "<script> block without src, but script-src in the response CSP has no 'unsafe-inline'; move it to a served .js file")
		}
		if reEventAttr.MatchString(html) {
			add("blocked-inline", `on*="" handler, but script-src in the response CSP has no 'unsafe-inline'; bind it with addEventListener instead`)
		}
	}

	// ── Referenced assets ───────────────────────────────────────────────────
	allowed := make(map[string]bool, len(opts.AllowExternalAssets))
	for _, host := range opts.AllowExternalAssets {
		allowed[host] = true
	}

	for _, ref := range pageAssets(html) {
		switch {
		case strings.HasPrefix(ref, "data:"):
			// Inlined payload, nothing to fetch.
		case strings.HasPrefix(ref, "http://"), strings.HasPrefix(ref, "https://"), strings.HasPrefix(ref, "//"):
			if host := refHost(ref); !allowed[host] {
				add("external-asset", "%s is loaded from %s, which the test does not allow", ref, host)
			}
		case strings.HasPrefix(ref, "/"):
			assetRec := httptest.NewRecorder()
			handler.ServeHTTP(assetRec, httptest.NewRequest(http.MethodGet, ref, nil))
			if assetRec.Code != http.StatusOK {
				add("missing-asset", "%s returns %d, want 200", ref, assetRec.Code)
			} else if assetRec.Body.Len() == 0 {
				add("missing-asset", "%s is served but empty", ref)
			}
		}
	}

	sort.Slice(problems, func(i, j int) bool {
		if problems[i].Kind != problems[j].Kind {
			return problems[i].Kind < problems[j].Kind
		}
		return problems[i].Detail < problems[j].Detail
	})
	return problems, nil
}

// hasInlineScript reports whether html contains a <script> element that has no
// src attribute and is not empty — the form a strict script-src drops.
func hasInlineScript(html string) bool {
	for _, m := range reScriptOpen.FindAllStringSubmatch(html, -1) {
		attrs, firstChar := m[1], m[2]
		if firstChar == "" || firstChar == "<" {
			continue // empty element, or immediately closed
		}
		if reSrcAttr.MatchString(attrs) {
			continue // external script, unaffected by 'unsafe-inline'
		}
		return true
	}
	return false
}

// pageAssets returns every stylesheet href and script src referenced by html.
func pageAssets(html string) []string {
	var refs []string
	for _, m := range reLinkHref.FindAllStringSubmatch(html, -1) {
		// Only stylesheets; icons and canonical URLs are not assets that
		// breaking would make the page unusable.
		if reRelStyle.MatchString(m[0]) {
			refs = append(refs, m[1])
		}
	}
	for _, m := range reScriptSrc.FindAllStringSubmatch(html, -1) {
		refs = append(refs, m[1])
	}
	return refs
}

func refHost(ref string) string {
	trimmed := strings.TrimPrefix(strings.TrimPrefix(ref, "https://"), "http://")
	trimmed = strings.TrimPrefix(trimmed, "//")
	if i := strings.IndexAny(trimmed, "/?#"); i >= 0 {
		trimmed = trimmed[:i]
	}
	return trimmed
}

// cspAllowsInline reports whether directive in csp permits inline content.
// An absent CSP, or a CSP without the directive and without a default-src
// fallback, imposes no restriction.
func cspAllowsInline(csp, directive string) bool {
	if strings.TrimSpace(csp) == "" {
		return true
	}
	values, ok := cspDirective(csp, directive)
	if !ok {
		if values, ok = cspDirective(csp, "default-src"); !ok {
			return true
		}
	}
	return strings.Contains(values, "'unsafe-inline'")
}

func cspDirective(csp, name string) (string, bool) {
	for _, part := range strings.Split(csp, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Fields(part)
		if strings.EqualFold(fields[0], name) {
			return strings.Join(fields[1:], " "), true
		}
	}
	return "", false
}
