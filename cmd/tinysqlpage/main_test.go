package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFrontMatter(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "page.sql")
	content := "-- nav_label: Overview\n-- nav_order: 3\n-- nav_hidden: true\nSELECT 1;\n"
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	m := parseFrontMatter(p)
	if m["nav_label"] != "Overview" {
		t.Fatalf("expected nav_label Overview, got %q", m["nav_label"])
	}
	if m["nav_order"] != "3" {
		t.Fatalf("expected nav_order 3, got %q", m["nav_order"])
	}
	if m["nav_hidden"] != "true" {
		t.Fatalf("expected nav_hidden true, got %q", m["nav_hidden"])
	}
}

func TestBuildNavHTML(t *testing.T) {
	// create temp pages dir with a few .sql files and front-matter
	d := t.TempDir()
	files := map[string]string{
		"index.sql":  "-- title: Home\nSELECT 1;\n",
		"about.sql":  "-- nav_label: About Us\n-- nav_order: 5\nSELECT 1;\n",
		"hidden.sql": "-- nav_label: Secret\n-- nav_hidden: true\nSELECT 1;\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(d, name), []byte(content), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	h := &pageHandler{pagesDir: d}
	nav := h.buildNavHTML("about")
	if !strings.Contains(nav, "About Us") {
		t.Fatalf("nav missing About Us: %s", nav)
	}
	if !strings.Contains(nav, "href=\"/\"") {
		t.Fatalf("nav missing index link: %s", nav)
	}
	// ensure hidden page not present
	if strings.Contains(nav, "Secret") {
		t.Fatalf("hidden page leaked into nav: %s", nav)
	}
}
