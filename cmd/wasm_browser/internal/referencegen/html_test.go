package referencegen

import (
	"bytes"
	"strings"
	"testing"
)

func TestHTMLPreservesCommentsAndEscapesSQL(t *testing.T) {
	var out bytes.Buffer
	sql := []byte("-- ====================\n-- TEST SECTION\n-- ====================\n-- NULL is distinct from empty text.\nSELECT '<script>alert(1)</script>';\n")
	if err := RenderHTML(&out, sql); err != nil {
		t.Fatal(err)
	}
	html := out.String()
	for _, expected := range []string{"NULL is distinct from empty text.", "&lt;script&gt;", `href="#section-0"`, "TEST SECTION"} {
		if !strings.Contains(html, expected) {
			t.Errorf("HTML omits %q", expected)
		}
	}
	if strings.Contains(html, "<script>") {
		t.Fatal("SQL literal became an executable HTML element")
	}
}
