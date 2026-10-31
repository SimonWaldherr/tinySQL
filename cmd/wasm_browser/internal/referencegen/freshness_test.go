package referencegen

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The browser reference is generated from docs/reference/functions.sql and checked in, so it
// goes stale silently whenever docs/reference/functions.sql gains examples and nobody re-runs
// the generator. It had been stale for weeks by the time the geo functions
// landed: none of ST_GEOMFROMTEXT, GEO_GEOHASH_ENCODE, ST_TRANSFORM or
// ROUTE_SHORTEST_PATH appeared in the playground's reference.
//
// The existing generator test only proves the tool writes a file for dummy
// input, which cannot catch that.
func TestCheckedInReferenceMatchesFunctionsSQL(t *testing.T) {
	root, err := FindRepoRoot(".")
	if err != nil {
		t.Fatalf("locate repository root: %v", err)
	}

	src, err := os.Open(filepath.Join(root, filepath.FromSlash(SourcePath)))
	if err != nil {
		t.Fatalf("open docs/reference/functions.sql: %v", err)
	}
	defer src.Close()

	sections, err := Generate(src)
	if err != nil {
		t.Fatalf("generate from docs/reference/functions.sql: %v", err)
	}
	var want bytes.Buffer
	if err := Encode(&want, sections); err != nil {
		t.Fatalf("encode sections: %v", err)
	}

	generatedPath := filepath.Join(root, "cmd", "wasm_browser", "web", "function_examples.json")
	got, err := os.ReadFile(generatedPath)
	if err != nil {
		t.Fatalf("read %s: %v", generatedPath, err)
	}

	// The file is written with LF; a checkout with autocrlf rewrites it.
	normalize := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n")) }
	if !bytes.Equal(normalize(got), normalize(want.Bytes())) {
		t.Errorf("cmd/wasm_browser/web/function_examples.json is out of date with docs/reference/functions.sql.\n"+
			"Regenerate it with:\n  go run ./cmd/wasm_browser/tools/gen_reference.go\n"+
			"(checked-in %d bytes, regenerated %d bytes)", len(got), want.Len())
	}
}

func TestCheckedInHTMLMatchesFunctionsSQL(t *testing.T) {
	root, err := FindRepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(SourcePath)))
	if err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if err := RenderHTML(&want, source); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "docs", "reference", "functions.html"))
	if err != nil {
		t.Fatal(err)
	}
	normalize := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n")) }
	if !bytes.Equal(normalize(got), normalize(want.Bytes())) {
		t.Fatal("HTML reference is stale; run make reference")
	}
}

func TestLegacySQLPathsMatchCanonicalSources(t *testing.T) {
	root, err := FindRepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	for legacy, canonical := range map[string]string{
		"FUNCTIONS.sql":        SourcePath,
		"example_showcase.sql": "demos/sql/showcase.sql",
	} {
		got, err := os.ReadFile(filepath.Join(root, legacy))
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(canonical)))
		if err != nil {
			t.Fatal(err)
		}
		normalize := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n")) }
		if !bytes.Equal(normalize(got), normalize(want)) {
			t.Errorf("%s is stale; run make reference", legacy)
		}
	}
}
