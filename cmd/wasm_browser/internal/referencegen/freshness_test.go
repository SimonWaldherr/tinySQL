package referencegen

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The browser reference is generated from FUNCTIONS.sql and checked in, so it
// goes stale silently whenever FUNCTIONS.sql gains examples and nobody re-runs
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

	src, err := os.Open(filepath.Join(root, "FUNCTIONS.sql"))
	if err != nil {
		t.Fatalf("open FUNCTIONS.sql: %v", err)
	}
	defer src.Close()

	sections, err := Generate(src)
	if err != nil {
		t.Fatalf("generate from FUNCTIONS.sql: %v", err)
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
		t.Errorf("cmd/wasm_browser/web/function_examples.json is out of date with FUNCTIONS.sql.\n"+
			"Regenerate it with:\n  go run ./cmd/wasm_browser/tools/gen_reference.go\n"+
			"(checked-in %d bytes, regenerated %d bytes)", len(got), want.Len())
	}
}
