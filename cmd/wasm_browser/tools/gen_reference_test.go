package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenReferenceWritesOutput(t *testing.T) {
	dir := t.TempDir()

	// Create the canonical SQL source under a temporary repository root.
	if err := os.MkdirAll(filepath.Join(dir, "docs", "reference"), 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "docs", "reference", "functions.sql")
	if err := os.WriteFile(src, []byte("-- Example\nSELECT 1;\n"), 0o644); err != nil {
		t.Fatalf("failed to write SQL reference: %v", err)
	}

	// Run main with working dir set to temp dir
	oldWd, _ := os.Getwd()
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir failed: %v", err)
	}

	main()

	out := filepath.Join(dir, "cmd", "wasm_browser", "web", "function_examples.json")
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("expected output file %s to exist, stat error: %v", out, err)
	}
}
