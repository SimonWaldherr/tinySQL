//go:build ignore
// +build ignore

package main

// gen_reference.go
// Simple generator to extract example SQL blocks from FUNCTIONS.sql and
// emit a JSON file consumed by the web reference.

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/SimonWaldherr/tinySQL/cmd/wasm_browser/internal/referencegen"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get working dir: %v\n", err)
		os.Exit(2)
	}
	root, err := referencegen.FindRepoRoot(cwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to find repository root: %v\n", err)
		os.Exit(2)
	}
	src := filepath.Join(root, "FUNCTIONS.sql")
	outDir := filepath.Join(root, "cmd", "wasm_browser", "web")
	outFile := filepath.Join(outDir, "function_examples.json")

	f, err := os.Open(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", src, err)
		os.Exit(2)
	}
	defer f.Close()

	sections, err := referencegen.Generate(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
		os.Exit(2)
	}

	// ensure outDir exists
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create out dir: %v\n", err)
		os.Exit(2)
	}

	of, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create %s: %v\n", outFile, err)
		os.Exit(2)
	}
	defer of.Close()

	if err := referencegen.Encode(of, sections); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write json: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("wrote %s with %d sections\n", outFile, len(sections))
}
