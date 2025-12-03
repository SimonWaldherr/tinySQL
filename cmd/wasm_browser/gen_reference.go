//go:build ignore
// +build ignore

package main

// gen_reference.go
// Simple generator to extract example SQL blocks from FUNCTIONS.sql and
// emit a JSON file consumed by the web reference.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type SectionExamples struct {
	Section  string   `json:"section"`
	Examples []string `json:"examples"`
}

func main() {
	root := filepath.Join("..", "..", "..") // repo root relative to cmd/wasm_browser
	src := filepath.Join(root, "FUNCTIONS.sql")
	outDir := filepath.Join("web")
	outFile := filepath.Join(outDir, "function_examples.json")

	f, err := os.Open(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", src, err)
		os.Exit(2)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	currentSection := "General"
	var buffer []string
	sections := make(map[string][]string)

	isSectionHeader := func(line string) (string, bool) {
		// Detect lines like: -- ============================================================
		// or: -- WINDOW FUNCTIONS - FULLY IMPLEMENTED
		s := strings.TrimSpace(line)
		if !strings.HasPrefix(s, "--") {
			return "", false
		}
		// remove leading '--'
		s = strings.TrimSpace(strings.TrimPrefix(s, "--"))
		if s == "" {
			return "", false
		}
		// treat uppercase headings or lines containing 'EXAMPLE' as section headers
		upper := strings.ToUpper(s)
		if strings.Contains(upper, "EXAMPLE") || strings.Contains(upper, "FUNCTIONS") || upper == s {
			// collapse repeated '=' lines into no-op
			if strings.Count(s, "=") > 10 {
				return "", false
			}
			return s, true
		}
		return "", false
	}

	pushBuffer := func() {
		if len(buffer) == 0 {
			return
		}
		// trim trailing blank lines
		// join and trim
		text := strings.TrimSpace(strings.Join(buffer, "\n"))
		if text != "" {
			sections[currentSection] = append(sections[currentSection], text)
		}
		buffer = buffer[:0]
	}

	for scanner.Scan() {
		line := scanner.Text()
		if sec, ok := isSectionHeader(line); ok {
			// push any pending buffer to previous section
			pushBuffer()
			// set current section
			currentSection = sec
			continue
		}

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			// skip comment-only lines
			continue
		}

		if trimmed == "" {
			// blank line -> end of block
			pushBuffer()
			continue
		}

		// add to buffer
		buffer = append(buffer, line)
	}
	// push leftover
	pushBuffer()

	// convert to ordered slice
	var out []SectionExamples
	for name, ex := range sections {
		out = append(out, SectionExamples{Section: name, Examples: ex})
	}

	if err := scanner.Err(); err != nil {
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

	enc := json.NewEncoder(of)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write json: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("wrote %s with %d sections\n", outFile, len(out))
}
