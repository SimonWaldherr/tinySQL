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
    // Find repository root by searching upward for FUNCTIONS.sql
    cwd, err := os.Getwd()
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to get working dir: %v\n", err)
        os.Exit(2)
    }

    findUp := func(start, name string) (string, error) {
        dir := start
        for {
            candidate := filepath.Join(dir, name)
            if _, err := os.Stat(candidate); err == nil {
                return dir, nil
            }
            parent := filepath.Dir(dir)
            if parent == dir {
                return "", fmt.Errorf("not found")
            }
            dir = parent
        }
    }

    repoRoot, err := findUp(cwd, "FUNCTIONS.sql")
    if err != nil {
        // fallback: assume two levels up
        repoRoot = filepath.Join(cwd, "..", "..")
    }

    src := filepath.Join(repoRoot, "FUNCTIONS.sql")
    outDir := filepath.Join(repoRoot, "cmd", "wasm_browser", "web")
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
        s := strings.TrimSpace(line)
        if !strings.HasPrefix(s, "--") {
            return "", false
        }
        s = strings.TrimSpace(strings.TrimPrefix(s, "--"))
        if s == "" {
            return "", false
        }
        upper := strings.ToUpper(s)
        if strings.Contains(upper, "EXAMPLE") || strings.Contains(upper, "FUNCTIONS") || upper == s {
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
        text := strings.TrimSpace(strings.Join(buffer, "\n"))
        if text != "" {
            sections[currentSection] = append(sections[currentSection], text)
        }
        buffer = buffer[:0]
    }

    for scanner.Scan() {
        line := scanner.Text()
        if sec, ok := isSectionHeader(line); ok {
            pushBuffer()
            currentSection = sec
            continue
        }

        trimmed := strings.TrimSpace(line)
        if strings.HasPrefix(trimmed, "--") {
            continue
        }

        if trimmed == "" {
            pushBuffer()
            continue
        }

        buffer = append(buffer, line)
    }
    pushBuffer()

    var out []SectionExamples
    for name, ex := range sections {
        out = append(out, SectionExamples{Section: name, Examples: ex})
    }

    if err := scanner.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
        os.Exit(2)
    }

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
