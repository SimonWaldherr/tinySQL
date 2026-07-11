// Package referencegen extracts the ordered example overview used by the
// browser-based function reference.
package referencegen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// SectionExamples contains the SQL example blocks that belong to one
// top-level section in FUNCTIONS.sql.
type SectionExamples struct {
	Section  string   `json:"section"`
	Examples []string `json:"examples"`
}

// Generate extracts SQL example blocks from FUNCTIONS.sql. A section is only
// started by a title enclosed by the decorative comment rules used in that
// file. This deliberately ignores ordinary comments such as "COUNT" or
// "Example 1", which are descriptions within the current section rather than
// categories of their own.
func Generate(r io.Reader) ([]SectionExamples, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var (
		sections      []SectionExamples
		current       = -1
		buffer        []string
		awaitingTitle bool
		pendingTitle  string
	)

	startSection := func(title string) {
		sections = append(sections, SectionExamples{Section: title})
		current = len(sections) - 1
	}
	commitPendingSection := func() {
		if pendingTitle == "" {
			return
		}
		startSection(pendingTitle)
		pendingTitle = ""
		awaitingTitle = false
	}
	pushBuffer := func() {
		text := strings.TrimSpace(strings.Join(buffer, "\n"))
		buffer = buffer[:0]
		if text == "" {
			return
		}
		if current < 0 {
			startSection("General")
		}
		sections[current].Examples = append(sections[current].Examples, text)
	}

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if isSectionRule(trimmed) {
			pushBuffer()
			if pendingTitle != "" {
				commitPendingSection()
			} else {
				awaitingTitle = true
			}
			continue
		}

		// A few legacy headings omit the closing rule. Treat the first line
		// after such a title as the start of its section instead of folding it
		// into the preceding category.
		commitPendingSection()

		if strings.HasPrefix(trimmed, "--") {
			title := strings.TrimSpace(strings.TrimPrefix(trimmed, "--"))
			if awaitingTitle && title != "" {
				pendingTitle = title
				awaitingTitle = false
			}
			continue
		}

		if trimmed == "" {
			pushBuffer()
			continue
		}

		// A divider without a following title is not a header.
		awaitingTitle = false
		pendingTitle = ""
		buffer = append(buffer, line)
	}
	pushBuffer()
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Ignore empty terminal headings such as "END OF EXAMPLES".
	out := sections[:0]
	for _, section := range sections {
		if len(section.Examples) > 0 {
			out = append(out, section)
		}
	}
	return out, nil
}

// Encode writes the generated overview in the format consumed by the web UI.
func Encode(w io.Writer, sections []SectionExamples) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(sections)
}

// FindRepoRoot returns the nearest parent directory that contains
// FUNCTIONS.sql. It lets both generator entry points work from any directory.
func FindRepoRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolve start directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "FUNCTIONS.sql")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("FUNCTIONS.sql not found above %s", start)
		}
		dir = parent
	}
}

func isSectionRule(line string) bool {
	if !strings.HasPrefix(line, "--") {
		return false
	}
	rule := strings.TrimSpace(strings.TrimPrefix(line, "--"))
	return len(rule) >= 10 && strings.Trim(rule, "=") == ""
}
