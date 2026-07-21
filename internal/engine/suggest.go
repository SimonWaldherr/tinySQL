package engine

import (
	"fmt"
	"strings"
)

// suggestSimilarName returns the candidate most similar to name under a
// case-insensitive Levenshtein edit distance (see levenshtein in
// extended_functions.go), or "" if none are close enough to plausibly be a
// typo of name. It backs "did you mean ...?" hints on unknown-column errors
// — a plain string-distance heuristic, not an AI/NLP feature, so it only
// runs on the already-slow error path.
func suggestSimilarName(name string, candidates []string) string {
	if name == "" || len(candidates) == 0 {
		return ""
	}
	lname := strings.ToLower(name)
	best := ""
	bestDist := -1
	seen := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		lc := strings.ToLower(c)
		if lc == lname || seen[lc] {
			continue
		}
		seen[lc] = true
		d := levenshtein(lname, lc)
		if best == "" || d < bestDist || (d == bestDist && suggestNameLess(c, best)) {
			bestDist = d
			best = c
		}
	}
	if best == "" || bestDist > maxTypoDistance(lname) {
		return ""
	}
	return best
}

// suggestNameLess breaks distance ties deterministically (shorter, then
// lexicographically first) so the suggestion doesn't depend on map
// iteration order.
func suggestNameLess(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

// maxTypoDistance bounds how many edits still count as "probably a typo"
// rather than an unrelated name, scaled to the length of the target so short
// identifiers don't match everything.
func maxTypoDistance(s string) int {
	switch {
	case len(s) <= 3:
		return 1
	case len(s) <= 6:
		return 2
	default:
		return 3
	}
}

// columnSuggestion finds a "did you mean ...?" candidate for an unresolved
// column name from one or more column-name -> index maps in scope.
func columnSuggestion(name string, indexes ...map[string]int) string {
	var names []string
	for _, idx := range indexes {
		for k := range idx {
			names = append(names, k)
		}
	}
	return suggestSimilarName(name, names)
}

// columnSuggestionFromRow finds a "did you mean ...?" candidate for an
// unresolved column name among the keys of one or more result rows in scope
// (unqualified and table/alias-qualified column names alike).
func columnSuggestionFromRow(name string, rows ...Row) string {
	var names []string
	for _, row := range rows {
		for k := range row {
			names = append(names, k)
		}
	}
	return suggestSimilarName(name, names)
}

// unknownColumnErr formats the standard "unknown column" error, appending a
// "did you mean ...?" hint when suggestion is non-empty. Qualified names
// (containing ".") keep the "unknown column reference" wording used
// elsewhere in the engine.
func unknownColumnErr(name, suggestion string) error {
	kind := "unknown column"
	if strings.Contains(name, ".") {
		kind = "unknown column reference"
	}
	if suggestion == "" {
		return fmt.Errorf("%s %q", kind, name)
	}
	return fmt.Errorf("%s %q - did you mean %q?", kind, name, suggestion)
}
