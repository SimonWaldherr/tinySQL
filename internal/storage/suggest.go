package storage

import "strings"

// suggestSimilar returns the candidate most similar to name under a
// case-insensitive Levenshtein edit distance, or "" if none are close enough
// to plausibly be a typo of name (as opposed to an unrelated identifier).
// This backs "did you mean ...?" hints on lookup errors (e.g. no such
// table) — a plain string-distance heuristic, not an AI/NLP feature.
func suggestSimilar(name string, candidates []string) string {
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
		d := levenshteinDistance(lname, lc)
		if best == "" || d < bestDist || (d == bestDist && suggestLess(c, best)) {
			bestDist = d
			best = c
		}
	}
	if best == "" || bestDist > maxTypoDistance(lname) {
		return ""
	}
	return best
}

// suggestLess breaks distance ties deterministically (shorter, then
// lexicographically first) so the suggestion doesn't depend on map
// iteration order.
func suggestLess(a, b string) bool {
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

// levenshteinDistance computes the edit distance between two strings using
// the standard dynamic-programming algorithm with a single rolling row.
func levenshteinDistance(s, t string) int {
	rs := []rune(s)
	rt := []rune(t)
	m, n := len(rs), len(rt)
	if m < n {
		rs, rt = rt, rs
		m, n = n, m
	}
	dp := make([]int, n+1)
	for j := range dp {
		dp[j] = j
	}
	for i := 1; i <= m; i++ {
		prev := i
		for j := 1; j <= n; j++ {
			cost := 1
			if rs[i-1] == rt[j-1] {
				cost = 0
			}
			del := dp[j] + 1
			ins := prev + 1
			sub := dp[j-1] + cost
			next := del
			if ins < next {
				next = ins
			}
			if sub < next {
				next = sub
			}
			dp[j-1] = prev
			prev = next
		}
		dp[n] = prev
	}
	return dp[n]
}
