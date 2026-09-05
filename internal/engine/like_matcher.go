package engine

import (
	"strings"
	"unicode/utf8"
)

// compileLikeStringMatcher binds the pattern once. Literal runs separated by
// % use string search instead of restarting the rune matcher at every position.
// Escape sequences and _ retain the Unicode-aware general matcher.
func compileLikeStringMatcher(pattern string, insensitive bool) func(string) bool {
	pat := pattern
	if insensitive {
		pat = strings.ToLower(pat)
	}
	var match func(string) bool
	switch {
	case strings.ContainsAny(pat, "_\\") || !utf8.ValidString(pat) || strings.ContainsRune(pat, utf8.RuneError):
		// RuneError can also match invalid input bytes in the general rune matcher;
		// byte-based literal search cannot express that equivalence.
		match = func(s string) bool { return matchLikePattern(s, pat, '\\') }
	case !strings.Contains(pat, "%"):
		match = func(s string) bool { return s == pat }
	case strings.HasSuffix(pat, "%") && !strings.Contains(pat[:len(pat)-1], "%"):
		prefix := pat[:len(pat)-1]
		match = func(s string) bool { return strings.HasPrefix(s, prefix) }
	case strings.HasPrefix(pat, "%") && !strings.Contains(pat[1:], "%"):
		suffix := pat[1:]
		match = func(s string) bool { return strings.HasSuffix(s, suffix) }
	case len(pat) >= 2 && strings.HasPrefix(pat, "%") && strings.HasSuffix(pat, "%") && !strings.Contains(pat[1:len(pat)-1], "%"):
		middle := pat[1 : len(pat)-1]
		match = func(s string) bool { return strings.Contains(s, middle) }
	default:
		parts := strings.Split(pat, "%")
		prefix, suffix := parts[0], parts[len(parts)-1]
		middle := make([]string, 0, len(parts)-2)
		for _, part := range parts[1 : len(parts)-1] {
			if part != "" {
				middle = append(middle, part)
			}
		}
		match = func(s string) bool {
			if !strings.HasPrefix(s, prefix) {
				return false
			}
			s = s[len(prefix):]
			// Reserve the anchored suffix so separate literals cannot overlap.
			if !strings.HasSuffix(s, suffix) {
				return false
			}
			s = s[:len(s)-len(suffix)]
			for _, part := range middle {
				at := strings.Index(s, part)
				if at < 0 {
					return false
				}
				s = s[at+len(part):]
			}
			return true
		}
	}
	if insensitive {
		return func(s string) bool { return match(strings.ToLower(s)) }
	}
	return match
}
