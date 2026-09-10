package engine

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// compileLikeStringMatcher binds the pattern once. Literal runs separated by
// % use string search instead of restarting the rune matcher at every position.
// Fixed-width _ patterns decode only their wildcard positions. Escaped literals
// share the string-search path; combinations of _ with % retain the rune matcher.
func compileLikeStringMatcher(pattern string, insensitive bool) func(string) bool {
	pat := pattern
	if insensitive {
		pat = strings.ToLower(pat)
	}
	var match func(string) bool
	switch {
	case strings.Contains(pat, "_") && !strings.ContainsAny(pat, "%\\") && utf8.ValidString(pat) && !strings.ContainsRune(pat, utf8.RuneError):
		parts := strings.Split(pat, "_")
		match = func(s string) bool {
			for _, part := range parts[:len(parts)-1] {
				if !strings.HasPrefix(s, part) {
					return false
				}
				s = s[len(part):]
				if len(s) == 0 {
					return false
				}
				_, width := utf8.DecodeRuneInString(s)
				s = s[width:]
			}
			return s == parts[len(parts)-1]
		}

	case !utf8.ValidString(pat) || strings.ContainsRune(pat, utf8.RuneError):
		// RuneError can also match invalid input bytes in the general rune matcher;
		// byte-based literal search cannot express that equivalence.
		match = func(s string) bool { return matchLikePattern(s, pat, '\\') }
	default:
		if parts, ok := likeLiteralParts(pat); ok {
			// Compare only the needed prefix, lowering one code point at a time.
			// This avoids allocating a lowercase copy of every log/message body.
			if insensitive && (len(parts) == 1 || len(parts) == 2 && parts[1] == "") {
				prefix, exact := parts[0], len(parts) == 1
				return func(s string) bool {
					n := lowerPrefixLen(s, prefix)
					return n >= 0 && (!exact || n == len(s))
				}
			}
			match = compileLikeLiteralParts(parts)
		} else {
			match = func(s string) bool { return matchLikePattern(s, pat, '\\') }
		}
	}
	if insensitive {
		return func(s string) bool { return match(strings.ToLower(s)) }
	}
	return match
}

// likeLiteralParts splits on unescaped %, retaining empty anchors. A trailing
// backslash is literal, matching matchLikePattern. Unescaped _ needs rune work.
func likeLiteralParts(pattern string) ([]string, bool) {
	if !strings.ContainsAny(pattern, "_\\") {
		return strings.Split(pattern, "%"), true
	}
	var parts []string
	var literal strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '_':
			return nil, false
		case '%':
			parts = append(parts, literal.String())
			literal.Reset()
		case '\\':
			if i+1 < len(pattern) {
				i++
			}
			literal.WriteByte(pattern[i])
		default:
			literal.WriteByte(pattern[i])
		}
	}
	return append(parts, literal.String()), true
}

func compileLikeLiteralParts(parts []string) func(string) bool {
	if len(parts) == 1 {
		return func(s string) bool { return s == parts[0] }
	}
	prefix, suffix := parts[0], parts[len(parts)-1]
	middle := make([]string, 0, len(parts)-2)
	for _, part := range parts[1 : len(parts)-1] {
		if part != "" {
			middle = append(middle, part)
		}
	}
	if len(middle) == 0 {
		if suffix == "" {
			return func(s string) bool { return strings.HasPrefix(s, prefix) }
		}
		if prefix == "" {
			return func(s string) bool { return strings.HasSuffix(s, suffix) }
		}
	}
	if prefix == "" && suffix == "" && len(middle) == 1 {
		return func(s string) bool { return strings.Contains(s, middle[0]) }
	}
	return func(s string) bool {
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

// lowerPrefixLen returns the consumed source bytes, or -1 for a mismatch.
// Use Unicode lowercasing, not EqualFold: ILIKE's established semantics lower
// both operands (so e.g. final sigma and ordinary sigma remain distinct).
func lowerPrefixLen(s, prefix string) int {
	i, j := 0, 0
	for j < len(prefix) {
		if i == len(s) {
			return -1
		}
		c := s[i]
		if c < utf8.RuneSelf && prefix[j] < utf8.RuneSelf {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != prefix[j] {
				return -1
			}
			i++
			j++
			continue
		}
		r, width := utf8.DecodeRuneInString(s[i:])
		p, pw := utf8.DecodeRuneInString(prefix[j:])
		if unicode.ToLower(r) != p {
			return -1
		}
		i += width
		j += pw
	}
	return i
}
