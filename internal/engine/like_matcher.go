package engine

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// compileLikeStringMatcher binds the pattern once. Literal runs separated by
// % use string search instead of restarting the rune matcher at every position.
// Fixed-width _ patterns decode only their wildcard positions. Mixed patterns
// check their anchored ends before using the rune matcher for the middle.
func compileLikeStringMatcher(pattern string, insensitive bool) func(string) bool {
	pat := pattern
	if insensitive {
		pat = strings.ToLower(pat)
	}
	var match func(string) bool
	switch {
	case strings.Contains(pat, "_") && !strings.ContainsAny(pat, "%\\") && utf8.ValidString(pat) && !strings.ContainsRune(pat, utf8.RuneError):
		parts := strings.Split(pat, "_")
		return func(s string) bool {
			return matchLikePrefix(s, parts, insensitive) == len(s)
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
			if insensitive && len(parts) == 2 && parts[0] == "" {
				return func(s string) bool { return lowerSuffixStart(s, parts[1]) >= 0 }
			}
			match = compileLikeLiteralParts(parts)
		} else {
			if anchored := compileLikeAnchoredMatcher(pat, insensitive); anchored != nil {
				return anchored
			}
			match = func(s string) bool { return matchLikePattern(s, pat, '\\') }
		}
	}
	if insensitive {
		return func(s string) bool { return match(strings.ToLower(s)) }
	}
	return match
}

// The parts before the first and after the last unescaped % have fixed rune
// widths, even when they contain _. Their positions are therefore known:
// checking a long suffix at every possible input position only repeats work.
// The caller excludes invalid UTF-8 and RuneError literals, whose matching
// semantics require decoding input rather than comparing literal bytes.
func compileLikeAnchoredMatcher(pattern string, insensitive bool) func(string) bool {
	first, last := -1, -1
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			i++
		case '%':
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 || first == 0 && last == len(pattern)-1 {
		return nil
	}
	prefix := likeAnchorParts(pattern[:first])
	suffix := likeAnchorParts(pattern[last+1:])
	middle := pattern[first : last+1]
	return func(s string) bool {
		start := matchLikePrefix(s, prefix, insensitive)
		if start < 0 {
			return false
		}
		// Restrict the suffix to the remainder so the anchors cannot overlap.
		s = s[start:]
		end := matchLikeSuffix(s, suffix, insensitive)
		if end < 0 {
			return false
		}
		if first == last {
			return true
		}
		s = s[:end]
		if insensitive {
			s = strings.ToLower(s)
		}
		return matchLikePattern(s, middle, '\\')
	}
}

// Split a %-free anchor into literal runs separated by unescaped _. Empty
// runs retain consecutive wildcards; a trailing backslash stays literal.
func likeAnchorParts(pattern string) []string {
	if !strings.ContainsRune(pattern, '\\') {
		return strings.Split(pattern, "_")
	}
	var parts []string
	var literal strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '_':
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
	return append(parts, literal.String())
}

func matchLikePrefix(s string, parts []string, insensitive bool) int {
	pos := 0
	for i, part := range parts {
		width := len(part)
		if insensitive {
			width = lowerPrefixLen(s[pos:], part)
			if width < 0 {
				return -1
			}
		} else if !strings.HasPrefix(s[pos:], part) {
			return -1
		}
		pos += width
		if i+1 < len(parts) {
			if pos == len(s) {
				return -1
			}
			_, width := utf8.DecodeRuneInString(s[pos:])
			pos += width
		}
	}
	return pos
}

func matchLikeSuffix(s string, parts []string, insensitive bool) int {
	pos := len(s)
	for i := len(parts) - 1; i >= 0; i-- {
		if insensitive {
			pos = lowerSuffixStart(s[:pos], parts[i])
			if pos < 0 {
				return -1
			}
		} else {
			if !strings.HasSuffix(s[:pos], parts[i]) {
				return -1
			}
			pos -= len(parts[i])
		}
		if i > 0 {
			if pos == 0 {
				return -1
			}
			_, width := utf8.DecodeLastRuneInString(s[:pos])
			pos -= width
		}
	}
	return pos
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

// lowerSuffixStart is lowerPrefixLen's backwards counterpart. The returned
// offset belongs to the original input: Unicode lowercasing can change byte
// widths (for example İ becomes i), so subtracting len(suffix) is insufficient.
func lowerSuffixStart(s, suffix string) int {
	i, j := len(s), len(suffix)
	for j > 0 {
		if i == 0 {
			return -1
		}
		c := s[i-1]
		if c < utf8.RuneSelf && suffix[j-1] < utf8.RuneSelf {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != suffix[j-1] {
				return -1
			}
			i--
			j--
			continue
		}
		r, width := utf8.DecodeLastRuneInString(s[:i])
		p, pw := utf8.DecodeLastRuneInString(suffix[:j])
		if unicode.ToLower(r) != p {
			return -1
		}
		i -= width
		j -= pw
	}
	return i
}
