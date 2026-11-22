package engine

import (
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

type regexpASCIIMask [2]uint64

func (m *regexpASCIIMask) set(c byte) {
	m[c/64] |= uint64(1) << (c % 64)
}

// A fixed-width ASCII match begins and ends on rune boundaries even in
// malformed UTF-8. Each accepted byte is a complete rune. Only accelerate
// short patterns with an anchor or a literal prefix for finding candidates.
func compileFixedASCIIRegexp(nodes []*syntax.Regexp, start, end bool) *fixedASCIIRegexp {
	const maxWidth = 64
	var prefix strings.Builder
	var masks []regexpASCIIMask
	for _, node := range nodes {
		count := 1
		if node.Op == syntax.OpRepeat {
			count = node.Min
			if count < 1 || count != node.Max || count > maxWidth {
				return nil
			}
			node = node.Sub[0]
		}
		if node.Flags&syntax.FoldCase != 0 {
			return nil
		}
		switch node.Op {
		case syntax.OpLiteral:
			if len(node.Rune) > (maxWidth-prefix.Len()-len(masks))/count {
				return nil
			}
			for range count {
				for _, r := range node.Rune {
					if r >= utf8.RuneSelf {
						return nil
					}
					if len(masks) == 0 {
						prefix.WriteByte(byte(r))
					} else {
						var mask regexpASCIIMask
						mask.set(byte(r))
						masks = append(masks, mask)
					}
				}
			}
		case syntax.OpCharClass:
			if prefix.Len()+len(masks)+count > maxWidth {
				return nil
			}
			var mask regexpASCIIMask
			for i := 0; i < len(node.Rune); i += 2 {
				lo, hi := node.Rune[i], node.Rune[i+1]
				if hi >= utf8.RuneSelf {
					return nil
				}
				for r := lo; r <= hi; r++ {
					mask.set(byte(r))
				}
			}
			for range count {
				masks = append(masks, mask)
			}
		default:
			return nil
		}
	}
	width := prefix.Len() + len(masks)
	if width == 0 || prefix.Len() == 0 && !start && !end {
		return nil
	}
	return &fixedASCIIRegexp{prefix: prefix.String(), masks: masks, width: width, start: start, end: end}
}

type fixedASCIIRegexp struct {
	prefix     string
	masks      []regexpASCIIMask
	width      int
	start, end bool
}

func (re *fixedASCIIRegexp) match(text string) bool { return re.index(text) >= 0 }

func (re *fixedASCIIRegexp) index(text string) int {
	if len(text) < re.width || re.start && re.end && len(text) != re.width {
		return -1
	}
	if re.start || re.end {
		at := 0
		if re.end {
			at = len(text) - re.width
		}
		if strings.HasPrefix(text[at:], re.prefix) && matchASCIIRegexpMasks(text[at+len(re.prefix):], re.masks) {
			return at
		}
		return -1
	}
	for from := 0; len(text)-from >= re.width; {
		advance := strings.Index(text[from:], re.prefix)
		if advance < 0 || len(text)-from-advance < re.width {
			return -1
		}
		at := from + advance
		if matchASCIIRegexpMasks(text[at+len(re.prefix):], re.masks) {
			return at
		}
		// A later candidate can overlap this occurrence of the prefix.
		from = at + 1
	}
	return -1
}

func matchASCIIRegexpMasks(s string, masks []regexpASCIIMask) bool {
	for i, mask := range masks {
		c := s[i]
		if c >= utf8.RuneSelf || mask[c/64]&(uint64(1)<<(c%64)) == 0 {
			return false
		}
	}
	return true
}
