package engine

import (
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

// compileRegexpStringMatcher specializes boolean matching only. Analyze the
// parsed regexp, never its spelling: escaped anchors, scoped flags and groups
// must keep their Go regexp meanings. Anything outside literal runs, text
// anchors and uniform dot-stars uses the original matcher.
func compileRegexpStringMatcher(re *regexp.Regexp) func(string) bool {
	expr, err := syntax.Parse(re.String(), syntax.Perl)
	if err != nil {
		return re.MatchString
	}
	var nodes []*syntax.Regexp
	flattenRegexpConcat(expr, &nodes)
	start, end := false, false
	if len(nodes) > 0 && nodes[0].Op == syntax.OpBeginText {
		start = true
		nodes = nodes[1:]
	}
	if len(nodes) > 0 && nodes[len(nodes)-1].Op == syntax.OpEndText {
		end = true
		nodes = nodes[:len(nodes)-1]
	}
	var parts []string
	if !start {
		parts = append(parts, "")
	}
	var literal strings.Builder
	dotMode := -1 // no dot-star yet; 0 excludes newline, 1 includes it
	literalNewline := false
	for _, node := range nodes {
		switch node.Op {
		case syntax.OpLiteral:
			if node.Flags&syntax.FoldCase != 0 {
				return re.MatchString
			}
			for _, r := range node.Rune {
				if r == utf8.RuneError {
					// Go regexp also matches malformed input bytes with RuneError.
					// A byte search for its UTF-8 encoding cannot do that.
					return re.MatchString
				}
				literalNewline = literalNewline || r == '\n'
			}
			for _, r := range node.Rune {
				literal.WriteRune(r)
			}
		case syntax.OpStar:
			mode := -1
			switch node.Sub[0].Op {
			case syntax.OpAnyCharNotNL:
				mode = 0
			case syntax.OpAnyChar:
				mode = 1
			}
			if mode < 0 || dotMode >= 0 && mode != dotMode {
				return re.MatchString
			}
			dotMode = mode
			parts = append(parts, literal.String())
			literal.Reset()
		default:
			return re.MatchString
		}
	}
	parts = append(parts, literal.String())
	if !end {
		parts = append(parts, "")
	}
	if dotMode == 0 && literalNewline {
		// Literal newlines can bridge lines even when dot cannot. Leave that
		// combination to regexp instead of splitting the input incorrectly.
		return re.MatchString
	}
	match := compileLikeLiteralParts(parts)
	if dotMode != 0 {
		return match
	}
	// A dot without (?s) cannot cross newline. Search individual lines while
	// preserving text anchors: ^ restricts to the first, $ to the last line.
	return func(s string) bool {
		if start && end {
			return match(s) && !strings.Contains(s, "\n")
		}
		if start {
			line, _, _ := strings.Cut(s, "\n")
			return match(line)
		}
		if end {
			return match(s[strings.LastIndexByte(s, '\n')+1:])
		}
		for {
			line, rest, found := strings.Cut(s, "\n")
			if match(line) {
				return true
			}
			if !found {
				return false
			}
			s = rest
		}
	}
}

func flattenRegexpConcat(expr *syntax.Regexp, nodes *[]*syntax.Regexp) {
	switch expr.Op {
	case syntax.OpEmptyMatch:
	case syntax.OpCapture:
		flattenRegexpConcat(expr.Sub[0], nodes)
	case syntax.OpConcat:
		for _, sub := range expr.Sub {
			flattenRegexpConcat(sub, nodes)
		}
	default:
		*nodes = append(*nodes, expr)
	}
}
