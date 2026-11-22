package engine

import "strings"

func (re *cachedRegexp) extractText(text string) any {
	if literal, ok := re.literal.(string); ok {
		if strings.Contains(text, literal) {
			return re.literal
		}
		return nil
	}
	re.prepareMatcher()
	if fixed := re.fixed; fixed != nil {
		if at := fixed.index(text); at >= 0 {
			return text[at : at+fixed.width]
		}
		return nil
	}
	if match := re.re.FindString(text); match != "" {
		return match
	}
	return nil
}

func (re *cachedRegexp) replaceText(original any, text, replacement string) any {
	prefix, _ := re.re.LiteralPrefix()
	literal, plain := re.literal.(string)
	literalReplacement := strings.IndexByte(replacement, '$') < 0
	var result string
	switch {
	case plain && literalReplacement:
		result = strings.ReplaceAll(text, literal, replacement)
	case prefix != "" && !strings.Contains(text, prefix):
		// The engine would return a copy of the input on a miss.
		result = text
	case literalReplacement:
		re.prepareMatcher()
		if fixed := re.fixed; fixed != nil {
			result = fixed.replaceText(text, replacement)
		} else {
			result = re.re.ReplaceAllLiteralString(text, replacement)
		}
	default:
		result = re.re.ReplaceAllString(text, replacement)
	}
	if _, ok := original.(string); ok && result == text {
		return original
	}
	return result
}

func (re *fixedASCIIRegexp) replaceText(text, replacement string) string {
	at := re.index(text)
	if at < 0 {
		return text
	}
	if re.start || re.end {
		return text[:at] + replacement + text[at+re.width:]
	}
	var result strings.Builder
	if len(text) <= 4096 || len(replacement) >= re.width {
		result.Grow(len(text))
	}
	for at >= 0 {
		result.WriteString(text[:at])
		result.WriteString(replacement)
		text = text[at+re.width:]
		at = re.index(text)
	}
	result.WriteString(text)
	return result.String()
}
