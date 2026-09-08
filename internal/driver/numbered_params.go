package driver

import (
	"strconv"
	"strings"
)

// markerSQLForNumberedParams gives every occurrence its own literal marker,
// while recording the argument ordinal to bind there. Mixed '?' forms and
// missing ordinals retain the existing text binder's validation.
func markerSQLForNumberedParams(sqlText string) (string, []int, int, bool) {
	var out strings.Builder
	out.Grow(len(sqlText) + 32)
	var order []int
	inputCount := 0
	for i := 0; i < len(sqlText); {
		start := i
		ch := sqlText[i]
		switch {
		case ch == '\'' || ch == '"' || ch == '`':
			i++
			for i < len(sqlText) {
				if sqlText[i] == ch {
					i++
					if i < len(sqlText) && sqlText[i] == ch {
						i++
						continue
					}
					break
				}
				i++
			}
		case strings.HasPrefix(sqlText[i:], "--"):
			for i < len(sqlText) && sqlText[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sqlText[i:], "/*"):
			end := strings.Index(sqlText[i+2:], "*/")
			if end < 0 {
				return "", nil, 0, false
			}
			i += end + 4
		case ch == '?':
			return "", nil, 0, false
		case (ch == '$' || ch == ':') && i+1 < len(sqlText) && sqlText[i+1] >= '0' && sqlText[i+1] <= '9':
			i++
			ordinal := 0
			for i < len(sqlText) && sqlText[i] >= '0' && sqlText[i] <= '9' {
				digit := int(sqlText[i] - '0')
				// A valid contiguous argument set cannot have more ordinals than
				// SQL bytes. This bound also prevents overflow and huge allocations.
				if ordinal > (len(sqlText)-digit)/10 {
					return "", nil, 0, false
				}
				ordinal = ordinal*10 + digit
				i++
			}
			if ordinal == 0 || ordinal > len(sqlText) {
				return "", nil, 0, false
			}
			out.WriteByte('\'')
			out.WriteString(preparedMarkerPrefix)
			out.WriteString(strconv.Itoa(len(order)))
			out.WriteString("__'")
			order = append(order, ordinal-1)
			inputCount = max(inputCount, ordinal)
			continue
		default:
			i++
		}
		out.WriteString(sqlText[start:i])
	}
	if inputCount == 0 || inputCount > len(order) {
		return "", nil, 0, false
	}
	used := make([]bool, inputCount)
	for _, ordinal := range order {
		used[ordinal] = true
	}
	for _, present := range used {
		if !present {
			return "", nil, 0, false
		}
	}
	return out.String(), order, inputCount, true
}
