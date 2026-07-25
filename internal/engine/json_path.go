// JSON path evaluation for JSON_GET and JSON_SET.
package engine

import (
	"strconv"
)

type pathPart struct {
	key string
	idx int
}

func parseJSONPath(s string) []pathPart {
	var out []pathPart
	cur := ""
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
		case '[':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
			j := i + 1
			for j < len(s) && s[j] != ']' {
				j++
			}
			if j <= len(s)-1 {
				n, _ := strconv.Atoi(s[i+1 : j])
				out = append(out, pathPart{idx: n})
				i = j
			}
		default:
			cur += string(s[i])
		}
	}
	if cur != "" {
		out = append(out, pathPart{key: cur, idx: -1})
	}
	return out
}

func jsonGet(v any, path string) any {
	if v == nil || path == "" {
		return nil
	}
	parts := parseJSONPath(path)
	cur := v
	for _, p := range parts {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[p.key]
		case []any:
			if p.idx >= 0 && p.idx < len(c) {
				cur = c[p.idx]
			} else {
				return nil
			}
		default:
			return nil
		}
	}
	return cur
}

//nolint:gocyclo // JSON setter walks paths with mixed map/array handling.
func jsonSet(v any, path string, value any) any {
	if path == "" {
		return value
	}

	parts := parseJSONPath(path)
	if len(parts) == 0 {
		return value
	}

	// If v is nil, create a new structure
	if v == nil {
		if parts[0].idx >= 0 {
			v = make([]any, parts[0].idx+1)
		} else {
			v = make(map[string]any)
		}
	}

	// Navigate to the parent of the target
	cur := v
	for i := 0; i < len(parts)-1; i++ {
		p := parts[i]
		switch c := cur.(type) {
		case map[string]any:
			if p.idx >= 0 {
				// This should be an array access, but we have a map
				return v
			}
			if c[p.key] == nil {
				// Create next level structure
				if parts[i+1].idx >= 0 {
					c[p.key] = make([]any, parts[i+1].idx+1)
				} else {
					c[p.key] = make(map[string]any)
				}
			}
			cur = c[p.key]
		case []any:
			if p.idx < 0 || p.idx >= len(c) {
				return v
			}
			if c[p.idx] == nil {
				// Create next level structure
				if parts[i+1].idx >= 0 {
					c[p.idx] = make([]any, parts[i+1].idx+1)
				} else {
					c[p.idx] = make(map[string]any)
				}
			}
			cur = c[p.idx]
		default:
			return v
		}
	}

	// Set the final value
	lastPart := parts[len(parts)-1]
	switch c := cur.(type) {
	case map[string]any:
		if lastPart.idx >= 0 {
			return v // Invalid: trying to use array index on map
		}
		c[lastPart.key] = value
	case []any:
		if lastPart.idx < 0 {
			return v // Invalid: trying to use string key on array
		}
		// Extend array if needed
		for len(c) <= lastPart.idx {
			c = append(c, nil)
		}
		c[lastPart.idx] = value
		// Update the reference in the parent
		if len(parts) > 1 {
			parentParts := parts[:len(parts)-1]
			parent := v
			for _, p := range parentParts[:len(parentParts)-1] {
				switch pc := parent.(type) {
				case map[string]any:
					parent = pc[p.key]
				case []any:
					parent = pc[p.idx]
				}
			}
			lastParentPart := parentParts[len(parentParts)-1]
			switch pc := parent.(type) {
			case map[string]any:
				pc[lastParentPart.key] = c
			case []any:
				pc[lastParentPart.idx] = c
			}
		} else {
			v = c
		}
	default:
		return v
	}

	return v
}
