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

// arrayIndex resolves a path part to a slice index for a []any container.
// Bracket syntax ("items[0]") already sets p.idx >= 0 explicitly. A bare dot
// segment ("items.0") always parses to {key: "0", idx: -1} -- parseJSONPath
// has no way to know ahead of time whether "0" will land on a map or an
// array -- so once the segment lands on a []any (which has no other way to
// be addressed than by position), it is resolved here by parsing p.key as a
// decimal index. ok is false when neither source yields a valid index.
func arrayIndex(p pathPart) (int, bool) {
	if p.idx >= 0 {
		return p.idx, true
	}
	n, err := strconv.Atoi(p.key)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
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
			idx, ok := arrayIndex(p)
			if !ok || idx >= len(c) {
				return nil
			}
			cur = c[idx]
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
				if nextIdx, ok := arrayIndex(parts[i+1]); ok {
					c[p.key] = make([]any, nextIdx+1)
				} else {
					c[p.key] = make(map[string]any)
				}
			}
			cur = c[p.key]
		case []any:
			idx, ok := arrayIndex(p)
			if !ok || idx >= len(c) {
				return v
			}
			if c[idx] == nil {
				// Create next level structure
				if nextIdx, ok := arrayIndex(parts[i+1]); ok {
					c[idx] = make([]any, nextIdx+1)
				} else {
					c[idx] = make(map[string]any)
				}
			}
			cur = c[idx]
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
		idx, ok := arrayIndex(lastPart)
		if !ok {
			return v // Invalid: trying to use a non-numeric key on array
		}
		// Extend array if needed
		for len(c) <= idx {
			c = append(c, nil)
		}
		c[idx] = value
		// Update the reference in the parent
		if len(parts) > 1 {
			parentParts := parts[:len(parts)-1]
			parent := v
			for _, p := range parentParts[:len(parentParts)-1] {
				switch pc := parent.(type) {
				case map[string]any:
					parent = pc[p.key]
				case []any:
					if pidx, ok := arrayIndex(p); ok && pidx < len(pc) {
						parent = pc[pidx]
					}
				}
			}
			lastParentPart := parentParts[len(parentParts)-1]
			switch pc := parent.(type) {
			case map[string]any:
				pc[lastParentPart.key] = c
			case []any:
				if pidx, ok := arrayIndex(lastParentPart); ok && pidx < len(pc) {
					pc[pidx] = c
				}
			}
		} else {
			v = c
		}
	default:
		return v
	}

	return v
}
