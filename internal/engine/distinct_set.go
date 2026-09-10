package engine

import "strconv"

// scalarDistinctKey preserves writeFmtKeyPart's type boundaries without
// formatting integers and booleans. Other values keep the canonical encoding.
func scalarDistinctKey(v any) (any, bool) {
	switch v.(type) {
	case nil, int, int64, bool:
		return v, true
	default:
		return nil, false
	}
}

// COUNT(DISTINCT) historically deduplicates by valueText, unlike SELECT
// DISTINCT's typed keys. Reuse a buffer but preserve that exact equivalence.
type distinctCountSet struct {
	seen map[string]struct{}
	buf  []byte
}

func (s *distinctCountSet) add(v any) bool {
	if v == nil {
		return false
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	if text, ok := v.(string); ok {
		if _, exists := s.seen[text]; exists {
			return false
		}
		s.seen[text] = struct{}{}
		return true
	}
	s.buf = s.buf[:0]
	switch x := v.(type) {
	case int:
		s.buf = strconv.AppendInt(s.buf, int64(x), 10)
	case int64:
		s.buf = strconv.AppendInt(s.buf, x, 10)
	case float64:
		s.buf = strconv.AppendFloat(s.buf, x, 'g', -1, 64)
	case bool:
		s.buf = strconv.AppendBool(s.buf, x)
	default:
		s.buf = append(s.buf, valueText(v)...)
	}
	if _, exists := s.seen[string(s.buf)]; exists {
		return false
	}
	s.seen[string(s.buf)] = struct{}{}
	return true
}
