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
// DISTINCT's typed keys. Integer-only inputs can use exact integer keys without
// formatting every row. The first other non-NULL type migrates those keys to
// text, preserving equivalences such as int(1), int64(1), 1.0 and "1".
type distinctCountSet struct {
	integers map[int64]struct{}
	seen     map[string]struct{}
	buf      []byte
}

func (s *distinctCountSet) add(v any) bool {
	if v == nil {
		return false
	}
	if s.seen == nil {
		var integer int64
		integerValue := true
		switch x := v.(type) {
		case int:
			integer = int64(x)
		case int64:
			integer = x
		default:
			integerValue = false
		}
		if integerValue {
			if _, exists := s.integers[integer]; exists {
				return false
			}
			if s.integers == nil {
				s.integers = make(map[int64]struct{})
			}
			s.integers[integer] = struct{}{}
			return true
		}
		s.seen = make(map[string]struct{}, len(s.integers)+1)
		for integer := range s.integers {
			s.seen[strconv.FormatInt(integer, 10)] = struct{}{}
		}
		s.integers = nil
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
