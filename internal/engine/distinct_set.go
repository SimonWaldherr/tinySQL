package engine

import (
	"math"
	"strconv"
)

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

// distinctFloatKey has the same equivalence classes as formatting with 'g',
// -1, 64: finite values and signed zeros retain their bits, while every NaN
// formats as "NaN" regardless of its sign or payload.
func distinctFloatKey(value float64) uint64 {
	if math.IsNaN(value) {
		return 0x7ff8000000000000
	}
	return math.Float64bits(value)
}

// COUNT(DISTINCT) historically deduplicates by valueText, unlike SELECT
// DISTINCT's typed keys. Homogeneous integer or float inputs avoid formatting
// every row. Integers migrate to text on a type change. Float-first inputs keep
// their numeric keys and check text equivalence only for new values.
type distinctCountSet struct {
	integers map[int64]struct{}
	floats   map[uint64]struct{}
	seen     map[string]struct{}
	buf      []byte
}

func (s *distinctCountSet) add(v any) bool {
	if v == nil {
		return false
	}
	if s.floats != nil {
		if value, ok := v.(float64); ok {
			key := distinctFloatKey(value)
			if _, exists := s.floats[key]; exists {
				return false
			}
			s.floats[key] = struct{}{}
			if s.seen == nil {
				return true
			}
			s.buf = strconv.AppendFloat(s.buf[:0], value, 'g', -1, 64)
			_, exists := s.seen[string(s.buf)]
			return !exists
		}
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
		if integerValue && s.floats == nil {
			if _, exists := s.integers[integer]; exists {
				return false
			}
			if s.integers == nil {
				s.integers = make(map[int64]struct{})
			}
			s.integers[integer] = struct{}{}
			return true
		}
		if value, ok := v.(float64); ok && s.integers == nil {
			key := distinctFloatKey(value)
			s.floats = make(map[uint64]struct{})
			s.floats[key] = struct{}{}
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
		return !s.containsFloatText(text)
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
	text := string(s.buf)
	s.seen[text] = struct{}{}
	return !s.containsFloatText(text)
}

// Only canonical float text can equal a formatted numeric key. Parsing alone
// would merge "1.00" with 1 or round large integer text to a nearby float.
func (s *distinctCountSet) containsFloatText(text string) bool {
	if s.floats == nil {
		return false
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return false
	}
	_, exists := s.floats[distinctFloatKey(value)]
	if !exists {
		return false
	}
	s.buf = strconv.AppendFloat(s.buf[:0], value, 'g', -1, 64)
	return string(s.buf) == text
}
