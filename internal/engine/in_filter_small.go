package engine

// Lists of at most four homogeneous literals avoid hashing while checking the
// probe's type once. Mixed domains retain the general sets and coercion rules.
func buildSmallInFilter(col int, values []any, negate bool) func([]any) (bool, error) {
	const (
		noDomain = iota
		integerDomain
		realDomain
		textDomain
	)
	var integers [4]int64
	var reals [4]float64
	var texts [4]string
	kind, count, hasNull := noDomain, 0, false
	for _, value := range values {
		switch value := value.(type) {
		case nil:
			hasNull = true
			continue
		case int:
			if kind != noDomain && kind != integerDomain {
				return nil
			}
			kind, integers[count] = integerDomain, int64(value)
		case int64:
			if kind != noDomain && kind != integerDomain {
				return nil
			}
			kind, integers[count] = integerDomain, value
		case float64:
			if kind != noDomain && kind != realDomain {
				return nil
			}
			kind, reals[count] = realDomain, value
		case string:
			if kind != noDomain && kind != textDomain {
				return nil
			}
			kind, texts[count] = textDomain, value
		default:
			return nil
		}
		count++
	}
	if count == 0 {
		if hasNull || !negate {
			return func([]any) (bool, error) { return false, nil }
		}
		return func(raw []any) (bool, error) { return raw[col] != nil, nil }
	}
	if negate && hasNull {
		return func([]any) (bool, error) { return false, nil }
	}
	match, miss := !negate, negate && !hasNull
	switch kind {
	case integerDomain:
		keys := integers
		return func(raw []any) (bool, error) {
			var integer int64
			switch value := raw[col].(type) {
			case nil:
				return false, nil
			case int:
				integer = int64(value)
			case int64:
				integer = value
			case float64:
				for _, key := range keys[:count] {
					if value == float64(key) {
						return match, nil
					}
				}
				return miss, nil
			default:
				return miss, nil
			}
			for _, key := range keys[:count] {
				if integer == key {
					return match, nil
				}
			}
			return miss, nil
		}
	case realDomain:
		keys := reals
		return func(raw []any) (bool, error) {
			value, ok := numericFast(raw[col])
			if !ok {
				return raw[col] != nil && miss, nil
			}
			for _, key := range keys[:count] {
				if value == key {
					return match, nil
				}
			}
			return miss, nil
		}
	default:
		keys := texts
		return func(raw []any) (bool, error) {
			value, ok := raw[col].(string)
			if !ok {
				return raw[col] != nil && miss, nil
			}
			for _, key := range keys[:count] {
				if value == key {
					return match, nil
				}
			}
			return miss, nil
		}
	}
}
