// Binding parameters into SQL text, and rendering a driver.Value as a literal.
package driver

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Placeholder Binding (einfach/sicher)
func bindPlaceholders(sqlStr string, args []driver.NamedValue) (string, error) {
	// Precompute literal strings for all args to avoid repeated formatting.
	lits := make([]string, len(args))
	for i := range args {
		lits[i] = sqlLiteral(args[i].Value)
	}
	used := make([]bool, len(lits))

	var sb strings.Builder
	sb.Grow(len(sqlStr) + len(lits)*8)
	argi := 0
	n := len(sqlStr)
	for i := 0; i < n; i++ {
		ch := sqlStr[i]
		// Copy quoted strings verbatim (single-quoted SQL literals)
		if ch == '\'' {
			sb.WriteByte(ch)
			i++
			for i < n {
				b := sqlStr[i]
				sb.WriteByte(b)
				if b == '\'' {
					// handle doubled single-quote escape inside SQL literal
					if i+1 < n && sqlStr[i+1] == '\'' {
						i++
						sb.WriteByte(sqlStr[i])
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}

		// Sequential placeholder '?'
		if ch == '?' {
			if argi >= len(lits) {
				return "", fmt.Errorf("not enough args for placeholders")
			}
			sb.WriteString(lits[argi])
			used[argi] = true
			argi++
			continue
		}

		// Numbered placeholders: $1, $2 or :1, :2 (1-based)
		if (ch == '$' || ch == ':') && i+1 < n {
			j := i + 1
			num := 0
			const maxInt = int(^uint(0) >> 1)
			for j < n {
				c := sqlStr[j]
				if c < '0' || c > '9' {
					break
				}
				d := int(c - '0')
				if num > (maxInt-d)/10 {
					return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, sqlStr[i+1:j+1])
				}
				num = num*10 + d
				j++
			}
			if j > i+1 {
				if num <= 0 || num > len(lits) {
					return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, sqlStr[i+1:j])
				}
				sb.WriteString(lits[num-1])
				used[num-1] = true
				i = j - 1
				continue
			}
		}

		sb.WriteByte(ch)
	}

	// Ensure every provided arg was used by at least one placeholder.
	for i := range used {
		if !used[i] {
			return "", fmt.Errorf("too many args for placeholders: arg %d unused", i+1)
		}
	}
	return sb.String(), nil
}

// sqlLiteral converts a Go value into a SQL literal string suitable for
// substitution in a query.
func sqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		// 'f' (not 'g') so small/large magnitudes never render in scientific
		// notation (e.g. 1e-05), which the SQL lexer cannot tokenize.
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case string:
		// escape single quotes by doubling them
		s := strings.ReplaceAll(x, "'", "''")
		return "'" + s + "'"
	case []byte:
		return "X'" + hex.EncodeToString(x) + "'"
	default:
		// Fallback: attempt JSON marshal (handles slices/maps)
		b, err := json.Marshal(x)
		if err != nil {
			// On marshal error, fall back to fmt.Sprintf representation
			s := strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''")
			return "'" + s + "'"
		}
		s := strings.ReplaceAll(string(b), "'", "''")
		return "'" + s + "'"
	}
}
