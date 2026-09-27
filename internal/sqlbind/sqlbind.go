// Package sqlbind renders Go values as tinySQL literals and substitutes ?,
// $n and :n placeholders in SQL text. It is shared by the database/sql driver,
// the root package's ExecSQLArgs and the native language bindings, so every
// host binds values with identical quoting rules.
//
// Placeholder characters inside single-quoted strings, double-quoted or
// backtick-quoted identifiers and SQL comments are copied verbatim; they are
// never parameters.
package sqlbind

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Literal converts a Go value into a SQL literal suitable for substitution.
func Literal(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
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
		return QuoteString(x)
	case time.Time:
		// Matches the database/sql driver's argument normalization.
		return QuoteString(x.UTC().Format(time.RFC3339Nano))
	case []byte:
		var out strings.Builder
		out.Grow(3 + hex.EncodedLen(len(x)))
		out.WriteString("X'")
		// Encode bounded chunks on the stack, avoiding a full-sized hex
		// intermediate while retaining encoding/hex's tight conversion loop.
		var chunk [512]byte
		for len(x) > 0 {
			n := min(len(x), len(chunk)/2)
			hex.Encode(chunk[:], x[:n])
			out.Write(chunk[:2*n])
			x = x[n:]
		}
		out.WriteByte('\'')
		return out.String()
	default:
		// Fallback: attempt JSON marshal (handles slices/maps)
		b, err := json.Marshal(x)
		if err != nil {
			// On marshal error, fall back to fmt.Sprintf representation
			return QuoteString(fmt.Sprintf("%v", x))
		}
		return QuoteString(string(b))
	}
}

// QuoteString allocates the final single-quoted literal once, including
// doubled quotes.
func QuoteString(s string) string {
	quotes := strings.Count(s, "'")
	if quotes == 0 {
		return "'" + s + "'"
	}
	var out strings.Builder
	out.Grow(len(s) + quotes + 2)
	out.WriteByte('\'')
	for {
		i := strings.IndexByte(s, '\'')
		if i < 0 {
			break
		}
		out.WriteString(s[:i+1])
		out.WriteByte('\'')
		s = s[i+1:]
	}
	out.WriteString(s)
	out.WriteByte('\'')
	return out.String()
}

// SkipOpaque returns the index just after a quoted string, quoted identifier
// or comment that starts at sql[i]. It returns i when none starts there.
// Unterminated regions extend to the end of sql. A doubled closing quote is
// an escaped quote inside the region.
func SkipOpaque(sql string, i int) int {
	if i >= len(sql) {
		return i
	}
	switch ch := sql[i]; ch {
	case '\'', '"', '`':
		j := i + 1
		for j < len(sql) {
			if sql[j] == ch {
				if j+1 < len(sql) && sql[j+1] == ch {
					j += 2
					continue
				}
				return j + 1
			}
			j++
		}
		return len(sql)
	case '-':
		if i+1 < len(sql) && sql[i+1] == '-' {
			if end := strings.IndexByte(sql[i+2:], '\n'); end >= 0 {
				return i + 2 + end
			}
			return len(sql)
		}
	case '/':
		if i+1 < len(sql) && sql[i+1] == '*' {
			if end := strings.Index(sql[i+2:], "*/"); end >= 0 {
				return i + 2 + end + 2
			}
			return len(sql)
		}
	}
	return i
}

// Bind substitutes placeholders in sql with the prepared literals. A '?'
// consumes the next literal in order; $n and :n (1-based) reference a literal
// directly and may repeat. Every literal must be referenced at least once.
func Bind(sql string, lits []string) (string, error) {
	// SQL without parameters needs no builder or copy. Potential placeholders
	// still go through validation, including when no arguments were supplied.
	if len(lits) == 0 && !strings.ContainsAny(sql, "?$:") {
		return sql, nil
	}
	used := make([]bool, len(lits))
	litLen := 0
	for _, lit := range lits {
		litLen += len(lit)
	}
	var sb strings.Builder
	sb.Grow(len(sql) + litLen)
	argi := 0
	n := len(sql)
	for i := 0; i < n; i++ {
		if end := SkipOpaque(sql, i); end > i {
			sb.WriteString(sql[i:end])
			i = end - 1
			continue
		}
		ch := sql[i]

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
				c := sql[j]
				if c < '0' || c > '9' {
					break
				}
				d := int(c - '0')
				if num > (maxInt-d)/10 {
					return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, sql[i+1:j+1])
				}
				num = num*10 + d
				j++
			}
			if j > i+1 {
				if num <= 0 || num > len(lits) {
					return "", fmt.Errorf("tinysql: invalid placeholder %c%s", ch, sql[i+1:j])
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

// BindValues renders args with Literal and substitutes them into sql.
func BindValues(sql string, args ...any) (string, error) {
	lits := make([]string, len(args))
	for i, arg := range args {
		lits[i] = Literal(arg)
	}
	return Bind(sql, lits)
}
