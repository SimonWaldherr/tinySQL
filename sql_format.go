package tinysql

import "strings"

// BeautifySQL formats SQL for human-readable output without parsing or
// changing its meaning. String literals, quoted identifiers, and comments are
// preserved verbatim. Use MinifySQL when compact output is preferred.
func BeautifySQL(sql string) string {
	tokens := sqlFormatTokens(sql)
	var out strings.Builder
	atLineStart := true
	write := func(s string) {
		if !atLineStart && out.Len() > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(s)
		atLineStart = false
	}
	newline := func() {
		if out.Len() > 0 && !atLineStart {
			out.WriteByte('\n')
		}
		atLineStart = true
	}

	for _, token := range tokens {
		upper := strings.ToUpper(token)
		if sqlFormatClause[upper] {
			newline()
		}
		if len(token) >= 2 && ((token[0] == '-' && token[1] == '-') || (token[0] == '/' && token[1] == '*')) {
			newline()
			out.WriteString(token)
			atLineStart = false
			newline()
			continue
		}
		switch token {
		case ",":
			out.WriteString(",")
			atLineStart = false
			continue
		case ")", ";":
			out.WriteString(token)
			atLineStart = false
			continue
		case "(":
			if !atLineStart && out.Len() > 0 && needsSQLSpaceBeforeParen(out.String()) {
				out.WriteByte(' ')
			}
			out.WriteByte('(')
			atLineStart = false
			continue
		}
		if sqlFormatKeyword[upper] {
			token = upper
		}
		write(token)
	}
	return strings.TrimSpace(out.String())
}

// MinifySQL removes non-essential whitespace from SQL without changing string
// literals, quoted identifiers, or comments. A separating space is retained
// where adjacent tokens would otherwise merge.
func MinifySQL(sql string) string {
	tokens := sqlFormatTokens(sql)
	var out strings.Builder
	for i, token := range tokens {
		if i > 0 && strings.HasPrefix(tokens[i-1], "--") {
			out.WriteByte('\n')
		} else if i > 0 && needsSQLSeparator(tokens[i-1], token) {
			out.WriteByte(' ')
		}
		out.WriteString(token)
	}
	return strings.TrimSpace(out.String())
}

var sqlFormatKeyword = map[string]bool{
	"SELECT": true, "DISTINCT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true, "IN": true, "LIKE": true, "BETWEEN": true, "IS": true, "NULL": true,
	"ORDER": true, "BY": true, "GROUP": true, "HAVING": true, "LIMIT": true, "OFFSET": true, "JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true, "ON": true,
	"INSERT": true, "INTO": true, "VALUES": true, "UPDATE": true, "SET": true, "DELETE": true, "CREATE": true, "ALTER": true, "DROP": true, "TABLE": true, "INDEX": true, "VIEW": true,
	"WITH": true, "AS": true, "UNION": true, "ALL": true, "EXCEPT": true, "INTERSECT": true, "CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
}

var sqlFormatClause = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "GROUP": true, "HAVING": true, "ORDER": true, "LIMIT": true, "OFFSET": true, "JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true,
	"INSERT": true, "UPDATE": true, "DELETE": true, "VALUES": true, "SET": true, "UNION": true, "EXCEPT": true, "INTERSECT": true, "AND": true, "OR": true,
}

func sqlFormatTokens(sql string) []string {
	tokens := make([]string, 0, 32)
	for i := 0; i < len(sql); {
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		start := i
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			tokens = append(tokens, sql[start:i])
			continue
		}
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			i += 2
			for i+1 < len(sql) && (sql[i] != '*' || sql[i+1] != '/') {
				i++
			}
			if i+1 < len(sql) {
				i += 2
			}
			tokens = append(tokens, sql[start:i])
			continue
		}
		c = sql[i]
		if c == '\'' || c == '"' || c == '`' {
			quote := c
			i++
			for i < len(sql) {
				if sql[i] == quote {
					i++
					if quote == '\'' && i < len(sql) && sql[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			tokens = append(tokens, sql[start:i])
			continue
		}
		if c == '[' {
			i++
			for i < len(sql) && sql[i] != ']' {
				i++
			}
			if i < len(sql) {
				i++
			}
			tokens = append(tokens, sql[start:i])
			continue
		}
		if isSQLWord(c) {
			for i < len(sql) && isSQLWord(sql[i]) {
				i++
			}
			tokens = append(tokens, sql[start:i])
			continue
		}
		if i+1 < len(sql) {
			c1, c2 := sql[i], sql[i+1]
			if (c1 == '<' || c1 == '>' || c1 == '!' || c1 == '=' || c1 == '|' || c1 == '&') &&
				(c2 == '=' || c2 == '<' || c2 == '>' || c2 == '|' || c2 == '&') {
				i += 2
				continue
			}
		}
		i++
		tokens = append(tokens, sql[start:i])
	}
	return tokens
}

var sqlWordTable [256]bool

func init() {
	for i := 0; i < 256; i++ {
		b := byte(i)
		sqlWordTable[i] = b == '_' || b == '$' || b == '.' ||
			(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
	}
}

func isSQLWord(b byte) bool {
	return sqlWordTable[b]
}
func needsSQLSeparator(left, right string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	if (len(left) >= 2 && ((left[0] == '-' && left[1] == '-') || (left[0] == '/' && left[1] == '*'))) ||
		(len(right) >= 2 && ((right[0] == '-' && right[1] == '-') || (right[0] == '/' && right[1] == '*'))) {
		return true
	}
	ln := len(left) - 1
	leftWord := isSQLWord(left[ln])
	leftQuote := left[ln] == '\'' || left[ln] == '"' || left[ln] == '`' || left[ln] == ']'
	rightWord := isSQLWord(right[0])
	rightQuote := right[0] == '\'' || right[0] == '"' || right[0] == '`' || right[0] == '['
	return (leftWord && rightWord) || (leftWord && rightQuote) || (leftQuote && rightWord)
}
func needsSQLSpaceBeforeParen(output string) bool {
	return len(output) > 0 && isSQLWord(output[len(output)-1])
}
