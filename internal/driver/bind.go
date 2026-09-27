// Binding parameters into SQL text, and rendering a driver.Value as a literal.
package driver

import (
	"database/sql/driver"

	"github.com/SimonWaldherr/tinySQL/internal/sqlbind"
)

// bindPlaceholders substitutes ?, $n and :n placeholders with SQL literals.
// Quoted strings, quoted identifiers and comments are copied verbatim.
func bindPlaceholders(sqlStr string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return sqlbind.Bind(sqlStr, nil)
	}
	// Precompute literal strings for all args to avoid repeated formatting.
	lits := make([]string, len(args))
	for i := range args {
		lits[i] = sqlLiteral(args[i].Value)
	}
	return sqlbind.Bind(sqlStr, lits)
}

// sqlLiteral converts a Go value into a SQL literal string suitable for
// substitution in a query.
func sqlLiteral(v any) string { return sqlbind.Literal(v) }
