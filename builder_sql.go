package tinysql

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// ToSQL converts a statement to SQL string (for debugging).
// Note: This is a best-effort implementation and may not produce
// perfectly formatted SQL in all cases.
func ToSQL(stmt engine.Statement) string {
	switch s := stmt.(type) {
	case *engine.Select:
		return selectToSQL(s)
	case *engine.Insert:
		return insertToSQL(s)
	case *engine.Update:
		return updateToSQL(s)
	case *engine.Delete:
		return deleteToSQL(s)
	case *engine.CreateTable:
		return createTableToSQL(s)
	case *engine.DropTable:
		return fmt.Sprintf("DROP TABLE %s", s.Name)
	default:
		return "UNKNOWN STATEMENT"
	}
}

func selectToSQL(s *engine.Select) string {
	var sb strings.Builder
	sb.Grow(estimateSelectSQLSize(s))
	writeSelectSQL(&sb, s)
	return sb.String()
}

func writeSelectSQL(sb *strings.Builder, s *engine.Select) {
	buildCTEs(sb, s.CTEs)
	buildSelectClause(sb, s)
	buildFromClause(sb, s.From)
	buildJoinClauses(sb, s.Joins)
	buildWhereClause(sb, s.Where)
	buildGroupByClause(sb, s.GroupBy)
	buildHavingClause(sb, s.Having)
	buildOrderByClause(sb, s.OrderBy)
	buildLimitOffsetClauses(sb, s.Limit, s.Offset)
}

// Helper: build CTEs clause
func buildCTEs(sb *strings.Builder, ctes []engine.CTE) {
	if len(ctes) > 0 {
		sb.WriteString("WITH ")
		for i, cte := range ctes {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(cte.Name)
			sb.WriteString(" AS (")
			writeSelectSQL(sb, cte.Select)
			sb.WriteString(")")
		}
		sb.WriteString(" ")
	}
}

func estimateSelectSQLSize(s *engine.Select) int {
	n := len("SELECT ")
	for _, proj := range s.Projs {
		if proj.Star {
			n++
		} else {
			n += estimateExprSQLSize(proj.Expr)
		}
		if proj.Alias != "" {
			n += len(" AS ") + len(proj.Alias)
		}
		n += len(", ")
	}
	if s.Distinct {
		n += len("DISTINCT ")
	}
	if s.From.Table != "" {
		n += len(" FROM ") + len(s.From.Table)
		if s.From.Alias != "" {
			n += len(" AS ") + len(s.From.Alias)
		}
	}
	for _, join := range s.Joins {
		n += len(" INNER JOIN ") + len(join.Right.Table) + len(" ON ") + estimateExprSQLSize(join.On)
		if join.Right.Alias != "" {
			n += len(" AS ") + len(join.Right.Alias)
		}
	}
	if s.Where != nil {
		n += len(" WHERE ") + estimateExprSQLSize(s.Where)
	}
	for _, expr := range s.GroupBy {
		n += estimateExprSQLSize(expr) + len(", ")
	}
	if len(s.GroupBy) > 0 {
		n += len(" GROUP BY ")
	}
	if s.Having != nil {
		n += len(" HAVING ") + estimateExprSQLSize(s.Having)
	}
	for _, order := range s.OrderBy {
		n += len(order.Col) + len(", ")
		if order.Desc {
			n += len(" DESC")
		}
	}
	if len(s.OrderBy) > 0 {
		n += len(" ORDER BY ")
	}
	if s.Limit != nil {
		n += len(" LIMIT ") + 20
	}
	if s.Offset != nil {
		n += len(" OFFSET ") + 20
	}
	for _, cte := range s.CTEs {
		n += len("WITH  AS ()") + len(cte.Name) + estimateSelectSQLSize(cte.Select)
	}
	return n
}

func estimateExprSQLSize(e engine.Expr) int {
	if e == nil {
		return len("NULL")
	}
	switch ex := e.(type) {
	case *engine.Literal:
		switch value := ex.Val.(type) {
		case nil:
			return len("NULL")
		case string:
			return len(value) + 2 + strings.Count(value, "'")
		case []byte:
			return len(value)*2 + len("X''")
		case bool:
			return len("FALSE")
		default:
			return 24
		}
	case *engine.VarRef:
		return len(ex.Name)
	case *engine.Binary:
		return estimateExprSQLSize(ex.Left) + len(ex.Op) + estimateExprSQLSize(ex.Right) + 4
	case *engine.Unary:
		return len(ex.Op) + estimateExprSQLSize(ex.Expr) + 3
	case *engine.IsNull:
		if ex.Negate {
			return estimateExprSQLSize(ex.Expr) + len("( IS NOT NULL)")
		}
		return estimateExprSQLSize(ex.Expr) + len("( IS NULL)")
	case *engine.FuncCall:
		n := len(ex.Name) + 2
		for _, arg := range ex.Args {
			n += estimateExprSQLSize(arg) + len(", ")
		}
		return n
	case *engine.ExistsExpr:
		return len("EXISTS ()") + estimateSelectSQLSize(ex.Select)
	default:
		return 1
	}
}

// Helper: build SELECT clause
func buildSelectClause(sb *strings.Builder, s *engine.Select) {
	sb.WriteString("SELECT ")
	if s.Distinct {
		sb.WriteString("DISTINCT ")
	}
	for i, proj := range s.Projs {
		if i > 0 {
			sb.WriteString(", ")
		}
		if proj.Star {
			sb.WriteString("*")
		} else {
			writeExprSQL(sb, proj.Expr)
			if proj.Alias != "" {
				sb.WriteString(" AS ")
				sb.WriteString(proj.Alias)
			}
		}
	}
}

// Helper: build FROM clause
func buildFromClause(sb *strings.Builder, from engine.FromItem) {
	if from.Table != "" {
		sb.WriteString(" FROM ")
		sb.WriteString(from.Table)
		if from.Alias != "" {
			sb.WriteString(" AS ")
			sb.WriteString(from.Alias)
		}
	}
}

// Helper: build JOIN clauses
func buildJoinClauses(sb *strings.Builder, joins []engine.JoinClause) {
	for _, join := range joins {
		switch join.Type {
		case engine.JoinInner:
			sb.WriteString(" INNER JOIN ")
		case engine.JoinLeft:
			sb.WriteString(" LEFT JOIN ")
		case engine.JoinRight:
			sb.WriteString(" RIGHT JOIN ")
		}
		sb.WriteString(join.Right.Table)
		if join.Right.Alias != "" {
			sb.WriteString(" AS ")
			sb.WriteString(join.Right.Alias)
		}
		sb.WriteString(" ON ")
		writeExprSQL(sb, join.On)
	}
}

// Helper: build WHERE clause
func buildWhereClause(sb *strings.Builder, where engine.Expr) {
	if where != nil {
		sb.WriteString(" WHERE ")
		writeExprSQL(sb, where)
	}
}

// Helper: build GROUP BY clause
func buildGroupByClause(sb *strings.Builder, groupBy []engine.Expr) {
	if len(groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, g := range groupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			writeExprSQL(sb, g)
		}
	}
}

// Helper: build HAVING clause
func buildHavingClause(sb *strings.Builder, having engine.Expr) {
	if having != nil {
		sb.WriteString(" HAVING ")
		writeExprSQL(sb, having)
	}
}

// Helper: build ORDER BY clause
func buildOrderByClause(sb *strings.Builder, orderBy []engine.OrderItem) {
	if len(orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, o := range orderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(o.Col)
			if o.Desc {
				sb.WriteString(" DESC")
			}
		}
	}
}

// Helper: build LIMIT/OFFSET clauses
func buildLimitOffsetClauses(sb *strings.Builder, limit *int, offset *int) {
	if limit != nil {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(*limit))
	}
	if offset != nil {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(*offset))
	}
}

func insertToSQL(i *engine.Insert) string {
	var sb strings.Builder
	size := len("INSERT INTO  () VALUES ") + len(i.Table)
	for _, col := range i.Cols {
		size += len(col) + 2
	}
	for _, row := range i.Rows {
		size += 4
		for _, value := range row {
			size += estimateExprSQLSize(value) + 2
		}
	}
	sb.Grow(size)
	sb.WriteString("INSERT INTO ")
	sb.WriteString(i.Table)
	if len(i.Cols) > 0 {
		sb.WriteString(" (")
		for n, col := range i.Cols {
			if n > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	sb.WriteString(" VALUES ")
	for ri, row := range i.Rows {
		if ri > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for ci, val := range row {
			if ci > 0 {
				sb.WriteString(", ")
			}
			writeExprSQL(&sb, val)
		}
		sb.WriteString(")")
	}
	return sb.String()
}

func updateToSQL(u *engine.Update) string {
	var sb strings.Builder
	size := len("UPDATE  SET  WHERE ") + len(u.Table) + estimateExprSQLSize(u.Where)
	cols := make([]string, 0, len(u.Sets))
	for col, value := range u.Sets {
		cols = append(cols, col)
		size += len(col) + 5 + estimateExprSQLSize(value)
	}
	sort.Strings(cols)
	sb.Grow(size)
	sb.WriteString("UPDATE ")
	sb.WriteString(u.Table)
	sb.WriteString(" SET ")
	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
		sb.WriteString(" = ")
		writeExprSQL(&sb, u.Sets[col])
	}
	if u.Where != nil {
		sb.WriteString(" WHERE ")
		writeExprSQL(&sb, u.Where)
	}
	return sb.String()
}

func deleteToSQL(d *engine.Delete) string {
	var sb strings.Builder
	sb.Grow(len("DELETE FROM  WHERE ") + len(d.Table) + estimateExprSQLSize(d.Where))
	sb.WriteString("DELETE FROM ")
	sb.WriteString(d.Table)
	if d.Where != nil {
		sb.WriteString(" WHERE ")
		writeExprSQL(&sb, d.Where)
	}
	return sb.String()
}

func createTableToSQL(c *engine.CreateTable) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if c.IsTemp {
		sb.WriteString("TEMP ")
	}
	sb.WriteString("TABLE ")
	sb.WriteString(c.Name)
	sb.WriteString(" (")
	for i, col := range c.Cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(col.Type.String())
	}
	sb.WriteString(")")
	return sb.String()
}

// literalToSQL renders a literal value as valid, re-parseable SQL, mirroring
// the driver's parameter encoding: single-quoted and escaped strings, X'..'
// blobs, quoted RFC3339 timestamps, bare numerics/bools, and NULL. Without
// this, string/blob/time literals in ToSQL output were unquoted and unescaped
// (e.g. WHERE name = O'Brien), i.e. syntactically invalid SQL.
func literalToSQL(v any) string {
	var sb strings.Builder
	writeLiteralSQL(&sb, v)
	return sb.String()
}

// Write literals into the statement buffer, avoiding temporary quoted,
// hexadecimal and numeric strings for every cell of a batch INSERT.
func writeLiteralSQL(sb *strings.Builder, v any) {
	var scratch [64]byte
	switch x := v.(type) {
	case nil:
		sb.WriteString("NULL")
	case string:
		sb.WriteByte('\'')
		for {
			pos := strings.IndexByte(x, '\'')
			if pos < 0 {
				sb.WriteString(x)
				break
			}
			sb.WriteString(x[:pos+1])
			sb.WriteByte('\'')
			x = x[pos+1:]
		}
		sb.WriteByte('\'')
	case []byte:
		sb.WriteString("X'")
		var encoded [512]byte
		for len(x) > 0 {
			n := min(len(x), len(encoded)/2)
			hex.Encode(encoded[:2*n], x[:n])
			sb.Write(encoded[:2*n])
			x = x[n:]
		}
		sb.WriteByte('\'')
	case time.Time:
		sb.WriteByte('\'')
		sb.Write(x.AppendFormat(scratch[:0], time.RFC3339Nano))
		sb.WriteByte('\'')
	case bool:
		if x {
			sb.WriteString("TRUE")
		} else {
			sb.WriteString("FALSE")
		}
	case float32:
		sb.Write(strconv.AppendFloat(scratch[:0], float64(x), 'f', -1, 32))
	case float64:
		sb.Write(strconv.AppendFloat(scratch[:0], x, 'f', -1, 64))
	case int:
		if x >= 0 && x < 100 {
			// strconv reuses its immutable small-integer strings.
			sb.WriteString(strconv.Itoa(x))
		} else {
			sb.Write(strconv.AppendInt(scratch[:0], int64(x), 10))
		}
	case int64:
		sb.Write(strconv.AppendInt(scratch[:0], x, 10))
	default:
		sb.WriteString(fmt.Sprintf("%v", v))
	}
}

func exprToSQL(e engine.Expr) string {
	var sb strings.Builder
	writeExprSQL(&sb, e)
	return sb.String()
}

// writeExprSQL renders directly into one builder so nested expressions do not
// allocate a temporary SQL string at each tree level.
func writeExprSQL(sb *strings.Builder, e engine.Expr) {
	if e == nil {
		sb.WriteString("NULL")
		return
	}
	switch ex := e.(type) {
	case *engine.Literal:
		writeLiteralSQL(sb, ex.Val)
	case *engine.VarRef:
		sb.WriteString(ex.Name)
	case *engine.Binary:
		sb.WriteByte('(')
		writeExprSQL(sb, ex.Left)
		sb.WriteByte(' ')
		sb.WriteString(ex.Op)
		sb.WriteByte(' ')
		writeExprSQL(sb, ex.Right)
		sb.WriteByte(')')
	case *engine.Unary:
		sb.WriteString(ex.Op)
		sb.WriteString(" (")
		writeExprSQL(sb, ex.Expr)
		sb.WriteByte(')')
	case *engine.IsNull:
		sb.WriteByte('(')
		writeExprSQL(sb, ex.Expr)
		if ex.Negate {
			sb.WriteString(" IS NOT NULL)")
			return
		}
		sb.WriteString(" IS NULL)")
	case *engine.FuncCall:
		sb.WriteString(ex.Name)
		sb.WriteByte('(')
		for i, arg := range ex.Args {
			if i > 0 {
				sb.WriteString(", ")
			}
			// The star in COUNT(*) is carried as a "*" string literal; render it
			// bare rather than quoting it as a string value.
			if lit, ok := arg.(*engine.Literal); ok && lit.Val == "*" {
				sb.WriteByte('*')
				continue
			}
			writeExprSQL(sb, arg)
		}
		sb.WriteByte(')')
	case *engine.ExistsExpr:
		sb.WriteString("EXISTS (")
		writeSelectSQL(sb, ex.Select)
		sb.WriteByte(')')
	default:
		sb.WriteByte('?')
	}
}
