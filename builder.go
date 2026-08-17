// Package tinysql - Fluent Query Builder API
//
// This file provides a type-safe, fluent interface for constructing SQL queries
// programmatically, similar to JOOQ, GORM, or Squirrel.

package tinysql

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Query Builder - Fluent API for constructing SQL queries
// ============================================================================

// SelectBuilder provides a fluent interface for building SELECT queries.
type SelectBuilder struct {
	distinct bool
	projs    []engine.SelectItem
	from     *engine.FromItem
	joins    []engine.JoinClause
	where    engine.Expr
	groupBy  []engine.Expr
	having   engine.Expr
	orderBy  []engine.OrderItem
	limit    *int
	offset   *int
	ctes     []engine.CTE
	unions   *engine.UnionClause
}

// Select creates a new SELECT query builder with the specified projections.
//
// Example:
//
//	q := tinysql.Select(Col("id"), Col("name")).
//	     From("users").
//	     Where(Gt(Col("age"), Val(18)))
func Select(projections ...ExprBuilder) *SelectBuilder {
	sb := &SelectBuilder{
		projs: make([]engine.SelectItem, len(projections)),
	}
	for i, p := range projections {
		sb.projs[i] = engine.SelectItem{Expr: p.Build()}
	}
	return sb
}

// SelectDistinct creates a SELECT DISTINCT query.
func SelectDistinct(projections ...ExprBuilder) *SelectBuilder {
	sb := Select(projections...)
	sb.distinct = true
	return sb
}

// SelectStar creates a SELECT * query.
func SelectStar() *SelectBuilder {
	return &SelectBuilder{
		projs: []engine.SelectItem{{Star: true}},
	}
}

// From specifies the FROM table.
func (sb *SelectBuilder) From(table string) *SelectBuilder {
	sb.from = &engine.FromItem{Table: table}
	return sb
}

// FromAs specifies the FROM table with an alias.
func (sb *SelectBuilder) FromAs(table, alias string) *SelectBuilder {
	sb.from = &engine.FromItem{Table: table, Alias: alias}
	return sb
}

// Where adds a WHERE condition.
func (sb *SelectBuilder) Where(condition ExprBuilder) *SelectBuilder {
	sb.where = condition.Build()
	return sb
}

// Join adds an INNER JOIN clause.
func (sb *SelectBuilder) Join(table string, on ExprBuilder) *SelectBuilder {
	sb.joins = append(sb.joins, engine.JoinClause{
		Type:  engine.JoinInner,
		Right: engine.FromItem{Table: table},
		On:    on.Build(),
	})
	return sb
}

// JoinAs adds an INNER JOIN with an alias.
func (sb *SelectBuilder) JoinAs(table, alias string, on ExprBuilder) *SelectBuilder {
	sb.joins = append(sb.joins, engine.JoinClause{
		Type:  engine.JoinInner,
		Right: engine.FromItem{Table: table, Alias: alias},
		On:    on.Build(),
	})
	return sb
}

// LeftJoin adds a LEFT OUTER JOIN clause.
func (sb *SelectBuilder) LeftJoin(table string, on ExprBuilder) *SelectBuilder {
	sb.joins = append(sb.joins, engine.JoinClause{
		Type:  engine.JoinLeft,
		Right: engine.FromItem{Table: table},
		On:    on.Build(),
	})
	return sb
}

// LeftJoinAs adds a LEFT OUTER JOIN with an alias.
func (sb *SelectBuilder) LeftJoinAs(table, alias string, on ExprBuilder) *SelectBuilder {
	sb.joins = append(sb.joins, engine.JoinClause{
		Type:  engine.JoinLeft,
		Right: engine.FromItem{Table: table, Alias: alias},
		On:    on.Build(),
	})
	return sb
}

// GroupBy adds GROUP BY columns.
func (sb *SelectBuilder) GroupBy(columns ...string) *SelectBuilder {
	for _, col := range columns {
		sb.groupBy = append(sb.groupBy, &engine.VarRef{Name: col, Lower: strings.ToLower(col)})
	}
	return sb
}

// GroupByExpr adds GROUP BY expressions.
func (sb *SelectBuilder) GroupByExpr(exprs ...ExprBuilder) *SelectBuilder {
	for _, expr := range exprs {
		sb.groupBy = append(sb.groupBy, expr.Build())
	}
	return sb
}

// Having adds a HAVING condition.
func (sb *SelectBuilder) Having(condition ExprBuilder) *SelectBuilder {
	sb.having = condition.Build()
	return sb
}

// OrderBy adds an ORDER BY clause (ascending).
func (sb *SelectBuilder) OrderBy(column string) *SelectBuilder {
	sb.orderBy = append(sb.orderBy, engine.OrderItem{Col: column, Desc: false})
	return sb
}

// OrderByDesc adds an ORDER BY clause (descending).
func (sb *SelectBuilder) OrderByDesc(column string) *SelectBuilder {
	sb.orderBy = append(sb.orderBy, engine.OrderItem{Col: column, Desc: true})
	return sb
}

// Limit sets the LIMIT clause.
func (sb *SelectBuilder) Limit(n int) *SelectBuilder {
	sb.limit = &n
	return sb
}

// Offset sets the OFFSET clause.
func (sb *SelectBuilder) Offset(n int) *SelectBuilder {
	sb.offset = &n
	return sb
}

// Build converts the builder to an engine.Select statement.
func (sb *SelectBuilder) Build() *engine.Select {
	sel := &engine.Select{
		Distinct: sb.distinct,
		Projs:    sb.projs,
		Joins:    sb.joins,
		Where:    sb.where,
		GroupBy:  sb.groupBy,
		Having:   sb.having,
		OrderBy:  sb.orderBy,
		Limit:    sb.limit,
		Offset:   sb.offset,
		CTEs:     sb.ctes,
		Union:    sb.unions,
	}
	if sb.from != nil {
		sel.From = *sb.from
	}
	return sel
}

// ============================================================================
// CTE (Common Table Expression) Support
// ============================================================================

// WithQuery represents a Common Table Expression (CTE).
type WithQuery struct {
	Name  string
	Query *SelectBuilder
}

// With adds Common Table Expressions (WITH clause) to a query.
//
// Example:
//
//	top := tinysql.WithQuery{
//	    Name: "top_users",
//	    Query: tinysql.Select(Col("id"), Col("score")).
//	           From("users").
//	           Where(Gt(Col("score"), Val(100))),
//	}
//	q := tinysql.With(top).
//	     Select(Col("id"), Col("amount")).
//	     From("orders")
func With(ctes ...WithQuery) *SelectBuilder {
	sb := &SelectBuilder{
		ctes: make([]engine.CTE, len(ctes)),
	}
	for i, cte := range ctes {
		sb.ctes[i] = engine.CTE{
			Name:   cte.Name,
			Select: cte.Query.Build(),
		}
	}
	return sb
}

// ============================================================================
// Expression Builders
// ============================================================================

// ExprBuilder is an interface for building SQL expressions.
type ExprBuilder interface {
	Build() engine.Expr
}

// exprWrapper wraps an engine.Expr to implement ExprBuilder.
type exprWrapper struct {
	expr engine.Expr
}

func (e exprWrapper) Build() engine.Expr { return e.expr }

// Col creates a column reference expression.
//
// Example:
//
//	Col("users.id")
//	Col("name")
func Col(name string) ExprBuilder {
	return exprWrapper{&engine.VarRef{Name: name, Lower: strings.ToLower(name)}}
}

// Val creates a literal value expression.
//
// Example:
//
//	Val(42)
//	Val("hello")
//	Val(true)
func Val(value any) ExprBuilder {
	return exprWrapper{&engine.Literal{Val: value}}
}

// Null creates a NULL literal.
func Null() ExprBuilder {
	return exprWrapper{&engine.Literal{Val: nil}}
}

// ============================================================================
// Comparison Operators
// ============================================================================

// Eq creates an equality comparison (=).
func Eq(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "=",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Ne creates a not-equal comparison (<>).
func Ne(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "<>",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Lt creates a less-than comparison (<).
func Lt(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "<",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Le creates a less-than-or-equal comparison (<=).
func Le(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "<=",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Gt creates a greater-than comparison (>).
func Gt(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    ">",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Ge creates a greater-than-or-equal comparison (>=).
func Ge(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    ">=",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// ============================================================================
// Logical Operators
// ============================================================================

// And creates a logical AND expression.
func And(exprs ...ExprBuilder) ExprBuilder {
	if len(exprs) == 0 {
		return Val(true)
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	result := exprs[0].Build()
	for i := 1; i < len(exprs); i++ {
		result = &engine.Binary{
			Op:    "AND",
			Left:  result,
			Right: exprs[i].Build(),
		}
	}
	return exprWrapper{result}
}

// Or creates a logical OR expression.
func Or(exprs ...ExprBuilder) ExprBuilder {
	if len(exprs) == 0 {
		return Val(false)
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	result := exprs[0].Build()
	for i := 1; i < len(exprs); i++ {
		result = &engine.Binary{
			Op:    "OR",
			Left:  result,
			Right: exprs[i].Build(),
		}
	}
	return exprWrapper{result}
}

// Not creates a logical NOT expression.
func Not(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Unary{
		Op:   "NOT",
		Expr: expr.Build(),
	}}
}

// ============================================================================
// NULL Checks
// ============================================================================

// IsNull creates an IS NULL expression.
func IsNull(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.IsNull{
		Expr:   expr.Build(),
		Negate: false,
	}}
}

// IsNotNull creates an IS NOT NULL expression.
func IsNotNull(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.IsNull{
		Expr:   expr.Build(),
		Negate: true,
	}}
}

// ============================================================================
// Arithmetic Operators
// ============================================================================

// Add creates an addition expression (+).
func Add(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "+",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Sub creates a subtraction expression (-).
func Sub(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "-",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Mul creates a multiplication expression (*).
func Mul(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "*",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Div creates a division expression (/).
func Div(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "/",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// ============================================================================
// Aggregate Functions
// ============================================================================

// Count creates a COUNT aggregate function.
func Count(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "COUNT",
		Args: []engine.Expr{expr.Build()},
	}}
}

// CountStar creates a COUNT(*) aggregate.
func CountStar() ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "COUNT",
		Args: []engine.Expr{&engine.Literal{Val: "*"}},
	}}
}

// Sum creates a SUM aggregate function.
func Sum(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SUM",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Avg creates an AVG aggregate function.
func Avg(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "AVG",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Min creates a MIN aggregate function.
func Min(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "MIN",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Max creates a MAX aggregate function.
func Max(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "MAX",
		Args: []engine.Expr{expr.Build()},
	}}
}

// ============================================================================
// String Functions
// ============================================================================

// Upper creates an UPPER function call.
func Upper(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "UPPER",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Lower creates a LOWER function call.
func Lower(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "LOWER",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Concat creates a CONCAT function call.
func Concat(exprs ...ExprBuilder) ExprBuilder {
	args := make([]engine.Expr, len(exprs))
	for i, e := range exprs {
		args[i] = e.Build()
	}
	return exprWrapper{&engine.FuncCall{
		Name: "CONCAT",
		Args: args,
	}}
}

// Trim creates a TRIM function call.
func Trim(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "TRIM",
		Args: []engine.Expr{expr.Build()},
	}}
}

// ============================================================================
// Hashing Functions
// ============================================================================

// MD5 creates an MD5 hash function call.
func MD5(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "MD5",
		Args: []engine.Expr{expr.Build()},
	}}
}

// SHA1 creates a SHA1 hash function call.
func SHA1(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SHA1",
		Args: []engine.Expr{expr.Build()},
	}}
}

// SHA256 creates a SHA256 hash function call.
func SHA256(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SHA256",
		Args: []engine.Expr{expr.Build()},
	}}
}

// SHA512 creates a SHA512 hash function call.
func SHA512(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SHA512",
		Args: []engine.Expr{expr.Build()},
	}}
}

// ============================================================================
// Special Functions
// ============================================================================

// Coalesce creates a COALESCE function call.
func Coalesce(exprs ...ExprBuilder) ExprBuilder {
	args := make([]engine.Expr, len(exprs))
	for i, e := range exprs {
		args[i] = e.Build()
	}
	return exprWrapper{&engine.FuncCall{
		Name: "COALESCE",
		Args: args,
	}}
}

// Exists creates an EXISTS predicate for a subquery.
func Exists(query *SelectBuilder) ExprBuilder {
	return exprWrapper{&engine.ExistsExpr{Select: query.Build()}}
}

// NotExists creates a NOT EXISTS predicate for a subquery.
func NotExists(query *SelectBuilder) ExprBuilder {
	return Not(Exists(query))
}

// ============================================================================
// Table Builder - For programmatic schema definition
// ============================================================================

// TableBuilder provides a fluent interface for defining tables.
type TableBuilder struct {
	name    string
	columns []storage.Column
	isTemp  bool
}

// NewTableBuilder creates a new table builder for programmatic schema definition.
func NewTableBuilder(name string) *TableBuilder {
	return &TableBuilder{name: name}
}

// Temp marks the table as temporary.
func (tb *TableBuilder) Temp() *TableBuilder {
	tb.isTemp = true
	return tb
}

// Column adds a column to the table.
func (tb *TableBuilder) Column(name string, colType ColType) *TableBuilder {
	tb.columns = append(tb.columns, storage.Column{
		Name: name,
		Type: colType,
	})
	return tb
}

// Int adds an INT column.
func (tb *TableBuilder) Int(name string) *TableBuilder {
	return tb.Column(name, IntType)
}

// Text adds a TEXT column.
func (tb *TableBuilder) Text(name string) *TableBuilder {
	return tb.Column(name, TextType)
}

// Bool adds a BOOL column.
func (tb *TableBuilder) Bool(name string) *TableBuilder {
	return tb.Column(name, BoolType)
}

// Float adds a FLOAT column.
func (tb *TableBuilder) Float(name string) *TableBuilder {
	return tb.Column(name, FloatType)
}

// Timestamp adds a TIMESTAMP column.
func (tb *TableBuilder) Timestamp(name string) *TableBuilder {
	return tb.Column(name, TimestampType)
}

// JSON adds a JSON column.
func (tb *TableBuilder) JSON(name string) *TableBuilder {
	return tb.Column(name, JsonType)
}

// Build creates the table definition.
func (tb *TableBuilder) Build() *engine.CreateTable {
	return &engine.CreateTable{
		Name:   tb.name,
		Cols:   tb.columns,
		IsTemp: tb.isTemp,
	}
}

// Create executes the CREATE TABLE statement.
func (tb *TableBuilder) Create(db *DB, tenant string) error {
	table := NewTable(tb.name, tb.columns, tb.isTemp)
	return db.Put(tenant, table)
}

// ============================================================================
// Insert Builder
// ============================================================================

// InsertBuilder provides a fluent interface for building INSERT statements.
type InsertBuilder struct {
	table string
	cols  []string
	rows  [][]engine.Expr
}

// InsertInto creates a new INSERT builder.
func InsertInto(table string) *InsertBuilder {
	return &InsertBuilder{table: table}
}

// Columns specifies the columns for the insert.
func (ib *InsertBuilder) Columns(cols ...string) *InsertBuilder {
	ib.cols = cols
	return ib
}

// Values specifies the values to insert.
func (ib *InsertBuilder) Values(values ...ExprBuilder) *InsertBuilder {
	row := make([]engine.Expr, len(values))
	for i, v := range values {
		row[i] = v.Build()
	}
	ib.rows = append(ib.rows, row)
	return ib
}

// Build creates the INSERT statement.
func (ib *InsertBuilder) Build() *engine.Insert {
	return &engine.Insert{
		Table: ib.table,
		Cols:  ib.cols,
		Rows:  ib.rows,
	}
}

// ============================================================================
// Update Builder
// ============================================================================

// UpdateBuilder provides a fluent interface for building UPDATE statements.
type UpdateBuilder struct {
	table string
	sets  map[string]engine.Expr
	where engine.Expr
}

// Update creates a new UPDATE builder.
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{
		table: table,
		sets:  make(map[string]engine.Expr),
	}
}

// Set adds a column assignment.
func (ub *UpdateBuilder) Set(column string, value ExprBuilder) *UpdateBuilder {
	ub.sets[column] = value.Build()
	return ub
}

// Where adds a WHERE condition.
func (ub *UpdateBuilder) Where(condition ExprBuilder) *UpdateBuilder {
	ub.where = condition.Build()
	return ub
}

// Build creates the UPDATE statement.
func (ub *UpdateBuilder) Build() *engine.Update {
	return &engine.Update{
		Table: ub.table,
		Sets:  ub.sets,
		Where: ub.where,
	}
}

// ============================================================================
// Delete Builder
// ============================================================================

// DeleteBuilder provides a fluent interface for building DELETE statements.
type DeleteBuilder struct {
	table string
	where engine.Expr
}

// DeleteFrom creates a new DELETE builder.
func DeleteFrom(table string) *DeleteBuilder {
	return &DeleteBuilder{table: table}
}

// Where adds a WHERE condition.
func (db *DeleteBuilder) Where(condition ExprBuilder) *DeleteBuilder {
	db.where = condition.Build()
	return db
}

// Build creates the DELETE statement.
func (db *DeleteBuilder) Build() *engine.Delete {
	return &engine.Delete{
		Table: db.table,
		Where: db.where,
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

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
	sb.WriteString("INSERT INTO ")
	sb.WriteString(i.Table)
	if len(i.Cols) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(i.Cols, ", "))
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
	sb.WriteString("UPDATE ")
	sb.WriteString(u.Table)
	sb.WriteString(" SET ")
	cols := make([]string, 0, len(u.Sets))
	for col := range u.Sets {
		cols = append(cols, col)
	}
	sort.Strings(cols)
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
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "X'" + hex.EncodeToString(x) + "'"
	case time.Time:
		return "'" + x.Format(time.RFC3339Nano) + "'"
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return fmt.Sprintf("%v", v)
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
		sb.WriteString(literalToSQL(ex.Val))
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
