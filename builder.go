package tinysql

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

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
