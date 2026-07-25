// Parsing SELECT: common table expressions, DISTINCT, the projection list,
// FROM, joins, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, PIVOT and the
// set operations that combine two selects.
package engine

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

func (p *Parser) parseSelectWithCTE() (*Select, error) {
	var ctes []CTE

	// Parse WITH clause if present
	if p.cur.Typ == tKeyword && p.cur.Val == "WITH" {
		p.next()

		// WITH RECURSIVE permits self references in following CTEs. Whether a
		// particular CTE is recursive is determined after parsing its SELECT.
		recursiveAll := false
		if p.cur.Typ == tKeyword && p.cur.Val == "RECURSIVE" {
			recursiveAll = true
			p.next()
		}

		for {
			// Parse CTE name
			cteName := p.parseIdentLike()
			if cteName == "" {
				return nil, p.errf("expected CTE name")
			}

			// Optional column list: WITH cte(col1, col2) AS (...)
			var cteColumns []string
			if p.cur.Typ == tSymbol && p.cur.Val == "(" {
				// Consume and retain the aliases; they rename the CTE output.
				p.next()
				for {
					// accept identifier-like column names
					if p.cur.Typ != tIdent && p.cur.Typ != tKeyword {
						return nil, p.errf("expected column name in CTE column list")
					}
					cteColumns = append(cteColumns, p.cur.Val)
					p.next()
					if p.cur.Typ == tSymbol && p.cur.Val == "," {
						p.next()
						continue
					}
					break
				}
				if err := p.expectSymbol(")"); err != nil {
					return nil, err
				}
			}

			if err := p.expectKeyword("AS"); err != nil {
				return nil, err
			}

			if err := p.expectSymbol("("); err != nil {
				return nil, err
			}

			// Parse the SELECT statement for this CTE
			cteSelect, err := p.parseSelect()
			if err != nil {
				return nil, err
			}

			if err := p.expectSymbol(")"); err != nil {
				return nil, err
			}

			ctes = append(ctes, CTE{
				Name:      cteName,
				Columns:   cteColumns,
				Select:    cteSelect,
				Recursive: recursiveAll && selectReferencesCTEName(cteSelect, cteName),
			})

			// Check for more CTEs
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}

	// Parse the main SELECT statement
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	// Attach CTEs to the main SELECT
	sel.CTEs = ctes

	return sel, nil
}

// selectReferencesCTEName reports whether sel's FROM tree references name.
// It is intentionally structural: the parser needs only distinguish an
// actual self-reference from an ordinary CTE written under WITH RECURSIVE.
func selectReferencesCTEName(sel *Select, name string) bool {
	if sel == nil {
		return false
	}
	name = strings.ToLower(name)
	fromReferences := func(from FromItem) bool {
		if strings.EqualFold(from.Table, name) {
			return true
		}
		return from.Subquery != nil && selectReferencesCTEName(from.Subquery, name)
	}
	if fromReferences(sel.From) {
		return true
	}
	for _, join := range sel.Joins {
		if fromReferences(join.Right) {
			return true
		}
	}
	for union := sel.Union; union != nil; union = union.Next {
		if selectReferencesCTEName(union.Right, name) {
			return true
		}
	}
	return false
}

func (p *Parser) parseSelect() (*Select, error) {
	if err := p.enterRecursion(); err != nil {
		return nil, err
	}
	defer p.exitRecursion()
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &Select{simplePlanCache: &simpleSelectPlanCache{}}

	// Parse DISTINCT
	if err := p.parseDistinct(sel); err != nil {
		return nil, err
	}

	// Parse projection list
	if err := p.parseProjections(sel); err != nil {
		return nil, err
	}

	// Parse FROM
	if err := p.parseFromClause(sel); err != nil {
		return nil, err
	}

	// Parse JOINs
	if err := p.parseJoinClauses(sel); err != nil {
		return nil, err
	}

	// Parse WHERE
	if err := p.parseWhereClause(sel); err != nil {
		return nil, err
	}

	// Parse PIVOT
	if err := p.parsePivotClause(sel); err != nil {
		return nil, err
	}

	// Parse GROUP BY
	if err := p.parseGroupByClause(sel); err != nil {
		return nil, err
	}

	// Parse HAVING
	if err := p.parseHavingClause(sel); err != nil {
		return nil, err
	}

	// Parse ORDER BY
	if err := p.parseOrderByClause(sel); err != nil {
		return nil, err
	}

	// Parse LIMIT and OFFSET
	if err := p.parseLimitOffset(sel); err != nil {
		return nil, err
	}

	// Parse UNION clauses
	if err := p.parseUnionClause(sel); err != nil {
		return nil, err
	}

	return sel, nil
}

// parsePivotClause parses an optional
// "PIVOT (agg(expr) FOR col IN (v1 [AS a1], v2 [AS a2], ...))" clause.
func (p *Parser) parsePivotClause(sel *Select) error {
	if p.cur.Typ != tKeyword || p.cur.Val != "PIVOT" {
		return nil
	}
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return err
	}

	if p.cur.Typ != tIdent && p.cur.Typ != tKeyword {
		return p.errf("expected aggregate function name in PIVOT")
	}
	aggFunc := strings.ToUpper(p.cur.Val)
	p.next()

	if err := p.expectSymbol("("); err != nil {
		return err
	}
	valueExpr, err := p.parseExpr()
	if err != nil {
		return err
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}

	if err := p.expectKeyword("FOR"); err != nil {
		return err
	}
	pivotCol := p.parseIdentLike()
	if pivotCol == "" {
		return p.errf("expected column name after FOR in PIVOT")
	}

	if err := p.expectKeyword("IN"); err != nil {
		return err
	}
	if err := p.expectSymbol("("); err != nil {
		return err
	}
	var values []PivotValue
	for {
		ve, err := p.parseExpr()
		if err != nil {
			return err
		}
		alias := ""
		if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
			p.next()
			alias = p.parseIdentLike()
			if alias == "" {
				return p.errf("expected alias after AS in PIVOT value list")
			}
		}
		values = append(values, PivotValue{Expr: ve, Alias: alias})
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}
	if len(values) == 0 {
		return p.errf("PIVOT IN (...) requires at least one value")
	}

	if err := p.expectSymbol(")"); err != nil {
		return err
	}

	sel.Pivot = &PivotClause{
		AggFunc:   aggFunc,
		ValueExpr: valueExpr,
		PivotCol:  pivotCol,
		Values:    values,
	}
	return nil
}

func (p *Parser) parseDistinct(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "DISTINCT" {
		p.next()
		// Check for DISTINCT ON (expr, ...)
		if p.cur.Typ == tKeyword && p.cur.Val == "ON" {
			p.next()
			if err := p.expectSymbol("("); err != nil {
				return err
			}
			var exprs []Expr
			for {
				e, err := p.parseExpr()
				if err != nil {
					return err
				}
				exprs = append(exprs, e)
				if p.cur.Typ == tSymbol && p.cur.Val == "," {
					p.next()
					continue
				}
				break
			}
			if err := p.expectSymbol(")"); err != nil {
				return err
			}
			sel.DistinctOn = exprs
			// Also mark generic Distinct true for compatibility
			sel.Distinct = true
			return nil
		}
		sel.Distinct = true
	}
	return nil
}

func (p *Parser) parseProjections(sel *Select) error {
	if p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		sel.Projs = append(sel.Projs, SelectItem{Star: true})
		return nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return err
		}
		alias := ""
		if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
			p.next()
			alias = p.parseIdentLike()
			if alias == "" {
				return p.errf("expected alias")
			}
		} else if p.cur.Typ == tIdent {
			alias = p.cur.Val
			p.next()
		}
		sel.Projs = append(sel.Projs, SelectItem{Expr: e, Alias: alias})
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	return nil
}

func (p *Parser) parseFromClause(sel *Select) error {
	// FROM is now optional (like MSSQL)
	if p.cur.Typ != tKeyword || p.cur.Val != "FROM" {
		// No FROM clause - this is allowed for expressions like SELECT NOW(), SELECT 1+1, etc.
		return nil
	}

	p.next() // consume FROM keyword

	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		return p.parseFromSubselect(sel)
	}
	return p.parseFromTableOrFunction(sel)
}

func (p *Parser) parseFromSubselect(sel *Select) error {
	p.next()
	subSel, err := p.parseSelect()
	if err != nil {
		return err
	}
	if p.cur.Typ != tSymbol || p.cur.Val != ")" {
		return p.errf("expected ) after subselect in FROM")
	}
	p.next()
	alias, err := p.parseRequiredAlias("expected alias after AS for subselect", "expected alias for subselect in FROM")
	if err != nil {
		return err
	}
	sel.From = FromItem{Subquery: subSel, Alias: alias}
	return nil
}

func (p *Parser) parseFromTableOrFunction(sel *Select) error {
	from := p.parseQualifiedIdentLike()
	if from == "" {
		return p.errf("expected table or table-valued function")
	}

	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		fcExpr, err := p.parseFuncCallWithName(from)
		if err != nil {
			return err
		}
		fc, ok := fcExpr.(*FuncCall)
		if !ok {
			return p.errf("internal: expected FuncCall for table function %q", from)
		}
		if fc.Over != nil {
			return p.errf("OVER clause not allowed for table-valued functions in FROM")
		}
		alias, err := p.parseOptionalAlias(from, "expected alias")
		if err != nil {
			return err
		}
		sel.From = FromItem{TableFunc: &TableFuncCall{Name: from, Args: fc.Args, Alias: alias}, Alias: alias}
		return nil
	}

	alias, err := p.parseOptionalAlias(from, "expected alias")
	if err != nil {
		return err
	}
	sel.From = FromItem{Table: from, Alias: alias}
	return nil
}

func (p *Parser) parseRequiredAlias(asMsg, missingMsg string) (string, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias := p.parseIdentLike()
		if alias == "" {
			return "", p.errf("%s", asMsg)
		}
		return alias, nil
	}
	if p.cur.Typ == tIdent {
		alias := p.cur.Val
		p.next()
		return alias, nil
	}
	return "", p.errf("%s", missingMsg)
}

func (p *Parser) parseOptionalAlias(defaultAlias, asMsg string) (string, error) {
	alias := defaultAlias
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias = p.parseIdentLike()
		if alias == "" {
			return "", p.errf("%s", asMsg)
		}
		return alias, nil
	}
	if p.cur.Typ == tIdent {
		alias = p.cur.Val
		p.next()
	}
	return alias, nil
}

func (p *Parser) parseJoinClauses(sel *Select) error {
	for {
		if p.cur.Typ == tKeyword && p.cur.Val == "JOIN" {
			p.next()
			right, on, err := p.parseJoinTail()
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: JoinInner, Right: right, On: on})
			continue
		}
		if p.cur.Typ == tKeyword && (p.cur.Val == "LEFT" || p.cur.Val == "RIGHT" || p.cur.Val == "FULL") {
			var jt JoinType
			switch p.cur.Val {
			case "LEFT":
				jt = JoinLeft
			case "RIGHT":
				jt = JoinRight
			case "FULL":
				jt = JoinFull
			}
			p.next()
			if p.cur.Typ == tKeyword && p.cur.Val == "OUTER" {
				p.next()
			}
			if err := p.expectKeyword("JOIN"); err != nil {
				return err
			}
			right, on, err := p.parseJoinTail()
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: jt, Right: right, On: on})
			continue
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "CROSS" {
			p.next()
			if err := p.expectKeyword("JOIN"); err != nil {
				return err
			}
			// CROSS JOIN is an unconditional Cartesian product: no ON clause,
			// so it can't reuse parseJoinTail (which always requires one).
			rt := p.parseQualifiedIdentLike()
			if rt == "" {
				return p.errf("expected table name after CROSS JOIN")
			}
			alias, err := p.parseOptionalAlias(rt, "expected alias")
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: JoinCross, Right: FromItem{Table: rt, Alias: alias}, On: nil})
			continue
		}
		break
	}
	return nil
}

func (p *Parser) parseWhereClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return err
		}
		sel.Where = e
	}
	return nil
}

func (p *Parser) parseGroupByClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "GROUP" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return err
			}
			sel.GroupBy = append(sel.GroupBy, expr)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}
	return nil
}

func (p *Parser) parseHavingClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "HAVING" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return err
		}
		sel.Having = e
	}
	return nil
}

func (p *Parser) parseOrderByClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "ORDER" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		for {
			var col string
			if (p.cur.Typ == tIdent || p.cur.Typ == tKeyword) && p.peek.Typ == tSymbol && p.peek.Val == "(" {
				expr, err := p.parseExpr()
				if err != nil {
					return err
				}
				var ok bool
				col, ok = orderByProjectionName(sel, expr)
				if !ok {
					return p.errf("ORDER BY expression must appear in the SELECT list or have an alias")
				}
			} else {
				col = p.parseIdentLike()
				if col == "" {
					return p.errf("ORDER BY expects column")
				}
			}
			desc := false
			if p.cur.Typ == tKeyword && (p.cur.Val == "ASC" || p.cur.Val == "DESC") {
				desc = (p.cur.Val == "DESC")
				p.next()
			}
			sel.OrderBy = append(sel.OrderBy, OrderItem{Col: col, Desc: desc})
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}
	return nil
}

// orderByProjectionName resolves a function/expression used in ORDER BY to
// the output column produced by the matching SELECT item. Sorting happens
// after projection in this executor, so an expression must be present in the
// projection list (typically with an alias such as COUNT(*) AS total).
func orderByProjectionName(sel *Select, expr Expr) (string, bool) {
	for i, item := range sel.Projs {
		if item.Star || !reflect.DeepEqual(item.Expr, expr) {
			continue
		}
		if item.Alias != "" {
			return item.Alias, true
		}
		if ref, ok := item.Expr.(*VarRef); ok {
			return ref.Name, true
		}
		return fmt.Sprintf("col_%d", i), true
	}
	return "", false
}

func (p *Parser) parseLimitOffset(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "LIMIT" {
		p.next()
		n, err := p.parseLimitOffsetValue("LIMIT")
		if err != nil {
			return err
		}
		sel.Limit = n
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		n, err := p.parseLimitOffsetValue("OFFSET")
		if err != nil {
			return err
		}
		sel.Offset = n
	}
	// SQL:2008 alternate syntax: OFFSET n ROWS [FETCH {FIRST|NEXT} m {ROW|ROWS} ONLY].
	// The bare "ROWS" after a numeric OFFSET is optional noise words.
	if p.cur.Typ == tKeyword && p.cur.Val == "ROWS" && sel.Offset != nil {
		p.next()
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "FETCH" {
		p.next()
		if p.cur.Typ != tKeyword || (p.cur.Val != "FIRST" && p.cur.Val != "NEXT") {
			return p.errf("expected FIRST or NEXT after FETCH")
		}
		p.next()
		n, err := p.parseLimitOffsetValue("FETCH")
		if err != nil {
			return err
		}
		sel.Limit = n
		if p.cur.Typ != tKeyword || (p.cur.Val != "ROW" && p.cur.Val != "ROWS") {
			return p.errf("expected ROW or ROWS after FETCH count")
		}
		p.next()
		if err := p.expectKeyword("ONLY"); err != nil {
			return err
		}
	}
	return nil
}

// parseLimitOffsetValue parses a LIMIT/OFFSET/FETCH value: either the
// SQL-standard "ALL" (no limit — returns nil, nil), or a constant integer
// expression evaluated immediately (LIMIT/OFFSET are resolved before
// execution, not per-row), e.g. a bare literal or arithmetic like "2 + 3".
// Non-constant expressions (column references, subqueries) are rejected
// with a clear error.
func (p *Parser) parseLimitOffsetValue(clause string) (*int, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "ALL" {
		p.next()
		return nil, nil
	}
	expr, err := p.parseAddSub()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", clause, err)
	}
	val, err := evalExpr(ExecEnv{}, expr, Row{})
	if err != nil {
		return nil, fmt.Errorf("%s must be a constant expression: %w", clause, err)
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("%s expects an integer, got %T", clause, val)
	}
	if f != math.Trunc(f) {
		return nil, fmt.Errorf("%s expects an integer, got %v", clause, f)
	}
	n := int(f)
	if n < 0 {
		return nil, fmt.Errorf("%s must be non-negative, got %d", clause, n)
	}
	return &n, nil
}

func (p *Parser) parseUnionClause(sel *Select) error {
	for p.cur.Typ == tKeyword && (p.cur.Val == "UNION" || p.cur.Val == "EXCEPT" || p.cur.Val == "INTERSECT") {
		unionType := UnionDistinct
		switch p.cur.Val {
		case "UNION":
			p.next()
			if p.cur.Typ == tKeyword && p.cur.Val == "ALL" {
				unionType = UnionAll
				p.next()
			}
		case "EXCEPT":
			unionType = Except
			p.next()
		case "INTERSECT":
			unionType = Intersect
			p.next()
		}

		// Parse the right-hand SELECT
		rightSelect, err := p.parseSelect()
		if err != nil {
			return err
		}

		// Create the union clause
		unionClause := &UnionClause{
			Type:  unionType,
			Right: rightSelect,
		}

		// Find the end of the union chain and append
		if sel.Union == nil {
			sel.Union = unionClause
		} else {
			current := sel.Union
			for current.Next != nil {
				current = current.Next
			}
			current.Next = unionClause
		}
	}
	return nil
}

func (p *Parser) parseJoinTail() (FromItem, Expr, error) {
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		return p.parseJoinSubselectTail()
	}
	return p.parseJoinTableOrFunctionTail()
}

func (p *Parser) parseJoinSubselectTail() (FromItem, Expr, error) {
	p.next()
	subSel, err := p.parseSelect()
	if err != nil {
		return FromItem{}, nil, err
	}
	if p.cur.Typ != tSymbol || p.cur.Val != ")" {
		return FromItem{}, nil, p.errf("expected ) after subselect in JOIN")
	}
	p.next()
	alias, err := p.parseRequiredAlias("expected alias after AS for subselect", "expected alias for subselect in JOIN")
	if err != nil {
		return FromItem{}, nil, err
	}
	on, err := p.parseJoinOnExpr()
	if err != nil {
		return FromItem{}, nil, err
	}
	return FromItem{Subquery: subSel, Alias: alias}, on, nil
}

func (p *Parser) parseJoinTableOrFunctionTail() (FromItem, Expr, error) {
	rt := p.parseQualifiedIdentLike()
	if rt == "" {
		return FromItem{}, nil, p.errf("expected table or table-valued function")
	}

	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		fcExpr, err := p.parseFuncCallWithName(rt)
		if err != nil {
			return FromItem{}, nil, err
		}
		fc, ok := fcExpr.(*FuncCall)
		if !ok {
			return FromItem{}, nil, p.errf("internal: expected FuncCall for table function %q", rt)
		}
		if fc.Over != nil {
			return FromItem{}, nil, p.errf("OVER clause not allowed for table-valued functions in JOIN")
		}
		alias, err := p.parseOptionalAlias(rt, "expected alias")
		if err != nil {
			return FromItem{}, nil, err
		}
		on, err := p.parseJoinOnExpr()
		if err != nil {
			return FromItem{}, nil, err
		}
		return FromItem{TableFunc: &TableFuncCall{Name: rt, Args: fc.Args, Alias: alias}, Alias: alias}, on, nil
	}

	alias, err := p.parseOptionalAlias(rt, "expected alias")
	if err != nil {
		return FromItem{}, nil, err
	}
	on, err := p.parseJoinOnExpr()
	if err != nil {
		return FromItem{}, nil, err
	}
	return FromItem{Table: rt, Alias: alias}, on, nil
}

func (p *Parser) parseJoinOnExpr() (Expr, error) {
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	on, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return on, nil
}
