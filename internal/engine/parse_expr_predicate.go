// Parsing the predicate levels of an expression, loosest first: OR, AND, NOT,
// IS NULL, and the comparisons — including BETWEEN, IN, LIKE, REGEXP and
// SIMILAR TO.
package engine

// Expressions
func (p *Parser) parseExpr() (Expr, error) {
	if err := p.enterRecursion(); err != nil {
		return nil, err
	}
	defer p.exitRecursion()
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tKeyword && p.cur.Val == "OR" {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "OR", Left: l, Right: r}
	}
	return l, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tKeyword && p.cur.Val == "AND" {
		p.next()
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "AND", Left: l, Right: r}
	}
	return l, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
		p.next()
		e, err := p.parseIsNull()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: "NOT", Expr: e}, nil
	}
	return p.parseIsNull()
}

func (p *Parser) parseIsNull() (Expr, error) {
	l, err := p.parseCmp()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "IS" {
		p.next()
		neg := false
		if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
			neg = true
			p.next()
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "NULL" {
			p.next()
			return &IsNull{Expr: l, Negate: neg}, nil
		}
		return nil, p.errf("expected NULL after IS/IS NOT")
	}
	return l, nil
}

//nolint:gocyclo // parseCmp handles many comparison operator permutations.
func (p *Parser) parseCmp() (Expr, error) {
	l, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	for {
		negate := p.consumeCmpNot()
		next, matched, err := p.parseCmpTail(l, negate)
		if err != nil {
			return nil, err
		}
		if !matched {
			break
		}
		l = next
	}

	return l, nil
}

func (p *Parser) consumeCmpNot() bool {
	if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
		p.next()
		return true
	}
	return false
}

func (p *Parser) parseCmpTail(l Expr, negate bool) (Expr, bool, error) {
	if expr, ok, err := p.parseCmpBetween(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpIn(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpLike(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpRegexp(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpSimilar(l, negate); ok || err != nil {
		return expr, ok, err
	}
	return p.parseCmpSymbol(l)
}

func (p *Parser) parseCmpBetween(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "BETWEEN" {
		return nil, false, nil
	}
	p.next()
	lo, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	if p.cur.Typ != tKeyword || p.cur.Val != "AND" {
		return nil, true, p.errf("expected AND after BETWEEN lower bound")
	}
	p.next()
	hi, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	switch l.(type) {
	case *VarRef, *Literal:
		// Desugar plain column/literal comparands: re-evaluating them is
		// free and the raw fast paths compile these Binary trees into
		// tight filters.
		if negate {
			return &Binary{Op: "OR",
				Left:  &Binary{Op: "<", Left: l, Right: lo},
				Right: &Binary{Op: ">", Left: l, Right: hi},
			}, true, nil
		}
		return &Binary{Op: "AND",
			Left:  &Binary{Op: ">=", Left: l, Right: lo},
			Right: &Binary{Op: "<=", Left: l, Right: hi},
		}, true, nil
	}
	// Complex comparand (function call, subquery, arithmetic, ...):
	// evaluate it once via a dedicated node.
	return &BetweenExpr{Expr: l, Lo: lo, Hi: hi, Negate: negate}, true, nil
}

func (p *Parser) parseCmpIn(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "IN" {
		return nil, false, nil
	}
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return nil, true, err
	}
	var values []Expr
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, true, err
		}
		values = append(values, e)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, true, err
	}
	return &InExpr{Expr: l, Values: values, Negate: negate}, true, nil
}

func (p *Parser) parseCmpLike(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword {
		return nil, false, nil
	}
	switch p.cur.Val {
	case "LIKE":
		p.next()
		pattern, escape, err := p.parseCmpPatternAndEscape()
		if err != nil {
			return nil, true, err
		}
		return &LikeExpr{Expr: l, Pattern: pattern, Escape: escape, Negate: negate}, true, nil
	case "ILIKE":
		p.next()
		pattern, escape, err := p.parseCmpPatternAndEscape()
		if err != nil {
			return nil, true, err
		}
		return &LikeExpr{Expr: l, Pattern: pattern, Escape: escape, Negate: negate, CaseInsensitive: true}, true, nil
	case "GLOB":
		p.next()
		pattern, err := p.parseAddSub()
		if err != nil {
			return nil, true, err
		}
		return &LikeExpr{Expr: l, Pattern: pattern, Negate: negate, GlobStyle: true}, true, nil
	default:
		return nil, false, nil
	}
}

func (p *Parser) parseCmpPatternAndEscape() (Expr, Expr, error) {
	pattern, err := p.parseAddSub()
	if err != nil {
		return nil, nil, err
	}
	var escape Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "ESCAPE" {
		p.next()
		escape, err = p.parseAddSub()
		if err != nil {
			return nil, nil, err
		}
	}
	return pattern, escape, nil
}

func (p *Parser) parseCmpRegexp(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || (p.cur.Val != "REGEXP" && p.cur.Val != "RLIKE") {
		return nil, false, nil
	}
	p.next()
	pattern, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	return &RegexpExpr{Expr: l, Pattern: pattern, Negate: negate}, true, nil
}

func (p *Parser) parseCmpSimilar(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "SIMILAR" {
		return nil, false, nil
	}
	p.next()
	if err := p.expectKeyword("TO"); err != nil {
		return nil, true, err
	}
	pattern, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	return &RegexpExpr{Expr: l, Pattern: pattern, Negate: negate, SimilarTo: true}, true, nil
}

func (p *Parser) parseCmpSymbol(l Expr) (Expr, bool, error) {
	if p.cur.Typ != tSymbol {
		return nil, false, nil
	}
	switch p.cur.Val {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		op := p.cur.Val
		p.next()
		r, err := p.parseAddSub()
		if err != nil {
			return nil, true, err
		}
		return &Binary{Op: op, Left: l, Right: r}, true, nil
	default:
		return nil, false, nil
	}
}
