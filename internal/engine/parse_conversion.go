package engine

import "strings"

// Type and datepart names are syntax, not column references. Keeping them as
// literals also avoids resolving them for every row in the raw evaluator.
func (p *Parser) parseConvertCall(name string) (Expr, error) {
	if p.cur.Typ != tIdent && p.cur.Typ != tKeyword && p.cur.Typ != tString {
		return nil, p.errf("%s expects a target type", name)
	}
	target := strings.ToUpper(p.cur.Val)
	p.next()
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		if p.cur.Typ != tNumber || numberLiteralIsFloat(p.cur.Val) {
			return nil, p.errf("%s expects an integer type length", name)
		}
		target += "(" + p.cur.Val + ")"
		p.next()
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
	}
	if err := p.expectSymbol(","); err != nil {
		return nil, err
	}
	value, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	args := []Expr{&Literal{Val: target}, value}
	if p.cur.Typ == tSymbol && p.cur.Val == "," {
		p.next()
		style, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, style)
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}
	return bindFuncHandler(&FuncCall{Name: name, Args: args}), nil
}

func (p *Parser) parseDatePartCall(name string) (Expr, error) {
	var part Expr
	if (p.cur.Typ == tIdent || p.cur.Typ == tKeyword) && p.peek.Typ == tSymbol && p.peek.Val == "," && p.cur.Val != "NULL" {
		part = &Literal{Val: strings.ToUpper(p.cur.Val)}
		p.next()
	} else {
		var err error
		part, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	if err := p.expectSymbol(","); err != nil {
		return nil, err
	}
	value, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}
	return bindFuncHandler(&FuncCall{Name: name, Args: []Expr{part, value}}), nil
}
