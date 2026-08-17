// Parsing a function call, and the OVER clause that turns one into a window
// function: PARTITION BY, ORDER BY and the frame.
package engine

import (
	"strconv"
	"strings"
)

func (p *Parser) parseFuncCall() (Expr, error) {
	name := p.cur.Val
	p.next()
	return p.parseFuncCallWithName(name)
}

//nolint:gocyclo // Function-call grammar involves numerous special cases.
func (p *Parser) parseFuncCallWithName(name string) (Expr, error) {
	// Normalize the function name once at parse time. SQL function names are
	// case-insensitive; evalFuncCall resolves handlers with an exact-match
	// lookup first and only then retries with strings.ToUpper — which, for a
	// lowercase-written call, costs an extra map lookup plus a string
	// allocation on every evaluation, i.e. once per row in scans. Uppercasing
	// here makes the first lookup hit for every spelling.
	name = strings.ToUpper(name)
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}

	// Handle CAST(expr AS type) specially
	if name == "CAST" {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		// Parse the type as an identifier/keyword
		if p.cur.Typ != tKeyword && p.cur.Typ != tIdent {
			return nil, p.errf("expected type name after AS")
		}
		typeName := p.cur.Val
		p.next()
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		// Return CAST as a function with the type as a literal string
		return bindFuncHandler(&FuncCall{Name: name, Args: []Expr{expr, &Literal{Val: typeName}}}), nil
	}

	// Handle COUNT(*)
	if name == "COUNT" && p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		return bindFuncHandler(&FuncCall{Name: name, Star: true}), nil
	}

	// Check for DISTINCT keyword after opening parenthesis
	distinct := false
	if p.cur.Typ == tKeyword && p.cur.Val == "DISTINCT" {
		distinct = true
		p.next()
	}

	var args []Expr
	if p.cur.Typ != tSymbol || p.cur.Val != ")" {
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, e)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}

	// Check for OVER clause (window functions)
	var overClause *OverClause
	if p.cur.Typ == tKeyword && p.cur.Val == "OVER" {
		p.next()
		oc, err := p.parseOverClause()
		if err != nil {
			return nil, err
		}
		overClause = oc
	}

	return foldConstFuncCall(bindFuncHandler(&FuncCall{Name: name, Args: args, Distinct: distinct, Over: overClause})), nil
}

// parseOverClause parses the OVER (PARTITION BY ... ORDER BY ... frame) clause
func (p *Parser) parseOverClause() (*OverClause, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}

	oc := &OverClause{}
	if err := p.parseOverPartitionBy(oc); err != nil {
		return nil, err
	}
	if err := p.parseOverOrderBy(oc); err != nil {
		return nil, err
	}
	if err := p.parseOverFrame(oc); err != nil {
		return nil, err
	}

	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}

	return oc, nil
}

func (p *Parser) parseOverPartitionBy(oc *OverClause) error {
	if p.cur.Typ != tKeyword || p.cur.Val != "PARTITION" {
		return nil
	}
	p.next()
	if err := p.expectKeyword("BY"); err != nil {
		return err
	}
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return err
		}
		oc.PartitionBy = append(oc.PartitionBy, expr)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		return nil
	}
}

func (p *Parser) parseOverOrderBy(oc *OverClause) error {
	if p.cur.Typ != tKeyword || p.cur.Val != "ORDER" {
		return nil
	}
	p.next()
	if err := p.expectKeyword("BY"); err != nil {
		return err
	}
	for {
		item, err := p.parseOverOrderItem()
		if err != nil {
			return err
		}
		oc.OrderBy = append(oc.OrderBy, item)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		return nil
	}
}

func (p *Parser) parseOverOrderItem() (OrderItem, error) {
	if p.cur.Typ != tIdent && p.cur.Typ != tKeyword {
		return OrderItem{}, p.errf("expected column name in ORDER BY")
	}
	col := p.cur.Val
	p.next()

	desc := false
	if p.cur.Typ == tKeyword && (p.cur.Val == "DESC" || p.cur.Val == "ASC") {
		desc = p.cur.Val == "DESC"
		p.next()
	}
	return OrderItem{Col: col, Desc: desc}, nil
}

func (p *Parser) parseOverFrame(oc *OverClause) error {
	if p.cur.Typ != tKeyword || (p.cur.Val != "ROWS" && p.cur.Val != "RANGE") {
		return nil
	}
	frame, err := p.parseWindowFrame()
	if err != nil {
		return err
	}
	oc.Frame = frame
	return nil
}

// parseWindowFrame parses ROWS/RANGE BETWEEN ... AND ...
func (p *Parser) parseWindowFrame() (*WindowFrame, error) {
	frame := &WindowFrame{}

	// ROWS or RANGE
	frame.Mode = p.cur.Val
	p.next()

	// BETWEEN keyword
	if p.cur.Typ == tKeyword && p.cur.Val == "BETWEEN" {
		p.next()

		// Parse start bound
		startType, startValue, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.StartType = startType
		frame.StartValue = startValue

		// AND keyword
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}

		// Parse end bound
		endType, endValue, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.EndType = endType
		frame.EndValue = endValue
	} else {
		// Simple form: ROWS n PRECEDING, etc.
		startType, startValue, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		frame.StartType = startType
		frame.StartValue = startValue
		frame.EndType = "CURRENT"
		frame.EndValue = 0
	}

	return frame, nil
}

// parseFrameBound parses a single frame bound: UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, n PRECEDING/FOLLOWING
func (p *Parser) parseFrameBound() (string, int, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "UNBOUNDED" {
		p.next()
		if p.cur.Typ != tKeyword || (p.cur.Val != "PRECEDING" && p.cur.Val != "FOLLOWING") {
			return "", 0, p.errf("expected PRECEDING or FOLLOWING after UNBOUNDED")
		}
		direction := p.cur.Val
		p.next()
		if direction == "PRECEDING" {
			return "UNBOUNDED_PRECEDING", 0, nil
		}
		return "UNBOUNDED_FOLLOWING", 0, nil
	}

	if p.cur.Typ == tKeyword && p.cur.Val == "CURRENT" {
		p.next()
		if p.cur.Typ == tKeyword && p.cur.Val == "ROW" {
			p.next()
		}
		return "CURRENT", 0, nil
	}

	// n PRECEDING/FOLLOWING
	if p.cur.Typ == tNumber {
		value := p.cur.Val
		p.next()

		// Parse the value as an integer. strconv.Atoi is allocation-free and,
		// unlike fmt.Sscanf("%d"), rejects a fractional token like "1.5" rather
		// than silently truncating it to 1.
		n, err := strconv.Atoi(value)
		if err != nil {
			return "", 0, p.errf("invalid frame offset: %s", value)
		}

		if p.cur.Typ != tKeyword || (p.cur.Val != "PRECEDING" && p.cur.Val != "FOLLOWING") {
			return "", 0, p.errf("expected PRECEDING or FOLLOWING after offset")
		}

		direction := p.cur.Val
		p.next()

		if direction == "PRECEDING" {
			return "OFFSET_PRECEDING", n, nil
		}
		return "OFFSET_FOLLOWING", n, nil
	}

	return "", 0, p.errf("invalid frame bound")
}
