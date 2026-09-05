// Parsing the operand levels of an expression: additive and multiplicative
// arithmetic, unary operators, and the primaries — literals, column references,
// parenthesised expressions, subqueries and CASE.
package engine

import (
	"strconv"
)

// parseAddSub parses the additive tier: + and -, plus the string
// concatenation operator ||.
//
// SQLite actually binds || tighter than * and /, so strictly speaking it
// belongs on its own tier between parseMulDiv and parseUnary. It lives here
// instead because the two placements can only disagree when concatenation and
// multiplicative arithmetic meet in one unparenthesised expression
// ("a || b * 2"), where SQLite multiplies the concatenated *text* — a shape
// that does not occur in real queries, and never in one whose author expected
// a meaningful result. Keeping || on an existing tier avoids adding a level to
// the precedence chain, which every nested expression pays for twice: once in
// call depth and once against maxParseDepth.
func (p *Parser) parseAddSub() (Expr, error) {
	l, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tSymbol && (p.cur.Val == "+" || p.cur.Val == "-" || p.cur.Val == "||") {
		op := p.cur.Val
		p.next()
		r, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: op, Left: l, Right: r}
	}
	return l, nil
}

func (p *Parser) parseMulDiv() (Expr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tSymbol && (p.cur.Val == "*" || p.cur.Val == "/" || p.cur.Val == "%") {
		op := p.cur.Val
		p.next()
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: op, Left: l, Right: r}
	}
	return l, nil
}

func (p *Parser) parseUnary() (Expr, error) {
	if p.cur.Typ == tSymbol && (p.cur.Val == "+" || p.cur.Val == "-") {
		op := p.cur.Val
		p.next()
		// Recurse into parseUnary so stacked prefix operators chain, e.g.
		// `- -1` or `- +x`. Precedence relative to * / + is unaffected because
		// parseMulDiv still enters operands through parseUnary.
		e, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: op, Expr: e}, nil
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
		p.next()
		e, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: "NOT", Expr: e}, nil
	}
	return p.parsePrimary()
}

// numberLiteralIsFloat reports whether a tNumber token's text can only parse
// as a float: it contains '.', 'e' or 'E'. strconv.Atoi/ParseInt require
// every byte to be a decimal digit (plus an optional leading sign the lexer
// never includes in a tNumber token), so any of these three bytes makes
// integer parsing an unconditional failure -- checked here without calling
// into strconv at all, let alone paying for its allocating error path twice.
func numberLiteralIsFloat(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.', 'e', 'E':
			return true
		}
	}
	return false
}

//nolint:gocyclo // Primary expression parsing covers numerous literal and sub-expression forms.
func (p *Parser) parsePrimary() (Expr, error) {
	switch p.cur.Typ {
	case tNumber:
		val := p.cur.Val
		p.next()
		// Try platform int first (most common). Fall back to int64 before float
		// so integers that exceed a 32-bit int (on wasm32/TinyGo) keep exact
		// integer semantics instead of silently becoming float64.
		//
		// A literal containing '.', 'e' or 'E' can never parse as an integer
		// -- the lexer only emits digits, at most one '.', and an optional
		// exponent into a tNumber token (see tokenizeNumber) -- so checking
		// for those bytes first (cheap, no allocation) skips straight to
		// ParseFloat instead of paying for two guaranteed-failing Atoi/
		// ParseInt calls first. Both failure paths allocate a *strconv.NumError,
		// so every float literal parsed (e.g. every row of a bulk INSERT into
		// a FLOAT column) used to cost two wasted allocations before this.
		if !numberLiteralIsFloat(val) {
			if n, err := strconv.Atoi(val); err == nil {
				return &Literal{Val: n}, nil
			}
			if n64, err := strconv.ParseInt(val, 10, 64); err == nil {
				return &Literal{Val: n64}, nil
			}
		}
		f, _ := strconv.ParseFloat(val, 64)
		return &Literal{Val: f}, nil
	case tString:
		s := p.cur.Val
		p.next()
		return &Literal{Val: s}, nil
	case tBlob:
		encoded := p.cur.Val
		decodeErr := p.cur.Err
		p.next()
		if decodeErr != "" {
			return nil, p.errf("invalid BLOB hex literal: %s", decodeErr)
		}
		// token.Val uses a string only because token values are textual. Copy
		// it into a byte slice so the AST owns binary data and cannot alias the
		// SQL input buffer.
		blob := append([]byte(nil), []byte(encoded)...)
		return &Literal{Val: blob}, nil
	case tKeyword:
		// Handle explicit keywords that are not identifiers first.
		switch p.cur.Val {
		case "CASE":
			return p.parseCaseExpr()
		case "SELECT":
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			return &SubqueryExpr{Select: sel}, nil
		case "EXISTS":
			p.next() // consume EXISTS
			if err := p.expectSymbol("("); err != nil {
				return nil, err
			}
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if err := p.expectSymbol(")"); err != nil {
				return nil, err
			}
			return &ExistsExpr{Select: sel}, nil
		case "TRUE":
			p.next()
			return &Literal{Val: true}, nil
		case "FALSE":
			p.next()
			return &Literal{Val: false}, nil
		case "NULL":
			p.next()
			return &Literal{Val: nil}, nil
		}

		// If the keyword is followed by '(' treat it as a function call; otherwise
		// accept keywords as identifier-like (e.g., a column named TIMESTAMP).
		if p.peek.Typ == tSymbol && p.peek.Val == "(" {
			return p.parseFuncCall()
		}

		// Otherwise treat the keyword as a variable/column reference
		name := p.cur.Val
		p.next()
		return newVarRef(name), nil
	case tIdent:
		name := p.cur.Val
		p.next()
		// Check if it's a function call
		if p.cur.Typ == tSymbol && p.cur.Val == "(" {
			// This is a function call with an identifier
			// Put the current position back and parse as function
			return p.parseFuncCallWithName(name)
		}
		return newVarRef(name), nil
	case tSymbol:
		if p.cur.Val == "(" {
			p.next()
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if err := p.expectSymbol(")"); err != nil {
				return nil, err
			}
			return e, nil
		}
	}

	return nil, p.errf("unexpected token %q", p.cur.Val)
}

//nolint:gocyclo // CASE parsing naturally involves multiple WHEN/ELSE branches.
func (p *Parser) parseCaseExpr() (Expr, error) {
	p.next() // consume CASE
	var operand Expr
	var err error
	if p.cur.Typ != tKeyword || p.cur.Val != "WHEN" {
		operand, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	whens := make([]CaseWhen, 0, 2)
	for {
		if err := p.expectKeyword("WHEN"); err != nil {
			return nil, err
		}
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return nil, err
		}
		res, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		whens = append(whens, CaseWhen{When: cond, Then: res})
		if p.cur.Typ != tKeyword || p.cur.Val != "WHEN" {
			break
		}
	}
	var elseExpr Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "ELSE" {
		p.next()
		elseExpr, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	if p.cur.Typ != tKeyword || p.cur.Val != "END" {
		return nil, p.errf("expected END to close CASE expression")
	}
	p.next()
	return &CaseExpr{Operand: operand, Whens: whens, Else: elseExpr}, nil
}
