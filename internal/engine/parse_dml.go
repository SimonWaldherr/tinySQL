// Parsing INSERT, UPDATE, DELETE and the RETURNING clause they share.
package engine

//nolint:gocyclo // INSERT parsing covers column lists and multi-row value sets.
func (p *Parser) parseInsert() (Statement, error) {
	p.next()
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	tname := p.parseIdentLike()
	if tname == "" {
		return nil, p.errf("expected table name")
	}
	var cols []string
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		for {
			id := p.parseIdentLike()
			if id == "" {
				return nil, p.errf("expected column name")
			}
			cols = append(cols, id)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			if err := p.expectSymbol(")"); err != nil {
				return nil, err
			}
			break
		}
	}
	var rows [][]Expr
	var selectSource *Select
	var err error
	switch p.cur.Val {
	case "VALUES":
		p.next()
		rows, err = p.parseInsertValueRows()
		if err != nil {
			return nil, err
		}
	case "SELECT", "WITH":
		selectSource, err = p.parseSelectWithCTE()
		if err != nil {
			return nil, err
		}
	default:
		return nil, p.errf("expected VALUES or SELECT")
	}
	onConflictDoNothing, onConflict, err := p.parseInsertOnConflict()
	if err != nil {
		return nil, err
	}
	returning, err := p.parseReturningClause()
	if err != nil {
		return nil, err
	}
	return &Insert{Table: tname, Cols: cols, Rows: rows, Select: selectSource, OnConflictDoNothing: onConflictDoNothing, OnConflict: onConflict, Returning: returning}, nil
}

func (p *Parser) parseInsertOnConflict() (bool, *OnConflictClause, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "ON" {
		return false, nil, nil
	}
	p.next()
	if err := p.expectKeyword("CONFLICT"); err != nil {
		return false, nil, err
	}
	var targetCols []string
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		for {
			id := p.parseIdentLike()
			if id == "" {
				return false, nil, p.errf("expected conflict target column name")
			}
			targetCols = append(targetCols, id)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			if err := p.expectSymbol(")"); err != nil {
				return false, nil, err
			}
			break
		}
	}
	if err := p.expectKeyword("DO"); err != nil {
		return false, nil, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "NOTHING" {
		p.next()
		if len(targetCols) == 0 {
			return true, nil, nil
		}
		return false, &OnConflictClause{TargetCols: targetCols, DoNothing: true}, nil
	}
	if p.cur.Typ != tKeyword || p.cur.Val != "UPDATE" {
		return false, nil, p.errf("expected NOTHING or UPDATE after ON CONFLICT DO")
	}
	p.next()
	if len(targetCols) == 0 {
		return false, nil, p.errf("ON CONFLICT DO UPDATE requires a conflict target")
	}
	if err := p.expectKeyword("SET"); err != nil {
		return false, nil, err
	}
	sets := map[string]Expr{}
	for {
		col := p.parseIdentLike()
		if col == "" {
			return false, nil, p.errf("expected column name")
		}
		if err := p.expectSymbol("="); err != nil {
			return false, nil, err
		}
		e, err := p.parseExpr()
		if err != nil {
			return false, nil, err
		}
		sets[col] = e
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	var where Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return false, nil, err
		}
		where = e
	}
	return false, &OnConflictClause{TargetCols: targetCols, DoUpdate: sets, Where: where}, nil
}

func (p *Parser) parseInsertValueRows() ([][]Expr, error) {
	var rows [][]Expr
	rowWidth := 0
	for {
		if err := p.expectSymbol("("); err != nil {
			return nil, err
		}
		// Most VALUES lists repeat the same number of columns. Once the
		// first row establishes that width, allocate subsequent rows once
		// instead of growing their expression slices for every column.
		vals := make([]Expr, 0, rowWidth)
		for {
			e, err := p.parseInsertValue()
			if err != nil {
				return nil, err
			}
			vals = append(vals, e)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			if err := p.expectSymbol(")"); err != nil {
				return nil, err
			}
			break
		}
		rows = append(rows, vals)
		rowWidth = len(vals)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	return rows, nil
}

// A literal followed by a VALUES delimiter cannot have an operator or
// suffix. Parse it directly while sharing the same conversion and depth
// limit as other expressions.
func (p *Parser) parseInsertValue() (Expr, error) {
	if p.depth < maxParseDepth && p.peek.Typ == tSymbol && (p.peek.Val == "," || p.peek.Val == ")") {
		switch p.cur.Typ {
		case tNumber, tString, tBlob:
			return p.parsePrimary()
		case tKeyword:
			switch p.cur.Val {
			case "NULL", "TRUE", "FALSE":
				return p.parsePrimary()
			}
		}
	}
	return p.parseExpr()
}

func (p *Parser) parseUpdate() (Statement, error) {
	p.next()
	tname := p.parseIdentLike()
	if tname == "" {
		return nil, p.errf("expected table name")
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	sets := map[string]Expr{}
	for {
		col := p.parseIdentLike()
		if col == "" {
			return nil, p.errf("expected column name")
		}
		if err := p.expectSymbol("="); err != nil {
			return nil, err
		}
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sets[col] = e
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	var where Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		where = e
	}
	returning, err := p.parseReturningClause()
	if err != nil {
		return nil, err
	}
	return &Update{Table: tname, Sets: sets, Where: where, Returning: returning}, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	p.next()
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	tname := p.parseIdentLike()
	if tname == "" {
		return nil, p.errf("expected table name")
	}
	var where Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		where = e
	}
	returning, err := p.parseReturningClause()
	if err != nil {
		return nil, err
	}
	return &Delete{Table: tname, Where: where, Returning: returning}, nil
}

func (p *Parser) parseReturningClause() ([]SelectItem, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "RETURNING" {
		return nil, nil
	}
	p.next()
	sel := &Select{simplePlanCache: &simpleSelectPlanCache{}}
	if err := p.parseProjections(sel); err != nil {
		return nil, err
	}
	return sel.Projs, nil
}
