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
	onConflictDoNothing, err := p.parseInsertOnConflict()
	if err != nil {
		return nil, err
	}
	returning, err := p.parseReturningClause()
	if err != nil {
		return nil, err
	}
	return &Insert{Table: tname, Cols: cols, Rows: rows, Select: selectSource, OnConflictDoNothing: onConflictDoNothing, Returning: returning}, nil
}

// parseInsertOnConflict supports PostgreSQL's target-less conflict action.
// A conflict target and DO UPDATE need composite-constraint metadata that the
// current catalog does not yet represent, so reject them explicitly instead
// of silently applying a different conflict policy.
func (p *Parser) parseInsertOnConflict() (bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "ON" {
		return false, nil
	}
	p.next()
	if err := p.expectKeyword("CONFLICT"); err != nil {
		return false, err
	}
	if err := p.expectKeyword("DO"); err != nil {
		return false, err
	}
	if p.cur.Typ != tKeyword || p.cur.Val != "NOTHING" {
		return false, p.errf("only ON CONFLICT DO NOTHING is supported")
	}
	p.next()
	return true, nil
}

func (p *Parser) parseInsertValueRows() ([][]Expr, error) {
	var rows [][]Expr
	for {
		if err := p.expectSymbol("("); err != nil {
			return nil, err
		}
		var vals []Expr
		for {
			e, err := p.parseExpr()
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
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	return rows, nil
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
