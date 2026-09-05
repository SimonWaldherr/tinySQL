// The statement dispatcher, and the statements small enough to parse in one
// place: CALL, EXPLAIN, ANALYZE and PRAGMA.
package engine

import (
	"strings"
)

// ParseStatement parses exactly one complete SQL statement into an AST. A
// single trailing semicolon is accepted; any remaining token is an error so a
// valid DML prefix can never silently ignore and execute before junk input.
func (p *Parser) ParseStatement() (Statement, error) {
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ == tSymbol && p.cur.Val == ";" {
		p.next()
	}
	if p.cur.Typ != tEOF {
		return nil, p.errf("unexpected token after statement")
	}
	return stmt, nil
}

// parseStatement parses one statement prefix without requiring EOF. It is
// used for individual statements inside CREATE TRIGGER ... BEGIN ... END;
// public callers must use ParseStatement above.
func (p *Parser) parseStatement() (Statement, error) {
	if p.cur.Typ == tIdent {
		return p.parseBareTableSelect()
	}
	if p.cur.Typ != tKeyword {
		return nil, p.errf("expected a statement")
	}

	switch p.cur.Val {
	case "EXPLAIN":
		return p.parseExplain()
	case "ANALYZE":
		return p.parseAnalyze()
	case "PRAGMA":
		return p.parsePragma()
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	case "ALTER":
		return p.parseAlter()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "CALL":
		return p.parseCallProcedure()
	case "REFRESH":
		return p.parseRefresh()
	case "GRANT":
		return p.parseGrantOrRevoke(true)
	case "REVOKE":
		return p.parseGrantOrRevoke(false)
	case "SELECT":
		return p.parseSelectWithCTE()
	case "WITH":
		return p.parseWithStatement()
	default:
		return p.parseBareTableSelect()
	}
}

func (p *Parser) parseCallProcedure() (Statement, error) {
	p.next()
	name := p.parseQualifiedIdentLike()
	if name == "" {
		return nil, p.errf("expected stored procedure name")
	}
	stmt := &CallProcedure{Name: name}
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		if p.cur.Typ != tSymbol || p.cur.Val != ")" {
			for {
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				stmt.Args = append(stmt.Args, arg)
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
	}
	return stmt, nil
}

func (p *Parser) parseExplain() (Statement, error) {
	p.next()
	analyze := false
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && upper(p.cur.Val) == "ANALYZE" {
		analyze = true
		p.next()
	}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	return &Explain{Statement: stmt, Analyze: analyze}, nil
}

func (p *Parser) parseAnalyze() (Statement, error) {
	p.next()
	stmt := &Analyze{}
	if p.cur.Typ != tEOF && (p.cur.Typ != tSymbol || p.cur.Val != ";") {
		stmt.Table = p.parseQualifiedIdentLike()
		if stmt.Table == "" {
			return nil, p.errf("expected table name after ANALYZE")
		}
	}
	return stmt, nil
}

func (p *Parser) parsePragma() (Statement, error) {
	p.next()
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected PRAGMA name")
	}
	schema := ""
	if p.cur.Typ == tSymbol && p.cur.Val == "." {
		schema = name
		p.next()
		name = p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected PRAGMA name after schema")
		}
	}

	stmt := &Pragma{Name: name, Schema: schema}
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		args, err := p.parsePragmaArgs()
		if err != nil {
			return nil, err
		}
		stmt.Args = args
	}
	if p.cur.Typ == tSymbol && p.cur.Val == "=" {
		p.next()
		value, err := p.parsePragmaValue()
		if err != nil {
			return nil, err
		}
		stmt.Value = &value
	}
	return stmt, nil
}

func (p *Parser) parsePragmaArgs() ([]string, error) {
	args := make([]string, 0, 1)
	var b strings.Builder
	for p.cur.Typ != tEOF {
		if p.cur.Typ == tSymbol && p.cur.Val == ")" {
			if strings.TrimSpace(b.String()) != "" {
				args = append(args, strings.TrimSpace(b.String()))
			}
			p.next()
			return args, nil
		}
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			args = append(args, strings.TrimSpace(b.String()))
			b.Reset()
			p.next()
			continue
		}
		b.WriteString(p.cur.Val)
		p.next()
	}
	return nil, p.errf("expected ')' after PRAGMA arguments")
}

func (p *Parser) parsePragmaValue() (string, error) {
	var b strings.Builder
	for p.cur.Typ != tEOF {
		if p.cur.Typ == tSymbol && p.cur.Val == ";" {
			break
		}
		b.WriteString(p.cur.Val)
		p.next()
	}
	value := strings.TrimSpace(b.String())
	if value == "" {
		return "", p.errf("expected PRAGMA value")
	}
	return value, nil
}
