package engine

import (
	"fmt"
	"strconv"
	"strings"

	"tinysql/internal/storage"
)

type Parser struct {
	lx   *Lexer
	cur  Token
	peek Token
}

func NewParser(sql string) *Parser {
	p := &Parser{lx: newLexer(sql)}
	p.cur = p.lx.Next()
	p.peek = p.lx.Next()
	return p
}
func (p *Parser) next() { p.cur, p.peek = p.peek, p.lx.Next() }
func (p *Parser) expectSymbol(sym string) error {
	if p.cur.Typ == tSymbol && p.cur.Val == sym {
		p.next()
		return nil
	}
	return p.errf("expected symbol %q", sym)
}
func (p *Parser) expectKeyword(kw string) error {
	if p.cur.Typ == tKeyword && p.cur.Val == kw {
		p.next()
		return nil
	}
	return p.errf("expected keyword %q", kw)
}
func (p *Parser) errf(format string, a ...any) error {
	return fmt.Errorf("parse error near %q: %s", p.cur.Val, fmt.Sprintf(format, a...))
}

// ------------------------------ AST ------------------------------

type Expr interface{}

type (
	VarRef  struct{ Name string }
	Literal struct{ Val any }
	Unary   struct {
		Op   string
		Expr Expr
	}
	Binary struct {
		Op          string
		Left, Right Expr
	}
	IsNull struct {
		Expr   Expr
		Negate bool
	}
	FuncCall struct {
		Name string
		Args []Expr
		Star bool
	}
)

type Statement interface{}

type CreateTable struct {
	Name     string
	Cols     []storage.Column
	IsTemp   bool
	AsSelect *Select
}
type DropTable struct{ Name string }
type Insert struct {
	Table string
	Cols  []string
	Vals  []Expr
}
type Update struct {
	Table string
	Sets  map[string]Expr
	Where Expr
}
type Delete struct {
	Table string
	Where Expr
}

type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
)

type Select struct {
	Distinct bool
	From     FromItem
	Joins    []JoinClause
	Projs    []SelectItem
	Where    Expr
	GroupBy  []VarRef
	Having   Expr
	OrderBy  []OrderItem
	Limit    *int
	Offset   *int
}
type FromItem struct{ Table, Alias string }
type JoinClause struct {
	Type  JoinType
	Right FromItem
	On    Expr
}
type SelectItem struct {
	Expr  Expr
	Alias string
	Star  bool
}
type OrderItem struct {
	Col  string
	Desc bool
}

// ------------------------------ Parse ------------------------------

func (p *Parser) ParseStatement() (Statement, error) {
	switch {
	case p.cur.Typ == tKeyword && p.cur.Val == "CREATE":
		return p.parseCreate()
	case p.cur.Typ == tKeyword && p.cur.Val == "DROP":
		return p.parseDrop()
	case p.cur.Typ == tKeyword && p.cur.Val == "INSERT":
		return p.parseInsert()
	case p.cur.Typ == tKeyword && p.cur.Val == "UPDATE":
		return p.parseUpdate()
	case p.cur.Typ == tKeyword && p.cur.Val == "DELETE":
		return p.parseDelete()
	case p.cur.Typ == tKeyword && p.cur.Val == "SELECT":
		return p.parseSelect()
	default:
		return nil, p.errf("expected a statement")
	}
}

func (p *Parser) parseCreate() (Statement, error) {
	p.next()
	isTemp := false
	if p.cur.Typ == tKeyword && p.cur.Val == "TEMP" {
		isTemp = true
		p.next()
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected table name")
	}
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		cols, err := p.parseColumnDefs()
		if err != nil {
			return nil, err
		}
		return &CreateTable{Name: name, Cols: cols, IsTemp: isTemp}, nil
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &CreateTable{Name: name, IsTemp: isTemp, AsSelect: sel}, nil
	}
	return nil, p.errf("expected '(' or AS SELECT")
}

func (p *Parser) parseDrop() (Statement, error) {
	p.next()
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected table name")
	}
	return &DropTable{Name: name}, nil
}

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
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
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
	return &Insert{Table: tname, Cols: cols, Vals: vals}, nil
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
	return &Update{Table: tname, Sets: sets, Where: where}, nil
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
	return &Delete{Table: tname, Where: where}, nil
}

func (p *Parser) parseSelect() (*Select, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &Select{}
	if p.cur.Typ == tKeyword && p.cur.Val == "DISTINCT" {
		sel.Distinct = true
		p.next()
	}
	if p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		sel.Projs = append(sel.Projs, SelectItem{Star: true})
	} else {
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			alias := ""
			if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
				p.next()
				alias = p.parseIdentLike()
				if alias == "" {
					return nil, p.errf("expected alias")
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
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	from := p.parseIdentLike()
	if from == "" {
		return nil, p.errf("expected table")
	}
	alias := from
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias = p.parseIdentLike()
		if alias == "" {
			return nil, p.errf("expected alias")
		}
	} else if p.cur.Typ == tIdent {
		alias = p.cur.Val
		p.next()
	}
	sel.From = FromItem{Table: from, Alias: alias}
	for {
		if p.cur.Typ == tKeyword && p.cur.Val == "JOIN" {
			p.next()
			rtbl, ralias, on, err := p.parseJoinTail()
			if err != nil {
				return nil, err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: JoinInner, Right: FromItem{Table: rtbl, Alias: ralias}, On: on})
			continue
		}
		if p.cur.Typ == tKeyword && (p.cur.Val == "LEFT" || p.cur.Val == "RIGHT") {
			jt := JoinLeft
			if p.cur.Val == "RIGHT" {
				jt = JoinRight
			}
			p.next()
			if p.cur.Typ == tKeyword && p.cur.Val == "OUTER" {
				p.next()
			}
			if err := p.expectKeyword("JOIN"); err != nil {
				return nil, err
			}
			rtbl, ralias, on, err := p.parseJoinTail()
			if err != nil {
				return nil, err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: jt, Right: FromItem{Table: rtbl, Alias: ralias}, On: on})
			continue
		}
		break
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Where = e
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "GROUP" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			id := p.parseIdentLike()
			if id == "" {
				return nil, p.errf("GROUP BY expects column")
			}
			sel.GroupBy = append(sel.GroupBy, VarRef{Name: id})
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "HAVING" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Having = e
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "ORDER" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			col := p.parseIdentLike()
			if col == "" {
				return nil, p.errf("ORDER BY expects column")
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
	if p.cur.Typ == tKeyword && p.cur.Val == "LIMIT" {
		p.next()
		if n := p.parseIntLiteral(); n != nil {
			sel.Limit = n
		} else {
			return nil, p.errf("LIMIT expects integer")
		}
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		if n := p.parseIntLiteral(); n != nil {
			sel.Offset = n
		} else {
			return nil, p.errf("OFFSET expects integer")
		}
	}
	return sel, nil
}

func (p *Parser) parseJoinTail() (string, string, Expr, error) {
	rt := p.parseIdentLike()
	if rt == "" {
		return "", "", nil, p.errf("expected table")
	}
	alias := rt
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias = p.parseIdentLike()
		if alias == "" {
			return "", "", nil, p.errf("expected alias")
		}
	} else if p.cur.Typ == tIdent {
		alias = p.cur.Val
		p.next()
	}
	if err := p.expectKeyword("ON"); err != nil {
		return "", "", nil, err
	}
	on, err := p.parseExpr()
	if err != nil {
		return "", "", nil, err
	}
	return rt, alias, on, nil
}

func (p *Parser) parseColumnDefs() ([]storage.Column, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	var cols []storage.Column
	for {
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected column name")
		}
		typ := p.parseType()
		if typ < 0 {
			return nil, p.errf("unknown type for column %q", name)
		}
		
		col := storage.Column{
			Name: name,
			Type: typ,
			Constraint: storage.NoConstraint,
		}
		
		// Parse constraints
		if p.cur.Typ == tKeyword {
			switch p.cur.Val {
			case "PRIMARY":
				p.next()
				if p.cur.Typ == tKeyword && p.cur.Val == "KEY" {
					p.next()
					col.Constraint = storage.PrimaryKey
				}
			case "FOREIGN":
				p.next()
				if p.cur.Typ == tKeyword && p.cur.Val == "KEY" {
					p.next()
					col.Constraint = storage.ForeignKey
					// Parse REFERENCES table(column)
					if p.cur.Typ == tKeyword && p.cur.Val == "REFERENCES" {
						p.next()
						table := p.parseIdentLike()
						if table != "" && p.cur.Typ == tSymbol && p.cur.Val == "(" {
							p.next()
							column := p.parseIdentLike()
							if column != "" {
								p.expectSymbol(")")
								col.ForeignKey = &storage.ForeignKeyRef{Table: table, Column: column}
							}
						}
					}
				}
			case "UNIQUE":
				p.next()
				col.Constraint = storage.Unique
			case "REFERENCES":
				// Handle table-level REFERENCES for POINTER type
				if typ == storage.PointerType {
					p.next()
					table := p.parseIdentLike()
					if table != "" {
						col.PointerTable = table
					}
				}
			}
		}
		
		cols = append(cols, col)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		break
	}
	return cols, nil
}
func (p *Parser) parseType() storage.ColType {
	if p.cur.Typ == tKeyword {
		switch p.cur.Val {
		case "INT":
			p.next()
			return storage.IntType
		case "FLOAT":
			p.next()
			return storage.FloatType
		case "TEXT":
			p.next()
			return storage.TextType
		case "BOOL":
			p.next()
			return storage.BoolType
		case "JSON":
			p.next()
			return storage.JsonType
		case "DATE":
			p.next()
			return storage.DateType
		case "DATETIME":
			p.next()
			return storage.DateTimeType
		case "DURATION":
			p.next()
			return storage.DurationType
		case "COMPLEX":
			p.next()
			return storage.ComplexType
		case "POINTER":
			p.next()
			return storage.PointerType
		}
	}
	return -1
}
func (p *Parser) parseIdentLike() string {
	if p.cur.Typ == tIdent {
		s := p.cur.Val
		p.next()
		return s
	}
	return ""
}
func (p *Parser) parseIntLiteral() *int {
	if p.cur.Typ == tNumber && !strings.Contains(p.cur.Val, ".") {
		n, _ := strconv.Atoi(p.cur.Val)
		p.next()
		return &n
	}
	return nil
}

// Expressions
func (p *Parser) parseExpr() (Expr, error) { return p.parseOr() }
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
func (p *Parser) parseCmp() (Expr, error) {
	l, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	for {
		if p.cur.Typ == tSymbol {
			switch p.cur.Val {
			case "=", "!=", "<>", "<", "<=", ">", ">=":
				op := p.cur.Val
				p.next()
				r, err := p.parseAddSub()
				if err != nil {
					return nil, err
				}
				l = &Binary{Op: op, Left: l, Right: r}
				continue
			}
		}
		break
	}
	return l, nil
}
func (p *Parser) parseAddSub() (Expr, error) {
	l, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tSymbol && (p.cur.Val == "+" || p.cur.Val == "-") {
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
	for p.cur.Typ == tSymbol && (p.cur.Val == "*" || p.cur.Val == "/") {
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
		e, err := p.parsePrimary()
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
func (p *Parser) parsePrimary() (Expr, error) {
	switch p.cur.Typ {
	case tNumber:
		val := p.cur.Val
		p.next()
		if strings.Contains(val, ".") {
			f, _ := strconv.ParseFloat(val, 64)
			return &Literal{Val: f}, nil
		}
		n, _ := strconv.Atoi(val)
		return &Literal{Val: n}, nil
	case tString:
		s := p.cur.Val
		p.next()
		return &Literal{Val: s}, nil
	case tKeyword:
		switch p.cur.Val {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "NULLIF", "JSON_GET":
			return p.parseFuncCall()
		case "TRUE":
			p.next()
			return &Literal{Val: true}, nil
		case "FALSE":
			p.next()
			return &Literal{Val: false}, nil
		case "NULL":
			p.next()
			return &Literal{Val: nil}, nil
		default:
			return nil, p.errf("unexpected keyword %q", p.cur.Val)
		}
	case tIdent:
		name := p.cur.Val
		p.next()
		return &VarRef{Name: name}, nil
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
func (p *Parser) parseFuncCall() (Expr, error) {
	name := p.cur.Val
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	if name == "COUNT" && p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		return &FuncCall{Name: name, Star: true}, nil
	}
	var args []Expr
	if !(p.cur.Typ == tSymbol && p.cur.Val == ")") {
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
	return &FuncCall{Name: name, Args: args}, nil
}
