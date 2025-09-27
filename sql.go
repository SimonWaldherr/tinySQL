// TinySQL - a small educational SQL engine using only Go's standard library.
// Focus: clarity & reliability over performance and SQL coverage.
// Supported (subset):
//   - CREATE TABLE name (col TYPE, ...)
//   - CREATE TEMP TABLE name AS SELECT ...
//   - DROP TABLE name
//   - INSERT INTO name (c1, c2, ...) VALUES (v1, v2, ...)
//   - UPDATE name SET c = expr [, ...] WHERE expr
//   - DELETE FROM name WHERE expr
//   - SELECT [*, expr [AS alias], ...]
//       FROM t [AS a] [JOIN u [AS b] ON expr]...
//       [WHERE expr]
//       [GROUP BY col[, ...]]
//       [HAVING expr]
//       [ORDER BY col [ASC|DESC][, ...]]
//       [LIMIT n] [OFFSET n]
//
// Aggregates: COUNT, COUNT(*), SUM, AVG, MIN, MAX
// Types: INT, FLOAT, TEXT, BOOL
//
// Notes & limits:
//   - SQL subset intentionally small. No subqueries; only INNER JOIN.
//   - GROUP BY expects column refs (no full expression grouping).
//   - SELECT * expands to alias.col names (or table.col if no alias).
//   - NULL semantics are minimal; IS NULL not implemented.
//   - Error messages aim to be helpful and localized.

package tinysql

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type ColType int

const (
	IntType ColType = iota
	FloatType
	TextType
	BoolType
)

func (t ColType) String() string {
	switch t {
	case IntType:
		return "INT"
	case FloatType:
		return "FLOAT"
	case TextType:
		return "TEXT"
	case BoolType:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}

type Column struct {
	Name string
	Type ColType
}

type Table struct {
	Name    string
	Cols    []Column
	Rows    [][]interface{} // each row len == len(Cols)
	IsTemp  bool
	colPos  map[string]int // lower-cased column name -> index
	Version int            // bump on write to help debug
}

func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int)
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp}
}

func (t *Table) colIndex(name string) (int, error) {
	i, ok := t.colPos[strings.ToLower(name)]
	if !ok {
		return -1, fmt.Errorf("unknown column %q on table %q", name, t.Name)
	}
	return i, nil
}

type DB struct {
	tables map[string]*Table // lower-cased name -> table
}

func NewDB() *DB { return &DB{tables: make(map[string]*Table)} }

func (db *DB) Get(name string) (*Table, error) {
	t, ok := db.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("no such table %q", name)
	}
	return t, nil
}

func (db *DB) Put(t *Table) error {
	_, exists := db.tables[strings.ToLower(t.Name)]
	if exists {
		return fmt.Errorf("table %q already exists", t.Name)
	}
	db.tables[strings.ToLower(t.Name)] = t
	return nil
}

func (db *DB) Drop(name string) error {
	_, ok := db.tables[strings.ToLower(name)]
	if !ok {
		return fmt.Errorf("no such table %q", name)
	}
	delete(db.tables, strings.ToLower(name))
	return nil
}

// TokenType is intentionally small. Keywords come as Keyword with .Val uppercased.
type TokenType int

const (
	tEOF TokenType = iota
	tIdent
	tNumber
	tString
	tSymbol // punctuation/operators (, ) , * + - / = != <> <= >= < > .
	tKeyword
)

type Token struct {
	Typ TokenType
	Val string
	Pos int // byte offset in input for error context
}

type Lexer struct {
	s   string
	pos int
}

func newLexer(s string) *Lexer { return &Lexer{s: s, pos: 0} }

func (lx *Lexer) peek() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	r, _ := utf8DecodeRuneInString(lx.s[lx.pos:])
	return r
}

func (lx *Lexer) next() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	r, size := utf8DecodeRuneInString(lx.s[lx.pos:])
	lx.pos += size
	return r
}

func (lx *Lexer) emit(tt TokenType, start int, val string) Token {
	return Token{Typ: tt, Val: val, Pos: start}
}

func (lx *Lexer) skipWhitespace() {
	for {
		r := lx.peek()
		if r == 0 {
			return
		}
		if unicode.IsSpace(r) {
			lx.next()
			continue
		}
		// SQL comments: -- line, /* block */
		if r == '-' && lx.peekN(1) == '-' {
			lx.next()
			lx.next()
			for {
				r2 := lx.next()
				if r2 == 0 || r2 == '\n' {
					break
				}
			}
			continue
		}
		if r == '/' && lx.peekN(1) == '*' {
			lx.next()
			lx.next()
			for {
				r2 := lx.next()
				if r2 == 0 {
					return
				}
				if r2 == '*' && lx.peek() == '/' {
					lx.next()
					break
				}
			}
			continue
		}
		return
	}
}

func (lx *Lexer) peekN(n int) rune {
	p := lx.pos
	for i := 0; i < n; i++ {
		if p >= len(lx.s) {
			return 0
		}
		_, sz := utf8DecodeRuneInString(lx.s[p:])
		p += sz
	}
	if p >= len(lx.s) {
		return 0
	}
	r, _ := utf8DecodeRuneInString(lx.s[p:])
	return r
}

func (lx *Lexer) NextToken() Token {
	lx.skipWhitespace()
	start := lx.pos
	r := lx.peek()
	if r == 0 {
		return lx.emit(tEOF, start, "")
	}
	// Strings: single quotes '...'
	if r == '\'' {
		lx.next() // consume opening
		var sb strings.Builder
	loop:
		for {
			ch := lx.next()
			if ch == 0 {
				return lx.emit(tString, start, sb.String())
			}
			if ch == '\'' {
				// escape: '' -> '
				if lx.peek() == '\'' {
					lx.next()
					sb.WriteRune('\'')
					continue
				}
				break loop
			}
			sb.WriteRune(ch)
		}
		return lx.emit(tString, start, sb.String())
	}
	// Numbers: simple [0-9]+(.[0-9]+)?
	if unicode.IsDigit(r) {
		var sb strings.Builder
		dot := false
		for unicode.IsDigit(lx.peek()) || (!dot && lx.peek() == '.') {
			if lx.peek() == '.' {
				dot = true
			}
			sb.WriteRune(lx.next())
		}
		return lx.emit(tNumber, start, sb.String())
	}
	// Identifiers / keywords: [A-Za-z_][A-Za-z0-9_]*
	if unicode.IsLetter(r) || r == '_' {
		var sb strings.Builder
		for {
			ch := lx.peek()
			if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
				sb.WriteRune(lx.next())
			} else {
				break
			}
		}
		val := sb.String()
		up := strings.ToUpper(val)
		if isKeyword(up) {
			return lx.emit(tKeyword, start, up)
		}
		return lx.emit(tIdent, start, val)
	}
	// Symbols and 2-char operators
	switch r {
	case '(', ')', ',', '*', '+', '-', '/', '.', ';':
		lx.next()
		return lx.emit(tSymbol, start, string(r))
	case '=', '<', '>', '!':
		a := lx.next()
		b := lx.peek()
		if (a == '<' && (b == '=' || b == '>')) || (a == '>' && b == '=') || (a == '!' && b == '=') {
			lx.next()
			return lx.emit(tSymbol, start, string(a)+string(b))
		}
		return lx.emit(tSymbol, start, string(a))
	default:
		// unknown char: consume and return as symbol
		lx.next()
		return lx.emit(tSymbol, start, string(r))
	}
}

func isKeyword(up string) bool {
	switch up {
	case "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
		"JOIN", "ON", "AS",
		"CREATE", "TABLE", "TEMP", "DROP",
		"INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE",
		"INT", "FLOAT", "TEXT", "BOOL",
		"AND", "OR", "NOT",
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"TRUE", "FALSE":
		return true
	default:
		return false
	}
}

// minimal utf8 decoding; avoids importing unicode/utf8 explicitly elsewhere
func utf8DecodeRuneInString(s string) (r rune, size int) {
	for i := range s {
		if i > 0 {
			return rune(s[0]), 1 // ascii fast-path
		}
	}
	// this path runs only for empty or single-rune ascii
	if len(s) == 0 {
		return 0, 0
	}
	return rune(s[0]), 1
}

type Parser struct {
	lx   *Lexer
	cur  Token
	peek Token
}

func NewParser(sql string) *Parser {
	p := &Parser{lx: newLexer(sql)}
	p.cur = p.lx.NextToken()
	p.peek = p.lx.NextToken()
	return p
}

func (p *Parser) next() {
	p.cur = p.peek
	p.peek = p.lx.NextToken()
}

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

func (p *Parser) errf(format string, a ...interface{}) error {
	// Basic error with pointer location
	return fmt.Errorf("parse error near %q: %s", p.cur.Val, fmt.Sprintf(format, a...))
}

type Expr interface{}

type (
	VarRef struct {
		// Optional qualifier "table.column" or just "column"
		// We keep the raw text; resolution happens at eval time against row context
		Name string
	}
	Literal struct {
		Val interface{}
	}
	Unary struct {
		Op   string // "-" or "NOT"
		Expr Expr
	}
	Binary struct {
		Op    string // + - * / = != <> < <= > >= AND OR
		Left  Expr
		Right Expr
	}
	FuncCall struct {
		Name string // COUNT, SUM, AVG, MIN, MAX
		Args []Expr // COUNT(*) => special case with Star = true
		Star bool
	}
)

type Statement interface{}

type CreateTable struct {
	Name   string
	Cols   []Column
	IsTemp bool
	// optional AS SELECT
	AsSelect *Select
}

type DropTable struct {
	Name string
}

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

type Select struct {
	From    FromItem
	Joins   []JoinClause
	Projs   []SelectItem
	Where   Expr
	GroupBy []VarRef // simple column refs for grouping
	Having  Expr
	OrderBy []OrderItem
	Limit   *int
	Offset  *int
}

type FromItem struct {
	Table string
	Alias string // if empty, alias = table
}

type JoinClause struct {
	Right FromItem
	On    Expr
}

type SelectItem struct {
	Expr  Expr // or nil if Star
	Alias string
	Star  bool
}

type OrderItem struct {
	Col  string
	Desc bool
}

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
		return nil, p.errf("expected a statement (CREATE/DROP/INSERT/UPDATE/DELETE/SELECT)")
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

	// Two forms:
	//   CREATE [TEMP] TABLE t (col TYPE, ...)
	//   CREATE [TEMP] TABLE t AS SELECT ...
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
	return nil, p.errf("expected '(' for column defs or AS SELECT")
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
		return nil, p.errf("expected table name after INSERT INTO")
	}
	var cols []string
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		for {
			id := p.parseIdentLike()
			if id == "" {
				return nil, p.errf("expected column name in INSERT column list")
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
		return nil, p.errf("expected table name in UPDATE")
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	sets := make(map[string]Expr)
	for {
		col := p.parseIdentLike()
		if col == "" {
			return nil, p.errf("expected column name in SET")
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
		return nil, p.errf("expected table name in DELETE FROM")
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
	// projections
	var projs []SelectItem
	if p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		projs = append(projs, SelectItem{Star: true})
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
					return nil, p.errf("expected alias after AS")
				}
			} else if p.cur.Typ == tIdent {
				// implicit alias (common SQL shortcut)
				alias = p.cur.Val
				p.next()
			}
			projs = append(projs, SelectItem{Expr: e, Alias: alias})
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
	fromTbl := p.parseIdentLike()
	if fromTbl == "" {
		return nil, p.errf("expected table after FROM")
	}
	alias := fromTbl
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias = p.parseIdentLike()
		if alias == "" {
			return nil, p.errf("expected alias after AS")
		}
	} else if p.cur.Typ == tIdent {
		alias = p.cur.Val
		p.next()
	}
	from := FromItem{Table: fromTbl, Alias: alias}

	var joins []JoinClause
	for p.cur.Typ == tKeyword && p.cur.Val == "JOIN" {
		p.next()
		rtbl := p.parseIdentLike()
		if rtbl == "" {
			return nil, p.errf("expected table after JOIN")
		}
		ralias := rtbl
		if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
			p.next()
			ralias = p.parseIdentLike()
			if ralias == "" {
				return nil, p.errf("expected alias after AS")
			}
		} else if p.cur.Typ == tIdent {
			ralias = p.cur.Val
			p.next()
		}
		if err := p.expectKeyword("ON"); err != nil {
			return nil, err
		}
		on, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		joins = append(joins, JoinClause{Right: FromItem{Table: rtbl, Alias: ralias}, On: on})
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

	var groupBy []VarRef
	if p.cur.Typ == tKeyword && p.cur.Val == "GROUP" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			id := p.parseIdentLike()
			if id == "" {
				return nil, p.errf("GROUP BY expects column identifiers")
			}
			groupBy = append(groupBy, VarRef{Name: id})
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}

	var having Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "HAVING" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		having = e
	}

	var orderBy []OrderItem
	if p.cur.Typ == tKeyword && p.cur.Val == "ORDER" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			col := p.parseIdentLike()
			if col == "" {
				return nil, p.errf("ORDER BY expects column identifiers")
			}
			desc := false
			if p.cur.Typ == tKeyword && (p.cur.Val == "ASC" || p.cur.Val == "DESC") {
				desc = (p.cur.Val == "DESC")
				p.next()
			}
			orderBy = append(orderBy, OrderItem{Col: col, Desc: desc})
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}

	var limit, offset *int
	if p.cur.Typ == tKeyword && p.cur.Val == "LIMIT" {
		p.next()
		n := p.parseIntLiteral()
		if n == nil {
			return nil, p.errf("LIMIT expects an integer")
		}
		limit = n
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		n := p.parseIntLiteral()
		if n == nil {
			return nil, p.errf("OFFSET expects an integer")
		}
		offset = n
	}

	return &Select{
		From:    from,
		Joins:   joins,
		Projs:   projs,
		Where:   where,
		GroupBy: groupBy,
		Having:  having,
		OrderBy: orderBy,
		Limit:   limit,
		Offset:  offset,
	}, nil
}

func (p *Parser) parseColumnDefs() ([]Column, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	var cols []Column
	for {
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected column name")
		}
		typ := p.parseType()
		if typ < 0 {
			return nil, p.errf("unknown or missing type for column %q", name)
		}
		cols = append(cols, Column{Name: name, Type: typ})
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

func (p *Parser) parseType() ColType {
	if p.cur.Typ == tKeyword {
		switch p.cur.Val {
		case "INT":
			p.next()
			return IntType
		case "FLOAT":
			p.next()
			return FloatType
		case "TEXT":
			p.next()
			return TextType
		case "BOOL":
			p.next()
			return BoolType
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
	// permit keywords behaving as types/func names used as identifiers? keep strict.
	return ""
}

func (p *Parser) parseIntLiteral() *int {
	if p.cur.Typ == tNumber {
		if strings.Contains(p.cur.Val, ".") {
			return nil
		}
		n, _ := strconv.Atoi(p.cur.Val)
		p.next()
		return &n
	}
	return nil
}

func (p *Parser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tKeyword && p.cur.Val == "OR" {
		op := p.cur.Val
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tKeyword && p.cur.Val == "AND" {
		op := p.cur.Val
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
		p.next()
		e, err := p.parseCmp()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: "NOT", Expr: e}, nil
	}
	return p.parseCmp()
}

func (p *Parser) parseCmp() (Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	for {
		if p.cur.Typ == tSymbol {
			switch p.cur.Val {
			case "=", "!=", "<>", "<", "<=", ">", ">=":
				op := p.cur.Val
				p.next()
				right, err := p.parseAddSub()
				if err != nil {
					return nil, err
				}
				left = &Binary{Op: op, Left: left, Right: right}
				continue
			}
		}
		break
	}
	return left, nil
}

func (p *Parser) parseAddSub() (Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tSymbol && (p.cur.Val == "+" || p.cur.Val == "-") {
		op := p.cur.Val
		p.next()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseMulDiv() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == tSymbol && (p.cur.Val == "*" || p.cur.Val == "/") {
		op := p.cur.Val
		p.next()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: op, Left: left, Right: right}
	}
	return left, nil
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
		// function call?
		switch p.cur.Val {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return p.parseFuncCall()
		case "TRUE":
			p.next()
			return &Literal{Val: true}, nil
		case "FALSE":
			p.next()
			return &Literal{Val: false}, nil
		default:
			// Allow types used as identifiers? Keep strict; treat as error.
			return nil, p.errf("unexpected keyword %q", p.cur.Val)
		}
	case tIdent:
		// identifier or qualified "t.col"
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
	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}
	return &FuncCall{Name: name, Args: args}, nil
}

type Row map[string]interface{} // keys: "alias.col" always; additionally "col" if unique

type resultSet struct {
	Cols []string
	Rows []Row
}

// Eval helpers

func GetVal(row Row, name string) (interface{}, bool) {
	v, ok := row[strings.ToLower(name)]
	return v, ok
}

func putVal(row Row, key string, val interface{}) {
	row[strings.ToLower(key)] = val
}

func numeric(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func truthy(v interface{}) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	case nil:
		return false
	default:
		return false
	}
}

func compare(a, b interface{}) (int, error) {
	// returns -1 if a<b, 0 if equal, +1 if a>b
	if a == nil || b == nil {
		return 0, errors.New("cannot compare with NULL")
	}
	switch ax := a.(type) {
	case int:
		if f, ok := numeric(b); ok {
			af := float64(ax)
			if af < f {
				return -1, nil
			} else if af > f {
				return 1, nil
			}
			return 0, nil
		}
	case float64:
		if f, ok := numeric(b); ok {
			if ax < f {
				return -1, nil
			} else if ax > f {
				return 1, nil
			}
			return 0, nil
		}
	case string:
		if bs, ok := b.(string); ok {
			if ax < bs {
				return -1, nil
			} else if ax > bs {
				return 1, nil
			}
			return 0, nil
		}
	case bool:
		if bb, ok := b.(bool); ok {
			if !ax && bb {
				return -1, nil
			} else if ax && !bb {
				return 1, nil
			}
			return 0, nil
		}
	}
	// fallback equality
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable values %T and %T", a, b)
}

// Expression evaluation (row-wise)

func evalExpr(e Expr, row Row) (interface{}, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		// try exact key, then allow both "alias.col" already included; if user provided "a.b", treat as exact
		if v, ok := GetVal(row, ex.Name); ok {
			return v, nil
		}
		// If unqualified, we may have both "col" and "alias.col"
		if strings.Contains(ex.Name, ".") {
			return nil, fmt.Errorf("unknown column reference %q", ex.Name)
		}
		if v, ok := GetVal(row, ex.Name); ok {
			return v, nil
		}
		return nil, fmt.Errorf("unknown column %q", ex.Name)
	case *Unary:
		v, err := evalExpr(ex.Expr, row)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case "+":
			if f, ok := numeric(v); ok {
				return f, nil
			}
			return nil, fmt.Errorf("unary + on non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			return nil, fmt.Errorf("unary - on non-numeric")
		case "NOT":
			return !truthy(v), nil
		default:
			return nil, fmt.Errorf("unknown unary op %q", ex.Op)
		}
	case *Binary:
		// short-circuit for AND/OR
		if ex.Op == "AND" {
			lv, err := evalExpr(ex.Left, row)
			if err != nil {
				return nil, err
			}
			if !truthy(lv) {
				return false, nil
			}
			rv, err := evalExpr(ex.Right, row)
			if err != nil {
				return nil, err
			}
			return truthy(rv), nil
		}
		if ex.Op == "OR" {
			lv, err := evalExpr(ex.Left, row)
			if err != nil {
				return nil, err
			}
			if truthy(lv) {
				return true, nil
			}
			rv, err := evalExpr(ex.Right, row)
			if err != nil {
				return nil, err
			}
			return truthy(rv), nil
		}

		lv, err := evalExpr(ex.Left, row)
		if err != nil {
			return nil, err
		}
		rv, err := evalExpr(ex.Right, row)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case "+":
			lf, lok := numeric(lv)
			rf, rok := numeric(rv)
			if lok && rok {
				return lf + rf, nil
			}
			return nil, fmt.Errorf("+ expects numeric")
		case "-":
			lf, lok := numeric(lv)
			rf, rok := numeric(rv)
			if lok && rok {
				return lf - rf, nil
			}
			return nil, fmt.Errorf("- expects numeric")
		case "*":
			lf, lok := numeric(lv)
			rf, rok := numeric(rv)
			if lok && rok {
				return lf * rf, nil
			}
			return nil, fmt.Errorf("* expects numeric")
		case "/":
			lf, lok := numeric(lv)
			rf, rok := numeric(rv)
			if lok && rok {
				if rf == 0 {
					return nil, errors.New("division by zero")
				}
				return lf / rf, nil
			}
			return nil, fmt.Errorf("/ expects numeric")
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			cmp, err := compare(lv, rv)
			if err != nil && (ex.Op != "=" && ex.Op != "!=" && ex.Op != "<>") {
				return nil, err
			}
			switch ex.Op {
			case "=":
				return cmp == 0, nil
			case "!=", "<>":
				return cmp != 0, nil
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">":
				return cmp > 0, nil
			case ">=":
				return cmp >= 0, nil
			}
		default:
			return nil, fmt.Errorf("unknown binary op %q", ex.Op)
		}
	case *FuncCall:
		// In non-aggregate context, MIN/MAX etc. of a single row is just the argument value; COUNT returns 1 if arg not null
		switch ex.Name {
		case "COUNT":
			if ex.Star {
				return 1, nil
			}
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("COUNT expects 1 argument")
			}
			v, err := evalExpr(ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			if v == nil {
				return 0, nil
			}
			return 1, nil
		case "SUM", "AVG", "MIN", "MAX":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 argument", ex.Name)
			}
			v, err := evalExpr(ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			return v, nil
		default:
			return nil, fmt.Errorf("unknown function %s", ex.Name)
		}
	default:
		return nil, fmt.Errorf("unknown expression")
	}
	return nil, nil
}

// Aggregate evaluation by scanning a group's rows on demand (clarity over speed).
func isAggregate(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
	case *Unary:
		return isAggregate(ex.Expr)
	case *Binary:
		return isAggregate(ex.Left) || isAggregate(ex.Right)
	}
	return false
}

func evalAggregate(e Expr, rows []Row) (interface{}, error) {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT":
			if ex.Star {
				return len(rows), nil
			}
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("COUNT expects 1 argument")
			}
			cnt := 0
			for _, r := range rows {
				v, err := evalExpr(ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if v != nil && !(fmt.Sprintf("%v", v) == "<nil>") {
					cnt++
				}
			}
			return cnt, nil
		case "SUM", "AVG":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 argument", ex.Name)
			}
			sum := 0.0
			n := 0
			for _, r := range rows {
				v, err := evalExpr(ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if f, ok := numeric(v); ok {
					sum += f
					n++
				}
			}
			if ex.Name == "SUM" {
				return sum, nil
			}
			if n == 0 {
				return 0.0, nil
			}
			return sum / float64(n), nil
		case "MIN", "MAX":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 argument", ex.Name)
			}
			var have bool
			var best interface{}
			for _, r := range rows {
				v, err := evalExpr(ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if !have {
					best = v
					have = true
				} else {
					cmp, err := compare(v, best)
					if err == nil {
						if ex.Name == "MIN" && cmp < 0 {
							best = v
						}
						if ex.Name == "MAX" && cmp > 0 {
							best = v
						}
					}
				}
			}
			if !have {
				return nil, nil
			}
			return best, nil
		}
	case *Unary:
		v, err := evalAggregate(ex.Expr, rows)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case "+":
			if f, ok := numeric(v); ok {
				return f, nil
			}
			return nil, fmt.Errorf("unary + on non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			return nil, fmt.Errorf("unary - on non-numeric")
		case "NOT":
			return !truthy(v), nil
		}
	case *Binary:
		lv, err := evalAggregate(ex.Left, rows)
		if err != nil {
			return nil, err
		}
		rv, err := evalAggregate(ex.Right, rows)
		if err != nil {
			return nil, err
		}
		// reuse the same ops as non-aggregate
		return evalExpr(&Binary{Op: ex.Op, Left: &Literal{Val: lv}, Right: &Literal{Val: rv}}, Row{})
	default:
		// Non-aggregate expression: evaluate it on first row (usual SQL would enforce GROUP BY correctness).
		if len(rows) == 0 {
			return nil, nil
		}
		return evalExpr(e, rows[0])
	}
	return nil, fmt.Errorf("unsupported aggregate expression")
}

// Build row contexts from table rows
func rowsFromTable(t *Table, alias string) ([]Row, []string) {
	cols := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		cols[i] = strings.ToLower(alias + "." + c.Name)
	}
	colSet := make(map[string]bool)
	for _, c := range cols {
		colSet[c] = true
	}
	// also include bare column names if unique across this table
	for range t.Cols {
		put := true
		// (within one table it's unique)
		_ = put // clarity
		_ = colSet
	}
	var out []Row
	for _, r := range t.Rows {
		row := make(Row)
		// alias.col always present
		for i, c := range t.Cols {
			putVal(row, alias+"."+c.Name, r[i])
		}
		// also "col" (unqualified) for convenience (later may be shadowed when joining)
		for i, c := range t.Cols {
			key := c.Name
			if _, exists := row[strings.ToLower(key)]; !exists {
				putVal(row, key, r[i])
			}
		}
		out = append(out, row)
	}
	return out, cols
}

func Execute(db *DB, stmt Statement) (*resultSet, error) {
	switch s := stmt.(type) {
	case *CreateTable:
		if s.AsSelect == nil {
			// CREATE TABLE t (cols)
			t := NewTable(s.Name, s.Cols, s.IsTemp)
			return nil, db.Put(t)
		}
		// CREATE [TEMP] TABLE t AS SELECT ...
		rs, err := Execute(db, s.AsSelect)
		if err != nil {
			return nil, err
		}
		// infer column names as TEXT (best effort) unless numeric
		cols := make([]Column, len(rs.Cols))
		if len(rs.Rows) > 0 {
			for i, c := range rs.Cols {
				cols[i] = Column{Name: c, Type: inferType(rs.Rows[0][c])}
			}
		} else {
			for i, c := range rs.Cols {
				cols[i] = Column{Name: c, Type: TextType}
			}
		}
		t := NewTable(s.Name, cols, s.IsTemp)
		// transform result rows into table's [][]interface{}
		for _, r := range rs.Rows {
			row := make([]interface{}, len(cols))
			for i, c := range cols {
				row[i] = r[strings.ToLower(c.Name)]
			}
			t.Rows = append(t.Rows, row)
		}
		return nil, db.Put(t)

	case *DropTable:
		return nil, db.Drop(s.Name)

	case *Insert:
		t, err := db.Get(s.Table)
		if err != nil {
			return nil, err
		}
		if len(s.Cols) > 0 && len(s.Cols) != len(s.Vals) {
			return nil, fmt.Errorf("INSERT column count and value count mismatch")
		}
		row := make([]interface{}, len(t.Cols))
		// Defaults: zero values if not provided.
		for i := range row {
			row[i] = zeroValue(t.Cols[i].Type)
		}
		// Evaluate values
		tmpRow := Row{} // literals only
		if len(s.Cols) == 0 {
			// INSERT INTO t VALUES ( … ) maps by position
			if len(s.Vals) != len(t.Cols) {
				return nil, fmt.Errorf("INSERT without columns expects %d values", len(t.Cols))
			}
			for i, e := range s.Vals {
				v, err := evalExpr(e, tmpRow)
				if err != nil {
					return nil, err
				}
				row[i], err = coerceToType(v, t.Cols[i].Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
				}
			}
		} else {
			for i, name := range s.Cols {
				idx, err := t.colIndex(name)
				if err != nil {
					return nil, err
				}
				v, err := evalExpr(s.Vals[i], tmpRow)
				if err != nil {
					return nil, err
				}
				row[idx], err = coerceToType(v, t.Cols[idx].Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
				}
			}
		}
		t.Rows = append(t.Rows, row)
		t.Version++
		return nil, nil

	case *Update:
		t, err := db.Get(s.Table)
		if err != nil {
			return nil, err
		}
		// Precompute column indices
		setIdx := make(map[int]Expr)
		for name, ex := range s.Sets {
			i, err := t.colIndex(name)
			if err != nil {
				return nil, err
			}
			setIdx[i] = ex
		}
		count := 0
		for ri, r := range t.Rows {
			row := Row{}
			for i, c := range t.Cols {
				putVal(row, c.Name, r[i])
				putVal(row, s.Table+"."+c.Name, r[i])
			}
			ok := true
			if s.Where != nil {
				v, err := evalExpr(s.Where, row)
				if err != nil {
					return nil, err
				}
				ok = truthy(v)
			}
			if ok {
				for i, ex := range setIdx {
					v, err := evalExpr(ex, row)
					if err != nil {
						return nil, err
					}
					cv, err := coerceToType(v, t.Cols[i].Type)
					if err != nil {
						return nil, err
					}
					t.Rows[ri][i] = cv
				}
				count++
			}
		}
		t.Version++
		return &resultSet{Cols: []string{fmt.Sprintf("updated")}, Rows: []Row{{"updated": count}}}, nil

	case *Delete:
		t, err := db.Get(s.Table)
		if err != nil {
			return nil, err
		}
		var kept [][]interface{}
		del := 0
		for _, r := range t.Rows {
			row := Row{}
			for i, c := range t.Cols {
				putVal(row, c.Name, r[i])
				putVal(row, s.Table+"."+c.Name, r[i])
			}
			ok := true
			if s.Where != nil {
				v, err := evalExpr(s.Where, row)
				if err != nil {
					return nil, err
				}
				ok = !truthy(v) // keep if WHERE false
			}
			if ok {
				kept = append(kept, r)
			} else {
				del++
			}
		}
		t.Rows = kept
		t.Version++
		return &resultSet{Cols: []string{fmt.Sprintf("deleted")}, Rows: []Row{{"deleted": del}}}, nil

	case *Select:
		// FROM
		leftT, err := db.Get(s.From.Table)
		if err != nil {
			return nil, err
		}
		leftRows, _ := rowsFromTable(leftT, s.From.AliasOrTable())

		// JOINs (nested loop join)
		cur := leftRows
		for _, j := range s.Joins {
			rt, err := db.Get(j.Right.Table)
			if err != nil {
				return nil, err
			}
			rightRows, _ := rowsFromTable(rt, j.Right.AliasOrTable())
			var joined []Row
			for _, l := range cur {
				for _, r := range rightRows {
					// merge rows (r overwrites unqualified keys; alias.col never collide)
					m := make(Row)
					for k, v := range l {
						m[k] = v
					}
					for k, v := range r {
						m[k] = v
					}
					ok := true
					if j.On != nil {
						val, err := evalExpr(j.On, m)
						if err != nil {
							return nil, err
						}
						ok = truthy(val)
					}
					if ok {
						joined = append(joined, m)
					}
				}
			}
			cur = joined
		}

		// WHERE
		filtered := cur
		if s.Where != nil {
			var tmp []Row
			for _, r := range filtered {
				v, err := evalExpr(s.Where, r)
				if err != nil {
					return nil, err
				}
				if truthy(v) {
					tmp = append(tmp, r)
				}
			}
			filtered = tmp
		}

		// GROUP & HAVING
		needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)
		var outRows []Row
		var outCols []string

		if needAgg {
			// group by keys
			type key struct{ parts []string }
			groups := make(map[string][]Row)
			orderKeys := []string{}
			for _, r := range filtered {
				var parts []string
				for _, g := range s.GroupBy {
					v, err := evalExpr(&g, r)
					if err != nil {
						return nil, err
					}
					parts = append(parts, fmt.Sprintf("%v", v))
				}
				ks := strings.Join(parts, "\x1f")
				if _, ok := groups[ks]; !ok {
					orderKeys = append(orderKeys, ks)
				}
				groups[ks] = append(groups[ks], r)
			}
			// build rows per group
			for _, k := range orderKeys {
				rows := groups[k]
				// HAVING may use aggregates
				if s.Having != nil {
					hv, err := evalAggregate(s.Having, rows)
					if err != nil {
						return nil, err
					}
					if !truthy(hv) {
						continue
					}
				}
				out := Row{}
				for i, it := range s.Projs {
					name := projName(it, i)
					var val interface{}
					if it.Star {
						// Expand * from the first row: alias.col names
						if len(rows) > 0 {
							for col, v := range rows[0] {
								// only include qualified names ("a.x")
								if strings.Contains(col, ".") {
									putVal(out, col, v)
									outCols = appendUnique(outCols, col)
								}
							}
							continue
						}
					}
					if isAggregate(it.Expr) || len(s.GroupBy) > 0 {
						val, err = evalAggregate(it.Expr, rows)
					} else {
						val, err = evalExpr(it.Expr, rows[0])
					}
					if err != nil {
						return nil, err
					}
					putVal(out, name, val)
					outCols = appendUnique(outCols, name)
				}
				outRows = append(outRows, out)
			}
		} else {
			// simple projection, row-wise
			for _, r := range filtered {
				out := Row{}
				for i, it := range s.Projs {
					if it.Star {
						// expand * => alias.col names from this row
						for col, v := range r {
							if strings.Contains(col, ".") {
								putVal(out, col, v)
								outCols = appendUnique(outCols, col)
							}
						}
						continue
					}
					val, err := evalExpr(it.Expr, r)
					if err != nil {
						return nil, err
					}
					name := projName(it, i)
					putVal(out, name, val)
					outCols = appendUnique(outCols, name)
				}
				outRows = append(outRows, out)
			}
		}

		// ORDER BY
		if len(s.OrderBy) > 0 {
			sort.SliceStable(outRows, func(i, j int) bool {
				a := outRows[i]
				b := outRows[j]
				for _, oi := range s.OrderBy {
					av, _ := GetVal(a, oi.Col)
					bv, _ := GetVal(b, oi.Col)
					cmp, err := compare(av, bv)
					if err != nil {
						// incomparable: keep original order
						cmp = 0
					}
					if cmp == 0 {
						continue
					}
					if oi.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
				return false
			})
		}

		// OFFSET / LIMIT
		start := 0
		if s.Offset != nil && *s.Offset > 0 {
			start = *s.Offset
		}
		if start > len(outRows) {
			outRows = []Row{}
		} else {
			outRows = outRows[start:]
		}
		if s.Limit != nil && *s.Limit < len(outRows) {
			outRows = outRows[:*s.Limit]
		}

		if len(outCols) == 0 {
			// if all projections were *, choose deterministic column order from rows
			outCols = columnsFromRows(outRows)
		}
		return &resultSet{Cols: outCols, Rows: outRows}, nil
	}
	return nil, fmt.Errorf("unknown statement")
}

func (f FromItem) AliasOrTable() string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Table
}

func projName(it SelectItem, idx int) string {
	if it.Alias != "" {
		return it.Alias
	}
	switch ex := it.Expr.(type) {
	case *VarRef:
		return lastPart(ex.Name)
	case *FuncCall:
		up := strings.ToUpper(ex.Name)
		if len(ex.Args) == 1 {
			switch a := ex.Args[0].(type) {
			case *VarRef:
				return fmt.Sprintf("%s_%s", up, lastPart(a.Name))
			}
		}
		return up
	default:
		return fmt.Sprintf("expr%d", idx+1)
	}
}

func lastPart(s string) string {
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[i+1:]
	}
	return s
}

func anyAggInSelect(items []SelectItem) bool {
	for _, it := range items {
		if isAggregate(it.Expr) {
			return true
		}
	}
	return false
}

func appendUnique(cols []string, c string) []string {
	lc := strings.ToLower(c)
	for _, x := range cols {
		if strings.ToLower(x) == lc {
			return cols
		}
	}
	return append(cols, c)
}

func columnsFromRows(rows []Row) []string {
	seen := map[string]bool{}
	var cols []string
	for _, r := range rows {
		for k := range r {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	// deterministic: sort
	sort.Strings(cols)
	return cols
}

func inferType(v interface{}) ColType {
	switch v.(type) {
	case int, int64:
		return IntType
	case float64:
		return FloatType
	case bool:
		return BoolType
	case string:
		return TextType
	default:
		return TextType
	}
}

func zeroValue(t ColType) interface{} {
	switch t {
	case IntType:
		return 0
	case FloatType:
		return 0.0
	case BoolType:
		return false
	case TextType:
		return ""
	default:
		return nil
	}
}

func coerceToType(v interface{}, t ColType) (interface{}, error) {
	if v == nil {
		return zeroValue(t), nil
	}
	switch t {
	case IntType:
		switch x := v.(type) {
		case int:
			return x, nil
		case float64:
			return int(x), nil
		case string:
			n, err := strconv.Atoi(strings.TrimSpace(x))
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to INT", x)
			}
			return n, nil
		case bool:
			if x {
				return 1, nil
			}
			return 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to INT", v)
		}
	case FloatType:
		switch x := v.(type) {
		case int:
			return float64(x), nil
		case float64:
			return x, nil
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to FLOAT", x)
			}
			return f, nil
		case bool:
			if x {
				return 1.0, nil
			}
			return 0.0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to FLOAT", v)
		}
	case TextType:
		return fmt.Sprintf("%v", v), nil
	case BoolType:
		switch x := v.(type) {
		case bool:
			return x, nil
		case int:
			return x != 0, nil
		case float64:
			return x != 0, nil
		case string:
			s := strings.ToLower(strings.TrimSpace(x))
			return s == "true" || s == "1" || s == "t" || s == "yes", nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BOOL", v)
		}
	default:
		return v, nil
	}
}

func PrintResult(rs *resultSet) {
	if rs == nil {
		return
	}
	if len(rs.Rows) == 1 && len(rs.Cols) == 1 && (rs.Cols[0] == "updated" || rs.Cols[0] == "deleted") {
		for _, r := range rs.Rows {
			fmt.Printf("%s: %v\n", rs.Cols[0], r[strings.ToLower(rs.Cols[0])])
		}
		return
	}
	// compute column widths
	width := make([]int, len(rs.Cols))
	for i, c := range rs.Cols {
		width[i] = len(c)
	}
	for _, r := range rs.Rows {
		for i, c := range rs.Cols {
			s := fmt.Sprintf("%v", r[strings.ToLower(c)])
			if len(s) > width[i] {
				width[i] = len(s)
			}
		}
	}
	// header
	for i, c := range rs.Cols {
		fmt.Print(padRight(c, width[i]))
		if i < len(rs.Cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for i := range rs.Cols {
		fmt.Print(strings.Repeat("-", width[i]))
		if i < len(rs.Cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	// rows
	for _, r := range rs.Rows {
		for i, c := range rs.Cols {
			s := fmt.Sprintf("%v", r[strings.ToLower(c)])
			fmt.Print(padRight(s, width[i]))
			if i < len(rs.Cols)-1 {
				fmt.Print("  ")
			}
		}
		fmt.Println()
	}
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}
