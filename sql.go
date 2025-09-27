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
	"bufio"
	"errors"
	"fmt"
	"os"
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

func (db *DB) ListTables() []*Table {
	names := make([]string, 0, len(db.tables))
	for k := range db.tables {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]*Table, 0, len(names))
	for _, n := range names {
		out = append(out, db.tables[n])
	}
	return out
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
		lx.next()
		return lx.emit(tSymbol, start, string(r))
	}
}

func isKeyword(up string) bool {
	switch up {
	case "SELECT", "DISTINCT",
		"FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
		"JOIN", "LEFT", "RIGHT", "OUTER", "ON", "AS",
		"CREATE", "TABLE", "TEMP", "DROP",
		"INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE",
		"INT", "FLOAT", "TEXT", "BOOL",
		"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE",
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"COALESCE", "NULLIF":
		return true
	default:
		return false
	}
}

// minimal ascii fast-path utf8 decoding
func utf8DecodeRuneInString(s string) (r rune, size int) {
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

func (p *Parser) next() { p.cur, p.peek = p.peek, p.lx.NextToken() }

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
	return fmt.Errorf("parse error near %q: %s", p.cur.Val, fmt.Sprintf(format, a...))
}

type Expr interface{}

type (
	VarRef struct{ Name string }
	Literal struct{ Val interface{} }
	Unary   struct {
		Op   string // "-" or "NOT"
		Expr Expr
	}
	Binary struct {
		Op    string // + - * / = != <> < <= > >= AND OR
		Left  Expr
		Right Expr
	}
	IsNull struct {
		Expr   Expr
		Negate bool
	}
	FuncCall struct {
		Name string // COUNT, SUM, AVG, MIN, MAX, COALESCE, NULLIF
		Args []Expr // COUNT(*) => Star = true
		Star bool
	}
)

type Statement interface{}

type CreateTable struct {
	Name     string
	Cols     []Column
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

type FromItem struct {
	Table string
	Alias string // if empty, alias = table
}

type JoinClause struct {
	Type  JoinType
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
	sel := &Select{}

	// DISTINCT
	if p.cur.Typ == tKeyword && p.cur.Val == "DISTINCT" {
		sel.Distinct = true
		p.next()
	}

	// projections
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
					return nil, p.errf("expected alias after AS")
				}
			} else if p.cur.Typ == tIdent {
				// implicit alias
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
	sel.From = FromItem{Table: fromTbl, Alias: alias}

	// JOINs: INNER, LEFT OUTER, RIGHT OUTER
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

	var where Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		where = e
	}
	sel.Where = where

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
	sel.GroupBy = groupBy

	if p.cur.Typ == tKeyword && p.cur.Val == "HAVING" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Having = e
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
	sel.OrderBy = orderBy

	if p.cur.Typ == tKeyword && p.cur.Val == "LIMIT" {
		p.next()
		n := p.parseIntLiteral()
		if n == nil {
			return nil, p.errf("LIMIT expects an integer")
		}
		sel.Limit = n
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		n := p.parseIntLiteral()
		if n == nil {
			return nil, p.errf("OFFSET expects an integer")
		}
		sel.Offset = n
	}

	return sel, nil
}

func (p *Parser) parseJoinTail() (string, string, Expr, error) {
	rtbl := p.parseIdentLike()
	if rtbl == "" {
		return "", "", nil, p.errf("expected table after JOIN")
	}
	ralias := rtbl
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		ralias = p.parseIdentLike()
		if ralias == "" {
			return "", "", nil, p.errf("expected alias after AS")
		}
	} else if p.cur.Typ == tIdent {
		ralias = p.cur.Val
		p.next()
	}
	if err := p.expectKeyword("ON"); err != nil {
		return "", "", nil, err
	}
	on, err := p.parseExpr()
	if err != nil {
		return "", "", nil, err
	}
	return rtbl, ralias, on, nil
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
		e, err := p.parseIsNull()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: "NOT", Expr: e}, nil
	}
	return p.parseIsNull()
}

func (p *Parser) parseIsNull() (Expr, error) {
	left, err := p.parseCmp()
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
			return &IsNull{Expr: left, Negate: neg}, nil
		}
		return nil, p.errf("expected NULL after IS/IS NOT")
	}
	return left, nil
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
		switch p.cur.Val {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "NULLIF":
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

type Row map[string]interface{} // keys: lower-cased; values may be nil (NULL)

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

func isNull(v interface{}) bool { return v == nil }

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
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}

// 3-valued logic helpers: TRUE=1, FALSE=0, UNKNOWN=2
const (
	tvFalse   = 0
	tvTrue    = 1
	tvUnknown = 2
)

func toTri(v interface{}) int {
	if v == nil {
		return tvUnknown
	}
	if truthy(v) {
		return tvTrue
	}
	return tvFalse
}
func triNot(t int) int {
	switch t {
	case tvTrue:
		return tvFalse
	case tvFalse:
		return tvTrue
	default:
		return tvUnknown
	}
}
func triAnd(a, b int) int {
	if a == tvFalse || b == tvFalse {
		return tvFalse
	}
	if a == tvTrue && b == tvTrue {
		return tvTrue
	}
	return tvUnknown
}
func triOr(a, b int) int {
	if a == tvTrue || b == tvTrue {
		return tvTrue
	}
	if a == tvFalse && b == tvFalse {
		return tvFalse
	}
	return tvUnknown
}

// compare for non-NULL values; returns -1/0/+1; errors on NULLs or incompatible types
func compare(a, b interface{}) (int, error) {
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
	// fallback equality stringification (rare)
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable values %T and %T", a, b)
}

// compareForOrder allows NULLs with rule:
//   ASC : NULLS LAST
//   DESC: NULLS FIRST
func compareForOrder(a, b interface{}, desc bool) int {
	// nil handling
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		// a should come last in ASC, first in DESC
		if desc {
			return -1
		}
		return 1
	}
	if b == nil {
		if desc {
			return 1
		}
		return -1
	}
	cmp, err := compare(a, b)
	if err != nil {
		return 0
	}
	return cmp
}

// Expression evaluation (row-wise)

func evalExpr(e Expr, row Row) (interface{}, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		if v, ok := GetVal(row, ex.Name); ok {
			return v, nil
		}
		if strings.Contains(ex.Name, ".") {
			return nil, fmt.Errorf("unknown column reference %q", ex.Name)
		}
		if v, ok := GetVal(row, ex.Name); ok {
			return v, nil
		}
		return nil, fmt.Errorf("unknown column %q", ex.Name)

	case *IsNull:
		v, err := evalExpr(ex.Expr, row)
		if err != nil {
			return nil, err
		}
		is := isNull(v)
		if ex.Negate {
			return !is, nil
		}
		return is, nil

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
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary + on non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary - on non-numeric")
		case "NOT":
			return triToValue(triNot(toTri(v))), nil
		default:
			return nil, fmt.Errorf("unknown unary op %q", ex.Op)
		}

	case *Binary:
		switch ex.Op {
		case "AND":
			lv, err := evalExpr(ex.Left, row)
			if err != nil {
				return nil, err
			}
			// short-circuit FALSE
			if toTri(lv) == tvFalse {
				return false, nil
			}
			rv, err := evalExpr(ex.Right, row)
			if err != nil {
				return nil, err
			}
			return triToValue(triAnd(toTri(lv), toTri(rv))), nil
		case "OR":
			lv, err := evalExpr(ex.Left, row)
			if err != nil {
				return nil, err
			}
			// short-circuit TRUE
			if toTri(lv) == tvTrue {
				return true, nil
			}
			rv, err := evalExpr(ex.Right, row)
			if err != nil {
				return nil, err
			}
			return triToValue(triOr(toTri(lv), toTri(rv))), nil
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
		case "+", "-", "*", "/":
			if lv == nil || rv == nil {
				return nil, nil
			}
			lf, lok := numeric(lv)
			rf, rok := numeric(rv)
			if !(lok && rok) {
				return nil, fmt.Errorf("%s expects numeric", ex.Op)
			}
			switch ex.Op {
			case "+":
				return lf + rf, nil
			case "-":
				return lf - rf, nil
			case "*":
				return lf * rf, nil
			case "/":
				if rf == 0 {
					return nil, errors.New("division by zero")
				}
				return lf / rf, nil
			}
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			// NULL handling -> UNKNOWN (nil)
			if lv == nil || rv == nil {
				return nil, nil
			}
			cmp, err := compare(lv, rv)
			if err != nil {
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
		switch ex.Name {
		case "COALESCE":
			for _, a := range ex.Args {
				v, err := evalExpr(a, row)
				if err != nil {
					return nil, err
				}
				if v != nil {
					return v, nil
				}
			}
			return nil, nil
		case "NULLIF":
			if len(ex.Args) != 2 {
				return nil, fmt.Errorf("NULLIF expects 2 arguments")
			}
			lv, err := evalExpr(ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			rv, err := evalExpr(ex.Args[1], row)
			if err != nil {
				return nil, err
			}
			if lv == nil {
				return nil, nil
			}
			if rv == nil {
				return lv, nil
			}
			cmp, err := compare(lv, rv)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return nil, nil
			}
			return lv, nil

		case "COUNT":
			// non-aggregate context: COUNT(x) -> 1 if x != NULL else 0; COUNT(*) -> 1
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
			// non-aggregate context: pass-through value (if any)
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
	return nil, fmt.Errorf("unhandled expression")
}

func triToValue(t int) interface{} {
	switch t {
	case tvTrue:
		return true
	case tvFalse:
		return false
	default:
		return nil
	}
}

// Aggregate evaluation by scanning a group's rows (clarity over speed).
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
	case *IsNull:
		return isAggregate(ex.Expr)
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
				if v != nil {
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
				return nil, nil // AVG over empty/non-numeric -> NULL
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
				if v == nil {
					continue
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
		case "COALESCE", "NULLIF":
			// not aggregates: evaluate on representative row
			if len(rows) == 0 {
				return nil, nil
			}
			return evalExpr(ex, rows[0])
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
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary + on non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary - on non-numeric")
		case "NOT":
			return triToValue(triNot(toTri(v))), nil
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
		return evalExpr(&Binary{Op: ex.Op, Left: &Literal{Val: lv}, Right: &Literal{Val: rv}}, Row{})
	case *IsNull:
		v, err := evalAggregate(ex.Expr, rows)
		if err != nil {
			return nil, err
		}
		if ex.Negate {
			return !isNull(v), nil
		}
		return isNull(v), nil
	default:
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
	var out []Row
	for _, r := range t.Rows {
		row := make(Row)
		for i, c := range t.Cols {
			putVal(row, alias+"."+c.Name, r[i])
		}
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

func keysOfRow(r Row) []string {
	ks := make([]string, 0, len(r))
	for k := range r {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func Execute(db *DB, stmt Statement) (*resultSet, error) {
	switch s := stmt.(type) {
	case *CreateTable:
		if s.AsSelect == nil {
			t := NewTable(s.Name, s.Cols, s.IsTemp)
			return nil, db.Put(t)
		}
		// CREATE ... AS SELECT
		rs, err := Execute(db, s.AsSelect)
		if err != nil {
			return nil, err
		}
		cols := make([]Column, len(rs.Cols))
		if len(rs.Rows) > 0 {
			for i, c := range rs.Cols {
				cols[i] = Column{Name: c, Type: inferType(rs.Rows[0][strings.ToLower(c)])}
			}
		} else {
			for i, c := range rs.Cols {
				cols[i] = Column{Name: c, Type: TextType}
			}
		}
		t := NewTable(s.Name, cols, s.IsTemp)
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
		// Default to NULLs (SQL default) for unspecified columns.
		for i := range row {
			row[i] = nil
		}
		tmpRow := Row{} // literals only
		if len(s.Cols) == 0 {
			if len(s.Vals) != len(t.Cols) {
				return nil, fmt.Errorf("INSERT without columns expects %d values", len(t.Cols))
			}
			for i, e := range s.Vals {
				v, err := evalExpr(e, tmpRow)
				if err != nil {
					return nil, err
				}
				row[i], err = coerceToTypeAllowNull(v, t.Cols[i].Type)
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
				row[idx], err = coerceToTypeAllowNull(v, t.Cols[idx].Type)
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
				ok = (toTri(v) == tvTrue)
			}
			if ok {
				for i, ex := range setIdx {
					v, err := evalExpr(ex, row)
					if err != nil {
						return nil, err
					}
					cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
					if err != nil {
						return nil, err
					}
					t.Rows[ri][i] = cv
				}
				count++
			}
		}
		t.Version++
		return &resultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": count}}}, nil

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
			keep := true
			if s.Where != nil {
				v, err := evalExpr(s.Where, row)
				if err != nil {
					return nil, err
				}
				// WHERE keeps only TRUE; FALSE/UNKNOWN are filtered
				if toTri(v) == tvTrue {
					keep = false
				}
			}
			if keep {
				kept = append(kept, r)
			} else {
				del++
			}
		}
		t.Rows = kept
		t.Version++
		return &resultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil

	case *Select:
		// FROM
		leftT, err := db.Get(s.From.Table)
		if err != nil {
			return nil, err
		}
		leftRows, _ := rowsFromTable(leftT, s.From.AliasOrTable())
		cur := leftRows

		// JOINs
		for _, j := range s.Joins {
			rt, err := db.Get(j.Right.Table)
			if err != nil {
				return nil, err
			}
			rightRows, _ := rowsFromTable(rt, j.Right.AliasOrTable())

			switch j.Type {
			case JoinInner:
				var joined []Row
				for _, l := range cur {
					for _, r := range rightRows {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(j.On, m)
							if err != nil {
								return nil, err
							}
							ok = (toTri(val) == tvTrue)
						}
						if ok {
							joined = append(joined, m)
						}
					}
				}
				cur = joined

			case JoinLeft:
				var joined []Row
				for _, l := range cur {
					matched := false
					for _, r := range rightRows {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(j.On, m)
							if err != nil {
								return nil, err
							}
							ok = (toTri(val) == tvTrue)
						}
						if ok {
							joined = append(joined, m)
							matched = true
						}
					}
					if !matched {
						m := cloneRow(l)
						// pad right side alias columns with NULLs
						addRightNulls(m, j.Right.AliasOrTable(), rt)
						joined = append(joined, m)
					}
				}
				cur = joined

			case JoinRight:
				var joined []Row
				// collect left keys prototype
				leftKeys := []string{}
				if len(cur) > 0 {
					leftKeys = keysOfRow(cur[0])
				}
				for _, r := range rightRows {
					matched := false
					for _, l := range cur {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(j.On, m)
							if err != nil {
								return nil, err
							}
							ok = (toTri(val) == tvTrue)
						}
						if ok {
							joined = append(joined, m)
							matched = true
						}
					}
					if !matched {
						m := cloneRow(r)
						// pad ALL left keys with NULLs (includes any prior joins on the left chain)
						for _, k := range leftKeys {
							m[k] = nil
						}
						joined = append(joined, m)
					}
				}
				cur = joined
			}
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
				if toTri(v) == tvTrue {
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
			groups := make(map[string][]Row)
			orderKeys := []string{}
			for _, r := range filtered {
				var parts []string
				for _, g := range s.GroupBy {
					v, err := evalExpr(&g, r)
					if err != nil {
						return nil, err
					}
					parts = append(parts, fmtKeyPart(v))
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
				// HAVING
				if s.Having != nil {
					hv, err := evalAggregate(s.Having, rows)
					if err != nil {
						return nil, err
					}
					if toTri(hv) != tvTrue {
						continue
					}
				}
				out := Row{}
				for i, it := range s.Projs {
					if it.Star {
						if len(rows) > 0 {
							for col, v := range rows[0] {
								if strings.Contains(col, ".") {
									putVal(out, col, v)
									outCols = appendUnique(outCols, col)
								}
							}
						}
						continue
					}
					name := projName(it, i)
					var val interface{}
					var err error
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
			// simple projection
			for _, r := range filtered {
				out := Row{}
				for i, it := range s.Projs {
					if it.Star {
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

		// DISTINCT
		if s.Distinct {
			outRows = distinctRows(outRows, outCols)
		}

		// ORDER BY
		if len(s.OrderBy) > 0 {
			sort.SliceStable(outRows, func(i, j int) bool {
				a := outRows[i]
				b := outRows[j]
				for _, oi := range s.OrderBy {
					av, _ := GetVal(a, oi.Col)
					bv, _ := GetVal(b, oi.Col)
					cmp := compareForOrder(av, bv, oi.Desc)
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

func mergeRows(l, r Row) Row {
	m := make(Row, len(l)+len(r))
	for k, v := range l {
		m[k] = v
	}
	for k, v := range r {
		m[k] = v
	}
	return m
}

func cloneRow(r Row) Row {
	m := make(Row, len(r))
	for k, v := range r {
		m[k] = v
	}
	return m
}

func addRightNulls(m Row, rightAlias string, rt *Table) {
	for _, c := range rt.Cols {
		putVal(m, rightAlias+"."+c.Name, nil)
		if _, exists := m[strings.ToLower(c.Name)]; !exists {
			putVal(m, c.Name, nil)
		}
	}
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
	case *IsNull:
		return fmt.Sprintf("isnull%d", idx+1)
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
		if it.Expr != nil && isAggregate(it.Expr) {
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

func coerceToTypeAllowNull(v interface{}, t ColType) (interface{}, error) {
	if v == nil {
		return nil, nil
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

func fmtKeyPart(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "N:"
	case int:
		return "I:" + strconv.Itoa(x)
	case float64:
		return "F:" + strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "B:1"
		}
		return "B:0"
	case string:
		return "S:" + x
	default:
		return fmt.Sprintf("T:%T:%v", v, v)
	}
}

func distinctRows(rows []Row, cols []string) []Row {
	seen := make(map[string]bool)
	var out []Row
	for _, r := range rows {
		var parts []string
		for _, c := range cols {
			parts = append(parts, fmtKeyPart(r[strings.ToLower(c)]))
		}
		key := strings.Join(parts, "|")
		if !seen[key] {
			seen[key] = true
			out = append(out, r)
		}
	}
	return out
}

func printResult(rs *resultSet) {
	if rs == nil {
		return
	}
	// single number results (updated/deleted)
	if len(rs.Rows) == 1 && len(rs.Cols) == 1 && (strings.ToLower(rs.Cols[0]) == "updated" || strings.ToLower(rs.Cols[0]) == "deleted") {
		for _, r := range rs.Rows {
			fmt.Printf("%s: %v\n", rs.Cols[0], r[strings.ToLower(rs.Cols[0])])
		}
		return
	}
	// compute widths
	width := make([]int, len(rs.Cols))
	for i, c := range rs.Cols {
		width[i] = len(c)
	}
	for _, r := range rs.Rows {
		for i, c := range rs.Cols {
			s := cellString(r[strings.ToLower(c)])
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
			s := cellString(r[strings.ToLower(c)])
			fmt.Print(padRight(s, width[i]))
			if i < len(rs.Cols)-1 {
				fmt.Print("  ")
			}
		}
		fmt.Println()
	}
}

func cellString(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

func runREPL(db *DB) {
	fmt.Println("TinySQL+ REPL (stdlib-only).  End a statement with ';'.  Type .help for commands.")
	sc := bufio.NewScanner(os.Stdin)
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print(" ... ")
		}
		if !sc.Scan() {
			fmt.Println()
			return
		}
		line := strings.TrimSpace(sc.Text())
		if buf.Len() == 0 && strings.HasPrefix(line, ".") {
			if handleMeta(db, line) {
				continue
			}
			// if not handled, fallthrough to buffer (allows dot lines inside statements if desired)
		}
		buf.WriteString(line)
		// multi-line: keep reading until we see a ';' terminator
		if strings.HasSuffix(line, ";") {
			sql := buf.String()
			sql = strings.TrimSuffix(sql, ";")
			buf.Reset()
			if strings.TrimSpace(sql) == "" {
				continue
			}
			p := NewParser(sql)
			st, err := p.ParseStatement()
			if err != nil {
				fmt.Println("ERR:", err)
				continue
			}
			rs, err := Execute(db, st)
			if err != nil {
				fmt.Println("ERR:", err)
				continue
			}
			printResult(rs)
		} else {
			buf.WriteString(" ")
		}
	}
}

func handleMeta(db *DB, line string) bool {
	switch {
	case line == ".help":
		fmt.Println(`
.meta commands:
  .help                 Show this help
  .tables               List tables
  .schema <table>       Show schema of a table
  .quit                 Exit`)
		return true
	case line == ".quit":
		os.Exit(0)
	case line == ".tables":
		tables := db.ListTables()
		if len(tables) == 0 {
			fmt.Println("(no tables)")
			return true
		}
		for _, t := range tables {
			fmt.Printf("%s  (cols=%d, rows=%d)\n", t.Name, len(t.Cols), len(t.Rows))
		}
		return true
	case strings.HasPrefix(line, ".schema"):
		parts := strings.Fields(line)
		if len(parts) != 2 {
			fmt.Println("usage: .schema <table>")
			return true
		}
		t, err := db.Get(parts[1])
		if err != nil {
			fmt.Println("ERR:", err)
			return true
		}
		for _, c := range t.Cols {
			fmt.Printf("%s %s\n", c.Name, c.Type.String())
		}
		return true
	default:
		fmt.Println("unknown meta command; try .help")
		return true
	}
	return true
}
