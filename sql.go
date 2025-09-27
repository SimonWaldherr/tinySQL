// TinySQL - a small educational SQL engine using only Go's standard library.
// Focus: clarity & reliability over performance and SQL coverage.

package tinysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

type ColType int

const (
	IntType ColType = iota
	FloatType
	TextType
	BoolType
	JsonType
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
	case JsonType:
		return "JSON"
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
	Rows    [][]any
	IsTemp  bool
	colPos  map[string]int
	Version int
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

type tenantDB struct {
	tables map[string]*Table
}
type DB struct {
	tenants map[string]*tenantDB
}

func NewDB() *DB { return &DB{tenants: make(map[string]*tenantDB)} }

func (db *DB) getTenant(tn string) *tenantDB {
	tn = strings.ToLower(tn)
	td := db.tenants[tn]
	if td == nil {
		td = &tenantDB{tables: map[string]*Table{}}
		db.tenants[tn] = td
	}
	return td
}
func (db *DB) Get(tn, name string) (*Table, error) {
	td := db.getTenant(tn)
	t, ok := td.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("no such table %q (tenant %q)", name, tn)
	}
	return t, nil
}
func (db *DB) Put(tn string, t *Table) error {
	td := db.getTenant(tn)
	lc := strings.ToLower(t.Name)
	if _, exists := td.tables[lc]; exists {
		return fmt.Errorf("table %q already exists (tenant %q)", t.Name, tn)
	}
	td.tables[lc] = t
	return nil
}
func (db *DB) Drop(tn, name string) error {
	td := db.getTenant(tn)
	lc := strings.ToLower(name)
	if _, ok := td.tables[lc]; !ok {
		return fmt.Errorf("no such table %q (tenant %q)", name, tn)
	}
	delete(td.tables, lc)
	return nil
}
func (db *DB) ListTables(tn string) []*Table {
	td := db.getTenant(tn)
	names := make([]string, 0, len(td.tables))
	for k := range td.tables {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]*Table, 0, len(names))
	for _, n := range names {
		out = append(out, td.tables[n])
	}
	return out
}

// DeepClone (für einfache Transaktionen/Checkpoints)
func (db *DB) DeepClone() *DB {
	out := NewDB()
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			cols := make([]Column, len(t.Cols))
			copy(cols, t.Cols)
			nt := NewTable(t.Name, cols, t.IsTemp)
			nt.Version = t.Version
			nt.Rows = make([][]any, len(t.Rows))
			for i := range t.Rows {
				row := make([]any, len(t.Rows[i]))
				copy(row, t.Rows[i])
				nt.Rows[i] = row
			}
			out.Put(tn, nt)
		}
	}
	return out
}

type TokenType int

const (
	tEOF TokenType = iota
	tIdent
	tNumber
	tString
	tSymbol
	tKeyword
)

type Token struct {
	Typ TokenType
	Val string
	Pos int
}
type Lexer struct {
	s   string
	pos int
}

func newLexer(s string) *Lexer { return &Lexer{s: s} }
func (lx *Lexer) peek() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	return rune(lx.s[lx.pos])
}
func (lx *Lexer) peekN(n int) rune {
	p := lx.pos + n
	if p >= len(lx.s) {
		return 0
	}
	return rune(lx.s[p])
}
func (lx *Lexer) next() rune {
	if lx.pos >= len(lx.s) {
		return 0
	}
	r := rune(lx.s[lx.pos])
	lx.pos++
	return r
}
func (lx *Lexer) skipWS() {
	for {
		if lx.pos >= len(lx.s) {
			return
		}
		r := rune(lx.s[lx.pos])
		if unicode.IsSpace(r) {
			lx.pos++
			continue
		}
		// -- line comment
		if r == '-' && lx.peekN(1) == '-' {
			lx.pos += 2
			for lx.pos < len(lx.s) && lx.s[lx.pos] != '\n' {
				lx.pos++
			}
			continue
		}
		// /* block */
		if r == '/' && lx.peekN(1) == '*' {
			lx.pos += 2
			for lx.pos < len(lx.s) {
				if lx.s[lx.pos] == '*' && lx.peekN(1) == '/' {
					lx.pos += 2
					break
				}
				lx.pos++
			}
			continue
		}
		return
	}
}
func (lx *Lexer) Next() Token {
	lx.skipWS()
	start := lx.pos
	if start >= len(lx.s) {
		return Token{Typ: tEOF, Val: "", Pos: start}
	}
	r := lx.peek()
	// string
	if r == '\'' {
		lx.next()
		var sb strings.Builder
		for {
			if lx.pos >= len(lx.s) {
				break
			}
			ch := lx.next()
			if ch == '\'' {
				if lx.peek() == '\'' {
					lx.next()
					sb.WriteRune('\'')
					continue
				}
				break
			}
			sb.WriteRune(ch)
		}
		return Token{Typ: tString, Val: sb.String(), Pos: start}
	}
	// number
	if unicode.IsDigit(r) {
		var sb strings.Builder
		dot := false
		for lx.pos < len(lx.s) {
			ch := lx.peek()
			if unicode.IsDigit(ch) || (!dot && ch == '.') {
				if ch == '.' {
					dot = true
				}
				sb.WriteByte(byte(ch))
				lx.pos++
			} else {
				break
			}
		}
		return Token{Typ: tNumber, Val: sb.String(), Pos: start}
	}
	// ident/keyword
	if unicode.IsLetter(r) || r == '_' {
		var sb strings.Builder
		for lx.pos < len(lx.s) {
			ch := lx.peek()
			if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
				sb.WriteByte(byte(ch))
				lx.pos++
			} else {
				break
			}
		}
		val := sb.String()
		up := strings.ToUpper(val)
		if isKeyword(up) {
			return Token{Typ: tKeyword, Val: up, Pos: start}
		}
		return Token{Typ: tIdent, Val: val, Pos: start}
	}
	// symbol
	switch r {
	case '(', ')', ',', '*', '+', '-', '/', '.', ';', '?':
		lx.next()
		return Token{Typ: tSymbol, Val: string(r), Pos: start}
	case '=', '<', '>', '!':
		a := lx.next()
		b := lx.peek()
		if (a == '<' && (b == '=' || b == '>')) || (a == '>' && b == '=') || (a == '!' && b == '=') {
			lx.next()
			return Token{Typ: tSymbol, Val: string(a) + string(b), Pos: start}
		}
		return Token{Typ: tSymbol, Val: string(a), Pos: start}
	default:
		lx.next()
		return Token{Typ: tSymbol, Val: string(r), Pos: start}
	}
}

func isKeyword(up string) bool {
	switch up {
	case "SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
		"ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
		"JOIN", "LEFT", "RIGHT", "OUTER", "ON", "AS",
		"CREATE", "TABLE", "TEMP", "DROP",
		"INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE",
		"INT", "FLOAT", "TEXT", "BOOL", "JSON",
		"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE",
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"COALESCE", "NULLIF",
		"JSON_GET":
		return true
	default:
		return false
	}
}

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

type Expr interface{}

type (
	VarRef  struct{ Name string }
	Literal struct{ Val any }
	Unary   struct {
		Op   string
		Expr Expr
	}
	Binary struct {
		Op    string
		Left  Expr
		Right Expr
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
	Alias string
}
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
		return nil, p.errf("expected table after FROM")
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
		n := p.parseIntLiteral()
		if n == nil {
			return nil, p.errf("LIMIT expects integer")
		}
		sel.Limit = n
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		n := p.parseIntLiteral()
		if n == nil {
			return nil, p.errf("OFFSET expects integer")
		}
		sel.Offset = n
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
			return nil, p.errf("unknown type for column %q", name)
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
		case "JSON":
			p.next()
			return JsonType
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

type Row map[string]any // keys lower-cased

type resultSet struct {
	Cols []string
	Rows []Row
}

type ExecEnv struct {
	ctx    context.Context
	tenant string
}

func GetVal(row Row, name string) (any, bool) { v, ok := row[strings.ToLower(name)]; return v, ok }
func PutVal(row Row, key string, val any)     { row[strings.ToLower(key)] = val }
func isNull(v any) bool                       { return v == nil }
func numeric(v any) (float64, bool) {
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
func truthy(v any) bool {
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

// tri-state boolean
const (
	tvFalse   = 0
	tvTrue    = 1
	tvUnknown = 2
)

func toTri(v any) int {
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

func compare(a, b any) (int, error) {
	if a == nil || b == nil {
		return 0, errors.New("cannot compare with NULL")
	}
	switch ax := a.(type) {
	case int:
		if f, ok := numeric(b); ok {
			af := float64(ax)
			switch {
			case af < f:
				return -1, nil
			case af > f:
				return 1, nil
			default:
				return 0, nil
			}
		}
	case float64:
		if f, ok := numeric(b); ok {
			switch {
			case ax < f:
				return -1, nil
			case ax > f:
				return 1, nil
			default:
				return 0, nil
			}
		}
	case string:
		if bs, ok := b.(string); ok {
			switch {
			case ax < bs:
				return -1, nil
			case ax > bs:
				return 1, nil
			default:
				return 0, nil
			}
		}
	case bool:
		if bb, ok := b.(bool); ok {
			switch {
			case !ax && bb:
				return -1, nil
			case ax && !bb:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable %T and %T", a, b)
}
func compareForOrder(a, b any, desc bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
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
	c, err := compare(a, b)
	if err != nil {
		return 0
	}
	return c
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func evalExpr(env ExecEnv, e Expr, row Row) (any, error) {
	if err := checkCtx(env.ctx); err != nil {
		return nil, err
	}
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
		v, err := evalExpr(env, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		is := isNull(v)
		if ex.Negate {
			return !is, nil
		}
		return is, nil
	case *Unary:
		v, err := evalExpr(env, ex.Expr, row)
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
			return nil, fmt.Errorf("unary + non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary - non-numeric")
		case "NOT":
			return triToValue(triNot(toTri(v))), nil
		}
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" {
			lv, err := evalExpr(env, ex.Left, row)
			if err != nil {
				return nil, err
			}
			if ex.Op == "AND" && toTri(lv) == tvFalse {
				return false, nil
			}
			if ex.Op == "OR" && toTri(lv) == tvTrue {
				return true, nil
			}
			rv, err := evalExpr(env, ex.Right, row)
			if err != nil {
				return nil, err
			}
			if ex.Op == "AND" {
				return triToValue(triAnd(toTri(lv), toTri(rv))), nil
			}
			return triToValue(triOr(toTri(lv), toTri(rv))), nil
		}
		lv, err := evalExpr(env, ex.Left, row)
		if err != nil {
			return nil, err
		}
		rv, err := evalExpr(env, ex.Right, row)
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
		}
	case *FuncCall:
		switch ex.Name {
		case "COALESCE":
			for _, a := range ex.Args {
				v, err := evalExpr(env, a, row)
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
				return nil, fmt.Errorf("NULLIF expects 2 args")
			}
			lv, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			rv, err := evalExpr(env, ex.Args[1], row)
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
		case "JSON_GET":
			if len(ex.Args) != 2 {
				return nil, fmt.Errorf("JSON_GET expects (json, path)")
			}
			jv, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			pv, err := evalExpr(env, ex.Args[1], row)
			if err != nil {
				return nil, err
			}
			ps, _ := pv.(string)
			return jsonGet(jv, ps), nil
		case "COUNT":
			if ex.Star {
				return 1, nil
			}
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("COUNT expects 1 arg")
			}
			v, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			if v == nil {
				return 0, nil
			}
			return 1, nil
		case "SUM", "AVG", "MIN", "MAX":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
			}
			v, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}
	return nil, fmt.Errorf("unknown expression")
}
func triToValue(t int) any {
	if t == tvTrue {
		return true
	}
	if t == tvFalse {
		return false
	}
	return nil
}
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
func evalAggregate(env ExecEnv, e Expr, rows []Row) (any, error) {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT":
			if ex.Star {
				return len(rows), nil
			}
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("COUNT expects 1 arg")
			}
			c := 0
			for _, r := range rows {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if v != nil {
					c++
				}
			}
			return c, nil
		case "SUM", "AVG":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
			}
			sum := 0.0
			n := 0
			for _, r := range rows {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, ex.Args[0], r)
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
				return nil, nil
			}
			return sum / float64(n), nil
		case "MIN", "MAX":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
			}
			var have bool
			var best any
			for _, r := range rows {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, ex.Args[0], r)
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
		}
	case *Unary:
		v, err := evalAggregate(env, ex.Expr, rows)
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
			return nil, fmt.Errorf("unary + non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary - non-numeric")
		case "NOT":
			return triToValue(triNot(toTri(v))), nil
		}
	case *Binary:
		lv, err := evalAggregate(env, ex.Left, rows)
		if err != nil {
			return nil, err
		}
		rv, err := evalAggregate(env, ex.Right, rows)
		if err != nil {
			return nil, err
		}
		return evalExpr(env, &Binary{Op: ex.Op, Left: &Literal{Val: lv}, Right: &Literal{Val: rv}}, Row{})
	case *IsNull:
		v, err := evalAggregate(env, ex.Expr, rows)
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
		return evalExpr(env, e, rows[0])
	}
	return nil, fmt.Errorf("unsupported aggregate")
}

func rowsFromTable(t *Table, alias string) ([]Row, []string) {
	cols := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		cols[i] = strings.ToLower(alias + "." + c.Name)
	}
	var out []Row
	for _, r := range t.Rows {
		row := Row{}
		for i, c := range t.Cols {
			PutVal(row, alias+"."+c.Name, r[i])
		}
		for i, c := range t.Cols {
			if _, exists := row[strings.ToLower(c.Name)]; !exists {
				PutVal(row, c.Name, r[i])
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
			if a, ok := ex.Args[0].(*VarRef); ok {
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
func fmtKeyPart(v any) string {
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
		b, _ := json.Marshal(x)
		return "J:" + string(b)
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

func inferType(v any) ColType {
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
		return JsonType
	}
}
func coerceToTypeAllowNull(v any, t ColType) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case IntType:
		switch x := v.(type) {
		case int:
			return x, nil
		case int64:
			return int(x), nil
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
		case int64:
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
		case int, int64:
			return x != 0, nil
		case float64:
			return x != 0, nil
		case string:
			s := strings.ToLower(strings.TrimSpace(x))
			return s == "true" || s == "1" || s == "t" || s == "yes", nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BOOL", v)
		}
	case JsonType:
		switch x := v.(type) {
		case string:
			var anyv any
			if err := json.Unmarshal([]byte(x), &anyv); err == nil {
				return anyv, nil
			}
			return x, nil
		default:
			return x, nil
		}
	default:
		return v, nil
	}
}

func cellString(v any) string {
	if v == nil {
		return "NULL"
	}
	switch vv := v.(type) {
	case map[string]any, []any:
		b, _ := json.Marshal(vv)
		return string(b)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func Execute(ctx context.Context, db *DB, tenant string, stmt Statement) (*resultSet, error) {
	env := ExecEnv{ctx: ctx, tenant: tenant}
	switch s := stmt.(type) {
	case *CreateTable:
		if s.AsSelect == nil {
			t := NewTable(s.Name, s.Cols, s.IsTemp)
			return nil, db.Put(tenant, t)
		}
		rs, err := Execute(ctx, db, tenant, s.AsSelect)
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
			row := make([]any, len(cols))
			for i, c := range cols {
				row[i] = r[strings.ToLower(c.Name)]
			}
			t.Rows = append(t.Rows, row)
		}
		return nil, db.Put(tenant, t)
	case *DropTable:
		return nil, db.Drop(tenant, s.Name)
	case *Insert:
		t, err := db.Get(tenant, s.Table)
		if err != nil {
			return nil, err
		}
		if len(s.Cols) > 0 && len(s.Cols) != len(s.Vals) {
			return nil, fmt.Errorf("INSERT column/value count mismatch")
		}
		row := make([]any, len(t.Cols))
		for i := range row {
			row[i] = nil
		}
		tmp := Row{}
		if len(s.Cols) == 0 {
			if len(s.Vals) != len(t.Cols) {
				return nil, fmt.Errorf("INSERT without columns expects %d values", len(t.Cols))
			}
			for i, e := range s.Vals {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, e, tmp)
				if err != nil {
					return nil, err
				}
				cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
				}
				row[i] = cv
			}
		} else {
			for i, name := range s.Cols {
				idx, err := t.colIndex(name)
				if err != nil {
					return nil, err
				}
				v, err := evalExpr(env, s.Vals[i], tmp)
				if err != nil {
					return nil, err
				}
				cv, err := coerceToTypeAllowNull(v, t.Cols[idx].Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
				}
				row[idx] = cv
			}
		}
		t.Rows = append(t.Rows, row)
		t.Version++
		return nil, nil
	case *Update:
		t, err := db.Get(tenant, s.Table)
		if err != nil {
			return nil, err
		}
		setIdx := map[int]Expr{}
		for name, ex := range s.Sets {
			i, err := t.colIndex(name)
			if err != nil {
				return nil, err
			}
			setIdx[i] = ex
		}
		n := 0
		for ri, r := range t.Rows {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
			row := Row{}
			for i, c := range t.Cols {
				PutVal(row, c.Name, r[i])
				PutVal(row, s.Table+"."+c.Name, r[i])
			}
			ok := true
			if s.Where != nil {
				v, err := evalExpr(env, s.Where, row)
				if err != nil {
					return nil, err
				}
				ok = (toTri(v) == tvTrue)
			}
			if ok {
				for i, ex := range setIdx {
					v, err := evalExpr(env, ex, row)
					if err != nil {
						return nil, err
					}
					cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
					if err != nil {
						return nil, err
					}
					t.Rows[ri][i] = cv
				}
				n++
			}
		}
		t.Version++
		return &resultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": n}}}, nil
	case *Delete:
		t, err := db.Get(tenant, s.Table)
		if err != nil {
			return nil, err
		}
		var kept [][]any
		del := 0
		for _, r := range t.Rows {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
			row := Row{}
			for i, c := range t.Cols {
				PutVal(row, c.Name, r[i])
				PutVal(row, s.Table+"."+c.Name, r[i])
			}
			keep := true
			if s.Where != nil {
				v, err := evalExpr(env, s.Where, row)
				if err != nil {
					return nil, err
				}
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
		leftT, err := db.Get(tenant, s.From.Table)
		if err != nil {
			return nil, err
		}
		leftRows, _ := rowsFromTable(leftT, aliasOr(s.From))
		cur := leftRows
		// JOINs
		for _, j := range s.Joins {
			rt, err := db.Get(tenant, j.Right.Table)
			if err != nil {
				return nil, err
			}
			rightRows, _ := rowsFromTable(rt, aliasOr(j.Right))
			switch j.Type {
			case JoinInner:
				var joined []Row
				for _, l := range cur {
					if err := checkCtx(ctx); err != nil {
						return nil, err
					}
					for _, r := range rightRows {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(env, j.On, m)
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
					if err := checkCtx(ctx); err != nil {
						return nil, err
					}
					matched := false
					for _, r := range rightRows {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(env, j.On, m)
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
						addRightNulls(m, aliasOr(j.Right), rt)
						joined = append(joined, m)
					}
				}
				cur = joined
			case JoinRight:
				var joined []Row
				leftKeys := []string{}
				if len(cur) > 0 {
					leftKeys = keysOfRow(cur[0])
				}
				for _, r := range rightRows {
					if err := checkCtx(ctx); err != nil {
						return nil, err
					}
					matched := false
					for _, l := range cur {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(env, j.On, m)
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
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, s.Where, r)
				if err != nil {
					return nil, err
				}
				if toTri(v) == tvTrue {
					tmp = append(tmp, r)
				}
			}
			filtered = tmp
		}
		// GROUP/HAVING
		needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)
		var outRows []Row
		var outCols []string
		if needAgg {
			groups := map[string][]Row{}
			var orderKeys []string
			for _, r := range filtered {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
				var parts []string
				for _, g := range s.GroupBy {
					v, err := evalExpr(env, &g, r)
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
			for _, k := range orderKeys {
				rows := groups[k]
				if s.Having != nil {
					hv, err := evalAggregate(env, s.Having, rows)
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
									PutVal(out, col, v)
									outCols = appendUnique(outCols, col)
								}
							}
						}
						continue
					}
					name := projName(it, i)
					var val any
					var err error
					if isAggregate(it.Expr) || len(s.GroupBy) > 0 {
						val, err = evalAggregate(env, it.Expr, rows)
					} else {
						val, err = evalExpr(env, it.Expr, rows[0])
					}
					if err != nil {
						return nil, err
					}
					PutVal(out, name, val)
					outCols = appendUnique(outCols, name)
				}
				outRows = append(outRows, out)
			}
		} else {
			for _, r := range filtered {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
				out := Row{}
				for i, it := range s.Projs {
					if it.Star {
						for col, v := range r {
							if strings.Contains(col, ".") {
								PutVal(out, col, v)
								outCols = appendUnique(outCols, col)
							}
						}
						continue
					}
					val, err := evalExpr(env, it.Expr, r)
					if err != nil {
						return nil, err
					}
					name := projName(it, i)
					PutVal(out, name, val)
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
		// OFFSET/LIMIT
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

func aliasOr(f FromItem) string {
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
func addRightNulls(m Row, alias string, t *Table) {
	for _, c := range t.Cols {
		PutVal(m, alias+"."+c.Name, nil)
		if _, ex := m[strings.ToLower(c.Name)]; !ex {
			PutVal(m, c.Name, nil)
		}
	}
}

func jsonGet(v any, path string) any {
	if v == nil || path == "" {
		return nil
	}
	parts := parseJSONPath(path)
	cur := v
	for _, p := range parts {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[p.key]
		case []any:
			if p.idx >= 0 && p.idx < len(c) {
				cur = c[p.idx]
			} else {
				return nil
			}
		default:
			return nil
		}
	}
	return cur
}

type pathPart struct {
	key string
	idx int
}

func parseJSONPath(s string) []pathPart {
	var out []pathPart
	cur := ""
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
		case '[':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
			j := i + 1
			for j < len(s) && s[j] != ']' {
				j++
			}
			if j <= len(s)-1 {
				n, _ := strconv.Atoi(s[i+1 : j])
				out = append(out, pathPart{idx: n})
				i = j
			}
		default:
			cur += string(s[i])
		}
	}
	if cur != "" {
		out = append(out, pathPart{key: cur, idx: -1})
	}
	return out
}

type diskColumn struct {
	Name string
	Type ColType
}
type diskTable struct {
	Tenant string
	Name   string
	Cols   []diskColumn
	Rows   [][]any
	IsTemp bool
}

func saveDBToFile(db *DB, filename string) error {
	var dump []diskTable
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			dt := diskTable{Tenant: tn, Name: t.Name, IsTemp: t.IsTemp}
			for _, c := range t.Cols {
				dt.Cols = append(dt.Cols, diskColumn{Name: c.Name, Type: c.Type})
			}
			for _, r := range t.Rows {
				row := make([]any, len(r))
				for i, v := range r {
					if v == nil {
						row[i] = nil
						continue
					}
					if t.Cols[i].Type == JsonType {
						b, _ := json.Marshal(v)
						row[i] = string(b)
					} else {
						row[i] = v
					}
				}
				dt.Rows = append(dt.Rows, row)
			}
			dump = append(dump, dt)
		}
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	return enc.Encode(dump)
}

func loadDBFromFile(filename string) (*DB, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return NewDB(), nil
		}
		return nil, err
	}
	defer f.Close()
	var dump []diskTable
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&dump); err != nil {
		if errors.Is(err, io.EOF) {
			return NewDB(), nil
		}
		return nil, err
	}
	db := NewDB()
	for _, dt := range dump {
		var cols []Column
		for _, c := range dt.Cols {
			cols = append(cols, Column{Name: c.Name, Type: c.Type})
		}
		t := NewTable(dt.Name, cols, dt.IsTemp)
		for _, r := range dt.Rows {
			row := make([]any, len(r))
			for i, v := range r {
				if v == nil {
					row[i] = nil
					continue
				}
				if cols[i].Type == JsonType {
					var anyv any
					if s, ok := v.(string); ok {
						if json.Unmarshal([]byte(s), &anyv) == nil {
							row[i] = anyv
						} else {
							row[i] = s
						}
					} else {
						row[i] = v
					}
				} else {
					row[i] = v
				}
			}
			t.Rows = append(t.Rows, row)
		}
		_ = db.Put(dt.Tenant, t)
	}
	return db, nil
}

type server struct {
	mu       sync.RWMutex
	db       *DB
	filePath string
	autosave bool
}

func newServer(db *DB, path string, autosave bool) *server {
	return &server{db: db, filePath: path, autosave: autosave}
}
func (s *server) saveIfNeeded() {
	if s.autosave && s.filePath != "" {
		_ = saveDBToFile(s.db, s.filePath)
	}
}

type cfg struct {
	tenant   string
	filePath string // "" => mem
	autosave bool
}

func parseDSN(dsn string) (cfg, error) {
	//   "mem://?tenant=acme"
	//   "file:/tmp/tinysql.db?tenant=acme&autosave=1"
	var c cfg
	c.tenant = "default"
	switch {
	case strings.HasPrefix(dsn, "mem://"):
		q := ""
		if i := strings.Index(dsn, "?"); i >= 0 {
			q = dsn[i+1:]
		}
		for _, kv := range strings.Split(q, "&") {
			if kv == "" {
				continue
			}
			parts := strings.SplitN(kv, "=", 2)
			k := parts[0]
			v := ""
			if len(parts) == 2 {
				v = parts[1]
			}
			switch k {
			case "tenant":
				if v != "" {
					c.tenant = v
				}
			case "autosave":
				c.autosave = (v == "1" || strings.ToLower(v) == "true")
			}
		}
		return c, nil
	case strings.HasPrefix(dsn, "file:"):
		path := strings.TrimPrefix(dsn, "file:")
		if i := strings.Index(path, "?"); i >= 0 {
			q := path[i+1:]
			path = path[:i]
			for _, kv := range strings.Split(q, "&") {
				if kv == "" {
					continue
				}
				parts := strings.SplitN(kv, "=", 2)
				k := parts[0]
				v := ""
				if len(parts) == 2 {
					v = parts[1]
				}
				switch k {
				case "tenant":
					if v != "" {
						c.tenant = v
					}
				case "autosave":
					c.autosave = (v == "1" || strings.ToLower(v) == "true")
				}
			}
		}
		if path == "" {
			return c, fmt.Errorf("file: path required")
		}
		c.filePath = filepath.Clean(path)
		return c, nil
	default:
		// Kompatibilitätsmodus: DSN leer -> mem
		if dsn == "" {
			return c, nil
		}
		return c, fmt.Errorf("unsupported DSN (use mem:// or file:)")
	}
}

type drv struct{ srv *server }

func (d *drv) Open(name string) (driver.Conn, error) {
	c, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	var s *server
	if d.srv != nil {
		s = d.srv
	} else {
		var db *DB
		if c.filePath != "" {
			db, err = loadDBFromFile(c.filePath)
			if err != nil {
				return nil, err
			}
		} else {
			db = NewDB()
		}
		s = newServer(db, c.filePath, c.autosave)
	}
	return &conn{srv: s, tenant: c.tenant}, nil
}

type conn struct {
	srv    *server
	tenant string
	inTx bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{c: c, sql: query}, nil
}
func (c *conn) Close() error {
	c.srv.saveIfNeeded()
	return nil
}
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// ConnBeginTx
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.srv.mu.Lock()
	c.inTx = true
	return &tx{c: c}, nil
}

type tx struct{ c *conn }

func (t *tx) Commit() error {
	t.c.inTx = false
	t.c.srv.saveIfNeeded()
	t.c.srv.mu.Unlock()
	return nil
}
func (t *tx) Rollback() error {
	t.c.inTx = false
	t.c.srv.mu.Unlock()
	return nil
}

// Query/Exec Context

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.execSQL(ctx, sqlStr)
}
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := bindPlaceholders(query, args)
	if err != nil {
		return nil, err
	}
	return c.querySQL(ctx, sqlStr)
}

// Fallback (database/sql ruft diese auf, wenn *Context nicht unterstützt)
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.ExecContext(context.Background(), query, named)
}
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.QueryContext(context.Background(), query, named)
}

func (c *conn) execSQL(ctx context.Context, sqlStr string) (driver.Result, error) {
	// Parser/Executor
	p := NewParser(sqlStr)
	st, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}
	// write detection
	isWrite := func(s Statement) bool {
		switch s.(type) {
		case *CreateTable, *DropTable, *Insert, *Update, *Delete:
			return true
		default:
			return false
		}
	}
	if isWrite(st) && !c.inTx {
		c.srv.mu.Lock()
		defer c.srv.mu.Unlock()
	}
	if !isWrite(st) {
		c.srv.mu.RLock()
		defer c.srv.mu.RUnlock()
	}
	rs, err := Execute(ctx, c.srv.db, c.tenant, st)
	if err != nil {
		return nil, err
	}
	var rowsAffected int64 = 0
	if rs != nil && len(rs.Rows) == 1 && len(rs.Cols) == 1 {
		switch strings.ToLower(rs.Cols[0]) {
		case "updated", "deleted":
			if n, ok := rs.Rows[0][rs.Cols[0]].(int); ok {
				rowsAffected = int64(n)
			}
		}
	}
	if isWrite(st) && !c.inTx {
		c.srv.saveIfNeeded()
	}
	return driver.RowsAffected(rowsAffected), nil
}

func (c *conn) querySQL(ctx context.Context, sqlStr string) (driver.Rows, error) {
	p := NewParser(sqlStr)
	st, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}
	// Nur SELECT produziert Rows
	_, ok := st.(*Select)
	if !ok {
		// für DDL/DML als Query: führe aus und gebe "empty rows" zurück
		if _, err := c.execSQL(ctx, sqlStr); err != nil {
			return nil, err
		}
		return emptyRows{}, nil
	}
	c.srv.mu.RLock()
	defer c.srv.mu.RUnlock()
	rs, err := Execute(ctx, c.srv.db, c.tenant, st)
	if err != nil {
		return nil, err
	}
	return &rows{rs: rs}, nil
}

// NamedValueChecker: normalize Values (time.Time->RFC3339 string, []byte->base64)
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch v := nv.Value.(type) {
	case time.Time:
		nv.Value = v.UTC().Format(time.RFC3339Nano)
	case []byte:
		nv.Value = base64.StdEncoding.EncodeToString(v)
	case int:
		nv.Value = int64(v)
	}
	return nil
}

type stmt struct {
	c   *conn
	sql string
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 } // variadisch

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	nv := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), nv)
}
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	nv := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), nv)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.execSQL(ctx, sqlStr)
}
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.querySQL(ctx, sqlStr)
}

// Rows

type rows struct {
	rs   *resultSet
	i    int
	once bool
}

func (r *rows) Columns() []string { return r.rs.Cols }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.rs.Rows) {
		return io.EOF
	}
	row := r.rs.Rows[r.i]
	for i, c := range r.rs.Cols {
		v := row[strings.ToLower(c)]
		switch vv := v.(type) {
		case nil:
			dest[i] = nil
		case int:
			dest[i] = int64(vv)
		case int64:
			dest[i] = vv
		case float64:
			dest[i] = vv
		case bool:
			dest[i] = vv
		case string:
			dest[i] = vv
		default:
			// JSON/andere -> JSON-String
			b, _ := json.Marshal(vv)
			dest[i] = string(b)
		}
	}
	r.i++
	return nil
}

// Optional ColumnType* interfaces
func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	// Unbekannt -> TEXT
	// In dieser einfachen Implementierung kennen wir die Spaltentypen im ResultSet
	// nicht sicher (Expressions). Wir geben heuristisch TEXT zurück.
	return "TEXT"
}
func (r *rows) ColumnTypeNullable(i int) (nullable, ok bool) { return true, true }
func (r *rows) ColumnTypeScanType(i int) reflectType {
	// ohne import reflect/Stringer: einfacher Alias
	return reflectType("interface{}")
}

// helper alias to avoid importing reflect; database/sql nutzt das nur informativ
type reflectType string

// emptyRows: für Exec-as-Query
type emptyRows struct{}

func (emptyRows) Columns() []string                     { return []string{} }
func (emptyRows) Close() error                          { return nil }
func (emptyRows) Next(dest []driver.Value) error        { return io.EOF }
func (emptyRows) ColumnTypeDatabaseTypeName(int) string { return "TEXT" }
func (emptyRows) ColumnTypeNullable(int) (bool, bool)   { return true, true }
func (emptyRows) ColumnTypeScanType(int) reflectType    { return reflectType("interface{}") }

// Placeholder-Bindung: ersetzt '?' in SQL sicher durch Literale
func bindPlaceholders(sqlStr string, args []driver.NamedValue) (string, error) {
	var sb strings.Builder
	argi := 0
	for i := 0; i < len(sqlStr); i++ {
		ch := sqlStr[i]
		if ch == '\'' {
			// Strings unverändert durchkopieren (mit Escapes)
			sb.WriteByte(ch)
			i++
			for i < len(sqlStr) {
				sb.WriteByte(sqlStr[i])
				if sqlStr[i] == '\'' {
					if i+1 < len(sqlStr) && sqlStr[i+1] == '\'' {
						i++
						sb.WriteByte(sqlStr[i])
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if ch == '?' {
			if argi >= len(args) {
				return "", fmt.Errorf("not enough args for placeholders")
			}
			sb.WriteString(sqlLiteral(args[argi].Value))
			argi++
			continue
		}
		sb.WriteByte(ch)
	}
	if argi != len(args) {
		return "", fmt.Errorf("too many args for placeholders")
	}
	return sb.String(), nil
}
func sqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case string:
		s := strings.ReplaceAll(x, "'", "''")
		return "'" + s + "'"
	default:
		// JSON-fallback
		b, _ := json.Marshal(x)
		s := strings.ReplaceAll(string(b), "'", "''")
		return "'" + s + "'"
	}
}

func printResult(rs *resultSet) {
	if rs == nil {
		return
	}
	if len(rs.Rows) == 1 && len(rs.Cols) == 1 && (strings.ToLower(rs.Cols[0]) == "updated" || strings.ToLower(rs.Cols[0]) == "deleted") {
		for _, r := range rs.Rows {
			fmt.Printf("%s: %v\n", rs.Cols[0], r[strings.ToLower(rs.Cols[0])])
		}
		return
	}
	// Breiten
	w := make([]int, len(rs.Cols))
	for i, c := range rs.Cols {
		w[i] = len(c)
	}
	for _, r := range rs.Rows {
		for i, c := range rs.Cols {
			s := cellString(r[strings.ToLower(c)])
			if len(s) > w[i] {
				w[i] = len(s)
			}
		}
	}
	// Header
	for i, c := range rs.Cols {
		fmt.Print(padRight(c, w[i]))
		if i < len(rs.Cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for i := range rs.Cols {
		fmt.Print(strings.Repeat("-", w[i]))
		if i < len(rs.Cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for _, r := range rs.Rows {
		for i, c := range rs.Cols {
			s := cellString(r[strings.ToLower(c)])
			fmt.Print(padRight(s, w[i]))
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

func init() {
	// database/sql Treiber registrieren
	sql.Register("tinysql", &drv{})
	// gob registrations (sicher nicht zwingend, aber gut)
	gob.Register(map[string]any{})
	gob.Register([]any{})
}
