// Package engine provides a hand-written SQL parser for tinySQL.
//
// What: It parses a practical subset of SQL into an AST (statements and
// expressions) used by the execution engine. Supported features include DDL,
// DML, SELECT with JOIN/GROUP/HAVING/ORDER/LIMIT/OFFSET, and set ops.
// How: A straightforward recursive-descent parser over a small token stream
// from the lexer. It favors clarity and precise error messages. Ident-like
// parsing accepts keywords as identifiers to keep the grammar practical for
// common column names.
// Why: A small, readable parser is easy to extend and reason about, enabling
// rapid iteration on language features without a complex generator toolchain.
package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Parser holds the lexer and current/peek tokens for recursive-descent parsing.
type Parser struct {
	lx   *lexer
	cur  token
	peek token
}

// NewParser creates a new SQL parser for the provided input string.
func NewParser(sql string) *Parser {
	p := &Parser{lx: newLexer(sql)}
	p.cur = p.lx.nextToken()
	p.peek = p.lx.nextToken()
	return p
}
func (p *Parser) next() { p.cur, p.peek = p.peek, p.lx.nextToken() }
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
	// VarRef refers to a column (qualified or unqualified) in expressions.
	VarRef struct{ Name string }
	// Literal holds a constant value (number, string, bool, NULL).
	Literal struct{ Val any }
	// Unary represents unary operators like +, -, NOT.
	Unary struct {
		Op   string
		Expr Expr
	}
	// Binary represents binary operators (+,-,*,/, comparisons, AND/OR).
	Binary struct {
		Op          string
		Left, Right Expr
	}
	// IsNull represents IS [NOT] NULL predicate.
	IsNull struct {
		Expr   Expr
		Negate bool
	}
	// FuncCall represents a function call, optionally with a star (COUNT(*)).
	FuncCall struct {
		Name     string
		Args     []Expr
		Star     bool
		Distinct bool        // For COUNT(DISTINCT col)
		Over     *OverClause // For window functions
	}
	// InExpr represents "expr IN (val1, val2, ...)"
	InExpr struct {
		Expr   Expr
		Values []Expr
		Negate bool // For NOT IN
	}
	// LikeExpr represents "expr LIKE pattern [ESCAPE char]"
	LikeExpr struct {
		Expr    Expr
		Pattern Expr
		Escape  Expr // Optional ESCAPE character
		Negate  bool // For NOT LIKE
	}
	// CaseExpr represents a CASE ... WHEN ... THEN ... [ELSE ...] END expression.
	CaseExpr struct {
		Operand Expr
		Whens   []CaseWhen
		Else    Expr
	}
	// CaseWhen pairs WHEN condition and THEN result expressions.
	CaseWhen struct {
		When Expr
		Then Expr
	}
	// SubqueryExpr wraps a SELECT used as an expression (scalar subquery).
	SubqueryExpr struct {
		Select *Select
	}
)

// Statement is the root interface for all parsed SQL statements.
type Statement interface{}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	Name        string
	Cols        []storage.Column
	IsTemp      bool
	AsSelect    *Select
	IfNotExists bool // IF NOT EXISTS clause
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Name     string
	IfExists bool // IF EXISTS clause
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name        string
	Table       string
	Columns     []string
	Unique      bool
	IfNotExists bool
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	Name     string
	Table    string // Optional: some DBs require table name
	IfExists bool
}

// CreateView represents a CREATE VIEW statement.
type CreateView struct {
	Name        string
	Select      *Select
	IfNotExists bool
	OrReplace   bool
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Name     string
	IfExists bool
}

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	Table     string
	AddColumn *storage.Column // For ADD COLUMN
	// Future: DropColumn, RenameColumn, etc.
}

// Insert represents an INSERT statement.
type Insert struct {
	Table string
	Cols  []string
	Rows  [][]Expr
}

// Update represents an UPDATE statement.
type Update struct {
	Table string
	Sets  map[string]Expr
	Where Expr
}

// Delete represents a DELETE statement.
type Delete struct {
	Table string
	Where Expr
}

type JoinType int

const (
	// JoinInner represents INNER JOIN.
	JoinInner JoinType = iota
	// JoinLeft represents LEFT (OUTER) JOIN.
	JoinLeft
	// JoinRight represents RIGHT (OUTER) JOIN.
	JoinRight
)

// Select represents a SELECT query and its clauses.
type Select struct {
	Distinct   bool
	DistinctOn []Expr
	From       FromItem
	Joins      []JoinClause
	Projs      []SelectItem
	Where      Expr
	GroupBy    []Expr
	Having     Expr
	OrderBy    []OrderItem
	Limit      *int
	Offset     *int
	Union      *UnionClause // For UNION operations
	CTEs       []CTE        // Common Table Expressions
}

// CTE represents a Common Table Expression (WITH clause)
type CTE struct {
	Name   string
	Select *Select
	// Recursive indicates this CTE was declared under WITH RECURSIVE
	Recursive bool
}

type UnionType int

const (
	// UnionDistinct corresponds to UNION (distinct).
	UnionDistinct UnionType = iota
	// UnionAll corresponds to UNION ALL.
	UnionAll
	// Except corresponds to EXCEPT.
	Except
	// Intersect corresponds to INTERSECT.
	Intersect
)

// UnionClause represents a set operation chaining RIGHT select with current one.
type UnionClause struct {
	Type  UnionType
	Right *Select
	Next  *UnionClause // For chaining multiple UNIONs
}

// FromItem kann eine echte Tabelle oder ein Subselect (Derived Table) sein.
type FromItem struct {
	Table    string  // Tabellenname (wenn echte Tabelle)
	Alias    string  // Alias für Tabelle oder Subselect
	Subquery *Select // Falls abgeleitete Tabelle: das Select-Statement
}

// JoinClause holds a JOIN type with the right side and join condition.
type JoinClause struct {
	Type  JoinType
	Right FromItem
	On    Expr
}

// SelectItem represents a projection item, optionally with alias or *.
type SelectItem struct {
	Expr  Expr
	Alias string
	Star  bool
}

// OrderItem specifies ordering column and direction.
type OrderItem struct {
	Col  string
	Desc bool
}

// OverClause represents the OVER clause for window functions.
type OverClause struct {
	PartitionBy []Expr       // PARTITION BY expressions
	OrderBy     []OrderItem  // ORDER BY items
	Frame       *WindowFrame // ROWS/RANGE frame specification
}

// WindowFrame represents ROWS/RANGE BETWEEN frame specification.
type WindowFrame struct {
	Mode       string // "ROWS" or "RANGE"
	StartType  string // "UNBOUNDED", "CURRENT", or "OFFSET"
	StartValue int    // Offset value for PRECEDING/FOLLOWING
	EndType    string // "UNBOUNDED", "CURRENT", or "OFFSET"
	EndValue   int    // Offset value for PRECEDING/FOLLOWING
}

// ------------------------------ Parse ------------------------------

// ParseStatement parses a single SQL statement into an AST.
func (p *Parser) ParseStatement() (Statement, error) {
	if p.cur.Typ != tKeyword {
		return nil, p.errf("expected a statement")
	}

	// handler map for simple single-keyword dispatch
	handlers := map[string]func() (Statement, error){
		"CREATE": p.parseCreate,
		"DROP":   p.parseDrop,
		"ALTER":  p.parseAlter,
		"INSERT": p.parseInsert,
		"UPDATE": p.parseUpdate,
		"DELETE": p.parseDelete,
	}

	if h, ok := handlers[p.cur.Val]; ok {
		return h()
	}

	// SELECT and WITH are handled by the same parser
	if p.cur.Val == "SELECT" || p.cur.Val == "WITH" {
		return p.parseSelectWithCTE()
	}

	return nil, p.errf("expected a statement")
}

//nolint:gocyclo // CREATE statement grammar is broad and handled centrally here.
func (p *Parser) parseCreate() (Statement, error) {
	p.next()

	// Check for CREATE INDEX
	if p.cur.Typ == tKeyword && (p.cur.Val == "INDEX" || p.cur.Val == "UNIQUE") {
		return p.parseCreateIndex()
	}

	// Check for CREATE VIEW
	if p.cur.Typ == tKeyword && (p.cur.Val == "VIEW" || p.cur.Val == "OR") {
		return p.parseCreateView()
	}

	// CREATE TABLE logic
	isTemp := false
	if p.cur.Typ == tKeyword && p.cur.Val == "TEMP" {
		isTemp = true
		p.next()
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
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
		return &CreateTable{Name: name, Cols: cols, IsTemp: isTemp, IfNotExists: ifNotExists}, nil
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &CreateTable{Name: name, IsTemp: isTemp, AsSelect: sel, IfNotExists: ifNotExists}, nil
	}
	return nil, p.errf("expected '(' or AS SELECT")
}

func (p *Parser) parseDrop() (Statement, error) {
	p.next()

	// Check for DROP INDEX
	if p.cur.Typ == tKeyword && p.cur.Val == "INDEX" {
		return p.parseDropIndex()
	}

	// Check for DROP VIEW
	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		return p.parseDropView()
	}

	// DROP TABLE
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	// Check for IF EXISTS
	ifExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected table name")
	}
	return &DropTable{Name: name, IfExists: ifExists}, nil
}

//nolint:gocyclo // Index creation grammar includes many optional clauses.
func (p *Parser) parseCreateIndex() (Statement, error) {
	// Already consumed CREATE, cur should be INDEX or UNIQUE
	unique := false
	if p.cur.Typ == tKeyword && p.cur.Val == "UNIQUE" {
		unique = true
		p.next()
		if err := p.expectKeyword("INDEX"); err != nil {
			return nil, err
		}
	}

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	indexName := p.parseIdentLike()
	if indexName == "" {
		return nil, p.errf("expected index name")
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}

	tableName := p.parseIdentLike()
	if tableName == "" {
		return nil, p.errf("expected table name")
	}

	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}

	var columns []string
	for {
		col := p.parseIdentLike()
		if col == "" {
			return nil, p.errf("expected column name")
		}
		columns = append(columns, col)

		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		break
	}

	return &CreateIndex{
		Name:        indexName,
		Table:       tableName,
		Columns:     columns,
		Unique:      unique,
		IfNotExists: ifNotExists,
	}, nil
}

func (p *Parser) parseDropIndex() (Statement, error) {
	// Already consumed DROP INDEX
	p.next()

	// Check for IF EXISTS
	ifExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	indexName := p.parseIdentLike()
	if indexName == "" {
		return nil, p.errf("expected index name")
	}

	// Optional: ON table_name
	var tableName string
	if p.cur.Typ == tKeyword && p.cur.Val == "ON" {
		p.next()
		tableName = p.parseIdentLike()
	}

	return &DropIndex{
		Name:     indexName,
		Table:    tableName,
		IfExists: ifExists,
	}, nil
}

func (p *Parser) parseCreateView() (Statement, error) {
	// Already consumed CREATE, check for OR REPLACE
	orReplace := false
	if p.cur.Typ == tKeyword && p.cur.Val == "OR" {
		p.next()
		if err := p.expectKeyword("REPLACE"); err != nil {
			return nil, err
		}
		orReplace = true
	}

	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	viewName := p.parseIdentLike()
	if viewName == "" {
		return nil, p.errf("expected view name")
	}

	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	return &CreateView{
		Name:        viewName,
		Select:      sel,
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
	}, nil
}

func (p *Parser) parseDropView() (Statement, error) {
	// Already consumed DROP VIEW
	p.next()

	// Check for IF EXISTS
	ifExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	viewName := p.parseIdentLike()
	if viewName == "" {
		return nil, p.errf("expected view name")
	}

	return &DropView{
		Name:     viewName,
		IfExists: ifExists,
	}, nil
}

func (p *Parser) parseAlter() (Statement, error) {
	p.next()

	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	tableName := p.parseIdentLike()
	if tableName == "" {
		return nil, p.errf("expected table name")
	}

	if err := p.expectKeyword("ADD"); err != nil {
		return nil, err
	}

	// Optional COLUMN keyword
	if p.cur.Typ == tKeyword && p.cur.Val == "COLUMN" {
		p.next()
	}

	// Parse column definition
	colName := p.parseIdentLike()
	if colName == "" {
		return nil, p.errf("expected column name")
	}

	colType := p.parseType()
	if colType < 0 {
		return nil, p.errf("unknown column type")
	}

	col := storage.Column{
		Name: colName,
		Type: colType,
	}

	return &AlterTable{
		Table:     tableName,
		AddColumn: &col,
	}, nil
}

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
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	rows, err := p.parseInsertValueRows()
	if err != nil {
		return nil, err
	}
	return &Insert{Table: tname, Cols: cols, Rows: rows}, nil
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

func (p *Parser) parseSelectWithCTE() (*Select, error) {
	var ctes []CTE

	// Parse WITH clause if present
	if p.cur.Typ == tKeyword && p.cur.Val == "WITH" {
		p.next()

		// Optional RECURSIVE keyword applies to all following CTEs
		recursiveAll := false
		if p.cur.Typ == tKeyword && p.cur.Val == "RECURSIVE" {
			recursiveAll = true
			p.next()
		}

		for {
			// Parse CTE name
			cteName := p.parseIdentLike()
			if cteName == "" {
				return nil, p.errf("expected CTE name")
			}

			// Optional column list: WITH cte(col1, col2) AS (...)
			if p.cur.Typ == tSymbol && p.cur.Val == "(" {
				// consume '(' and skip until matching ')'
				p.next()
				for {
					// accept identifier-like column names
					if p.cur.Typ != tIdent && p.cur.Typ != tKeyword {
						return nil, p.errf("expected column name in CTE column list")
					}
					p.next()
					if p.cur.Typ == tSymbol && p.cur.Val == "," {
						p.next()
						continue
					}
					break
				}
				if err := p.expectSymbol(")"); err != nil {
					return nil, err
				}
			}

			if err := p.expectKeyword("AS"); err != nil {
				return nil, err
			}

			if err := p.expectSymbol("("); err != nil {
				return nil, err
			}

			// Parse the SELECT statement for this CTE
			cteSelect, err := p.parseSelect()
			if err != nil {
				return nil, err
			}

			if err := p.expectSymbol(")"); err != nil {
				return nil, err
			}

			ctes = append(ctes, CTE{Name: cteName, Select: cteSelect, Recursive: recursiveAll})

			// Check for more CTEs
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}

	// Parse the main SELECT statement
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	// Attach CTEs to the main SELECT
	sel.CTEs = ctes

	return sel, nil
}

func (p *Parser) parseSelect() (*Select, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &Select{}

	// Parse DISTINCT
	if err := p.parseDistinct(sel); err != nil {
		return nil, err
	}

	// Parse projection list
	if err := p.parseProjections(sel); err != nil {
		return nil, err
	}

	// Parse FROM
	if err := p.parseFromClause(sel); err != nil {
		return nil, err
	}

	// Parse JOINs
	if err := p.parseJoinClauses(sel); err != nil {
		return nil, err
	}

	// Parse WHERE
	if err := p.parseWhereClause(sel); err != nil {
		return nil, err
	}

	// Parse GROUP BY
	if err := p.parseGroupByClause(sel); err != nil {
		return nil, err
	}

	// Parse HAVING
	if err := p.parseHavingClause(sel); err != nil {
		return nil, err
	}

	// Parse ORDER BY
	if err := p.parseOrderByClause(sel); err != nil {
		return nil, err
	}

	// Parse LIMIT and OFFSET
	if err := p.parseLimitOffset(sel); err != nil {
		return nil, err
	}

	// Parse UNION clauses
	if err := p.parseUnionClause(sel); err != nil {
		return nil, err
	}

	return sel, nil
}

func (p *Parser) parseDistinct(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "DISTINCT" {
		p.next()
		// Check for DISTINCT ON (expr, ...)
		if p.cur.Typ == tKeyword && p.cur.Val == "ON" {
			p.next()
			if err := p.expectSymbol("("); err != nil {
				return err
			}
			var exprs []Expr
			for {
				e, err := p.parseExpr()
				if err != nil {
					return err
				}
				exprs = append(exprs, e)
				if p.cur.Typ == tSymbol && p.cur.Val == "," {
					p.next()
					continue
				}
				break
			}
			if err := p.expectSymbol(")"); err != nil {
				return err
			}
			sel.DistinctOn = exprs
			// Also mark generic Distinct true for compatibility
			sel.Distinct = true
			return nil
		}
		sel.Distinct = true
	}
	return nil
}

func (p *Parser) parseProjections(sel *Select) error {
	if p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		sel.Projs = append(sel.Projs, SelectItem{Star: true})
		return nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return err
		}
		alias := ""
		if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
			p.next()
			alias = p.parseIdentLike()
			if alias == "" {
				return p.errf("expected alias")
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
	return nil
}

func (p *Parser) parseFromClause(sel *Select) error {
	// FROM is now optional (like MSSQL)
	if p.cur.Typ != tKeyword || p.cur.Val != "FROM" {
		// No FROM clause - this is allowed for expressions like SELECT NOW(), SELECT 1+1, etc.
		return nil
	}

	p.next() // consume FROM keyword

	// Prüfe auf Subselect: (SELECT ...)
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		subSel, err := p.parseSelect()
		if err != nil {
			return err
		}
		if p.cur.Typ != tSymbol || p.cur.Val != ")" {
			return p.errf("expected ) after subselect in FROM")
		}
		p.next()
		alias := ""
		if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
			p.next()
			alias = p.parseIdentLike()
			if alias == "" {
				return p.errf("expected alias after AS for subselect")
			}
		} else if p.cur.Typ == tIdent {
			alias = p.cur.Val
			p.next()
		} else {
			return p.errf("expected alias for subselect in FROM")
		}
		sel.From = FromItem{Subquery: subSel, Alias: alias}
		return nil
	}
	// Normale Tabelle
	from := p.parseIdentLike()
	if from == "" {
		return p.errf("expected table")
	}
	alias := from
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias = p.parseIdentLike()
		if alias == "" {
			return p.errf("expected alias")
		}
	} else if p.cur.Typ == tIdent {
		alias = p.cur.Val
		p.next()
	}
	sel.From = FromItem{Table: from, Alias: alias}
	return nil
}

func (p *Parser) parseJoinClauses(sel *Select) error {
	for {
		if p.cur.Typ == tKeyword && p.cur.Val == "JOIN" {
			p.next()
			rtbl, ralias, on, err := p.parseJoinTail()
			if err != nil {
				return err
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
				return err
			}
			rtbl, ralias, on, err := p.parseJoinTail()
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: jt, Right: FromItem{Table: rtbl, Alias: ralias}, On: on})
			continue
		}
		break
	}
	return nil
}

func (p *Parser) parseWhereClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "WHERE" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return err
		}
		sel.Where = e
	}
	return nil
}

func (p *Parser) parseGroupByClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "GROUP" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return err
			}
			sel.GroupBy = append(sel.GroupBy, expr)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}
	return nil
}

func (p *Parser) parseHavingClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "HAVING" {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return err
		}
		sel.Having = e
	}
	return nil
}

func (p *Parser) parseOrderByClause(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "ORDER" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		for {
			col := p.parseIdentLike()
			if col == "" {
				return p.errf("ORDER BY expects column")
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
	return nil
}

func (p *Parser) parseLimitOffset(sel *Select) error {
	if p.cur.Typ == tKeyword && p.cur.Val == "LIMIT" {
		p.next()
		if n := p.parseIntLiteral(); n != nil {
			sel.Limit = n
		} else {
			return p.errf("LIMIT expects integer")
		}
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		if n := p.parseIntLiteral(); n != nil {
			sel.Offset = n
		} else {
			return p.errf("OFFSET expects integer")
		}
	}
	return nil
}

func (p *Parser) parseUnionClause(sel *Select) error {
	for p.cur.Typ == tKeyword && (p.cur.Val == "UNION" || p.cur.Val == "EXCEPT" || p.cur.Val == "INTERSECT") {
		unionType := UnionDistinct
		switch p.cur.Val {
		case "UNION":
			p.next()
			if p.cur.Typ == tKeyword && p.cur.Val == "ALL" {
				unionType = UnionAll
				p.next()
			}
		case "EXCEPT":
			unionType = Except
			p.next()
		case "INTERSECT":
			unionType = Intersect
			p.next()
		}

		// Parse the right-hand SELECT
		rightSelect, err := p.parseSelect()
		if err != nil {
			return err
		}

		// Create the union clause
		unionClause := &UnionClause{
			Type:  unionType,
			Right: rightSelect,
		}

		// Find the end of the union chain and append
		if sel.Union == nil {
			sel.Union = unionClause
		} else {
			current := sel.Union
			for current.Next != nil {
				current = current.Next
			}
			current.Next = unionClause
		}
	}
	return nil
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
	cols := make([]storage.Column, 0, 8) // Pre-allocate for typical table
	for {
		col, err := p.parseSingleColumnDef()
		if err != nil {
			return nil, err
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

func (p *Parser) parseSingleColumnDef() (storage.Column, error) {
	name := p.parseIdentLike()
	if name == "" {
		return storage.Column{}, p.errf("expected column name")
	}
	typ := p.parseType()
	if typ < 0 {
		return storage.Column{}, p.errf("unknown type for column %q", name)
	}

	col := storage.Column{
		Name:       name,
		Type:       typ,
		Constraint: storage.NoConstraint,
	}

	// Parse constraints
	err := p.parseColumnConstraints(&col)
	if err != nil {
		return storage.Column{}, err
	}

	return col, nil
}

func (p *Parser) parseColumnConstraints(col *storage.Column) error {
	if p.cur.Typ != tKeyword {
		return nil
	}

	switch p.cur.Val {
	case "PRIMARY":
		return p.parsePrimaryKeyConstraint(col)
	case "FOREIGN":
		return p.parseForeignKeyConstraint(col)
	case "UNIQUE":
		return p.parseUniqueConstraint(col)
	case "REFERENCES":
		return p.parseReferencesConstraint(col)
	}
	return nil
}

func (p *Parser) parsePrimaryKeyConstraint(col *storage.Column) error {
	p.next()
	if p.cur.Typ == tKeyword && p.cur.Val == "KEY" {
		p.next()
		col.Constraint = storage.PrimaryKey
	}
	return nil
}

func (p *Parser) parseForeignKeyConstraint(col *storage.Column) error {
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
	return nil
}

func (p *Parser) parseUniqueConstraint(col *storage.Column) error {
	p.next()
	col.Constraint = storage.Unique
	return nil
}

func (p *Parser) parseReferencesConstraint(col *storage.Column) error {
	// Handle table-level REFERENCES for POINTER type
	if col.Type == storage.PointerType {
		p.next()
		table := p.parseIdentLike()
		if table != "" {
			col.PointerTable = table
		}
	}
	return nil
}

var typeKeywordMap = map[string]storage.ColType{
	// Integer types
	"INT":    storage.IntType,
	"INT8":   storage.Int8Type,
	"INT16":  storage.Int16Type,
	"INT32":  storage.Int32Type,
	"INT64":  storage.Int64Type,
	"UINT":   storage.UintType,
	"UINT8":  storage.Uint8Type,
	"UINT16": storage.Uint16Type,
	"UINT32": storage.Uint32Type,
	"UINT64": storage.Uint64Type,
	// Floating point types
	"FLOAT":   storage.Float64Type,
	"FLOAT64": storage.Float64Type,
	"DOUBLE":  storage.Float64Type,
	"FLOAT32": storage.Float32Type,
	// String and character types
	"STRING": storage.StringType,
	"TEXT":   storage.TextType,
	"RUNE":   storage.RuneType,
	"BYTE":   storage.ByteType,
	// Boolean type
	"BOOL":    storage.BoolType,
	"BOOLEAN": storage.BoolType,
	// Time types
	"TIME":      storage.TimeType,
	"DATE":      storage.DateType,
	"DATETIME":  storage.DateTimeType,
	"TIMESTAMP": storage.TimestampType,
	"DURATION":  storage.DurationType,
	// Complex data types
	"JSON":  storage.JsonType,
	"JSONB": storage.JsonbType,
	"MAP":   storage.MapType,
	"SLICE": storage.SliceType,
	"ARRAY": storage.SliceType,
	// Advanced types
	"COMPLEX64":  storage.Complex64Type,
	"COMPLEX128": storage.Complex128Type,
	"COMPLEX":    storage.Complex128Type,
	"POINTER":    storage.PointerType,
	"PTR":        storage.PointerType,
	"INTERFACE":  storage.InterfaceType,
}

func (p *Parser) parseType() storage.ColType {
	if p.cur.Typ == tKeyword {
		if colType, ok := typeKeywordMap[p.cur.Val]; ok {
			p.next()
			return colType
		}
	}
	return -1
}
func (p *Parser) parseIdentLike() string {
	// Accept both identifiers and keywords as identifier-like names.
	// This allows column/table names like "timestamp" even if they are keywords.
	if p.cur.Typ == tIdent || p.cur.Typ == tKeyword {
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

//nolint:gocyclo // parseCmp handles many comparison operator permutations.
func (p *Parser) parseCmp() (Expr, error) {
	l, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	for {
		// Check for NOT IN or NOT LIKE
		negate := false
		if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
			// Peek ahead to see if it's IN or LIKE
			if p.peek.Typ == tKeyword && (p.peek.Val == "IN" || p.peek.Val == "LIKE") {
				negate = true
				p.next() // Consume NOT
				// Continue with IN/LIKE parsing below
			} else {
				// Not IN/LIKE, stop here
				break
			}
		}

		// Check for IN operator
		if p.cur.Typ == tKeyword && p.cur.Val == "IN" {
			p.next()
			if err := p.expectSymbol("("); err != nil {
				return nil, err
			}
			var values []Expr
			for {
				val, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				values = append(values, val)
				if p.cur.Typ == tSymbol && p.cur.Val == "," {
					p.next()
					continue
				}
				if err := p.expectSymbol(")"); err != nil {
					return nil, err
				}
				break
			}
			l = &InExpr{Expr: l, Values: values, Negate: negate}
			continue
		}

		// Check for LIKE operator
		if p.cur.Typ == tKeyword && p.cur.Val == "LIKE" {
			p.next()
			pattern, err := p.parseAddSub()
			if err != nil {
				return nil, err
			}
			var escape Expr
			if p.cur.Typ == tKeyword && p.cur.Val == "ESCAPE" {
				p.next()
				escape, err = p.parseAddSub()
				if err != nil {
					return nil, err
				}
			}
			l = &LikeExpr{Expr: l, Pattern: pattern, Escape: escape, Negate: negate}
			continue
		}

		// Regular comparison operators
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

//nolint:gocyclo // Primary expression parsing covers numerous literal and sub-expression forms.
func (p *Parser) parsePrimary() (Expr, error) {
	switch p.cur.Typ {
	case tNumber:
		val := p.cur.Val
		p.next()
		// Try int first (most common), fall back to float
		if n, err := strconv.Atoi(val); err == nil {
			return &Literal{Val: n}, nil
		}
		f, _ := strconv.ParseFloat(val, 64)
		return &Literal{Val: f}, nil
	case tString:
		s := p.cur.Val
		p.next()
		return &Literal{Val: s}, nil
	case tKeyword:
		switch p.cur.Val {
		case "CASE":
			return p.parseCaseExpr()
		case "SELECT":
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			return &SubqueryExpr{Select: sel}, nil
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "COALESCE", "NVL", "IFNULL", "NULLIF",
			"JSON_GET", "JSON_SET", "JSON_EXTRACT",
			"NOW", "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP", "GETDATE", "TODAY",
			"DATEDIFF", "FROM_TIMESTAMP", "TIMESTAMP",
			"LTRIM", "RTRIM", "TRIM", "ISNULL",
			"BASE64", "BASE64_DECODE",
			"UPPER", "LOWER", "CONCAT", "CONCAT_WS", "LENGTH", "SUBSTRING", "SUBSTR",
			"LEFT", "RIGHT",
			"MD5", "SHA1", "SHA256", "SHA512",
			"CAST",
			"REPLACE", "INSTR", "LOCATE", "POSITION", "REVERSE", "REPEAT", "PRINTF", "FORMAT",
			"CHAR_LENGTH", "LPAD", "RPAD",
			"ABS", "ROUND", "FLOOR", "CEIL", "CEILING",
			"MOD", "POWER", "POW", "SQRT", "LOG", "LN", "LOG10", "LOG2", "EXP",
			"SIGN", "TRUNCATE", "TRUNC", "PI",
			"SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2",
			"DEGREES", "RADIANS",
			"GREATEST", "LEAST", "IF", "IIF",
			"STRFTIME", "DATE", "TIME", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
			"DAYOFWEEK", "DAYOFYEAR", "WEEKOFYEAR", "QUARTER",
			"DATE_ADD", "DATE_SUB", "DATEADD", "DATESUB",
			"RANDOM", "RAND",
			"SPACE", "ASCII", "CHAR", "CHR", "INITCAP", "SPLIT_PART", "SOUNDEX",
			"QUOTE", "HEX", "UNHEX",
			"UUID", "TYPEOF", "VERSION",
			"IN_PERIOD", "EXTRACT", "DATE_TRUNC", "EOMONTH", "ADD_MONTHS",
			"REGEXP_MATCH", "REGEXP_EXTRACT", "REGEXP_REPLACE",
			"SPLIT", "FIRST", "LAST", "ARRAY_LENGTH", "ARRAY_CONTAINS", "IN_ARRAY",
			"ARRAY_JOIN", "ARRAY_DISTINCT", "ARRAY_SORT",
			"ROW_NUMBER", "LAG", "LEAD", "MOVING_SUM", "MOVING_AVG",
			"MIN_BY", "MAX_BY", "ARG_MIN", "ARG_MAX", "FIRST_VALUE", "LAST_VALUE":
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
		// Check if it's a function call
		if p.cur.Typ == tSymbol && p.cur.Val == "(" {
			// This is a function call with an identifier
			// Put the current position back and parse as function
			return p.parseFuncCallWithName(name)
		}
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

//nolint:gocyclo // CASE parsing naturally involves multiple WHEN/ELSE branches.
func (p *Parser) parseCaseExpr() (Expr, error) {
	p.next() // consume CASE
	var operand Expr
	var err error
	if !(p.cur.Typ == tKeyword && p.cur.Val == "WHEN") {
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
		if !(p.cur.Typ == tKeyword && p.cur.Val == "WHEN") {
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
func (p *Parser) parseFuncCall() (Expr, error) {
	name := p.cur.Val
	p.next()
	return p.parseFuncCallWithName(name)
}

//nolint:gocyclo // Function-call grammar involves numerous special cases.
func (p *Parser) parseFuncCallWithName(name string) (Expr, error) {
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
		return &FuncCall{Name: name, Args: []Expr{expr, &Literal{Val: typeName}}}, nil
	}

	// Handle COUNT(*)
	if name == "COUNT" && p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		return &FuncCall{Name: name, Star: true}, nil
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

	return &FuncCall{Name: name, Args: args, Distinct: distinct, Over: overClause}, nil
}

// parseOverClause parses the OVER (PARTITION BY ... ORDER BY ... frame) clause
func (p *Parser) parseOverClause() (*OverClause, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}

	oc := &OverClause{}

	// Parse PARTITION BY clause (optional)
	if p.cur.Typ == tKeyword && p.cur.Val == "PARTITION" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}

		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			oc.PartitionBy = append(oc.PartitionBy, expr)

			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}

	// Parse ORDER BY clause (optional)
	if p.cur.Typ == tKeyword && p.cur.Val == "ORDER" {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}

		for {
			if p.cur.Typ != tIdent && p.cur.Typ != tKeyword {
				return nil, p.errf("expected column name in ORDER BY")
			}
			col := p.cur.Val
			p.next()

			desc := false
			if p.cur.Typ == tKeyword && (p.cur.Val == "DESC" || p.cur.Val == "ASC") {
				if p.cur.Val == "DESC" {
					desc = true
				}
				p.next()
			}

			oc.OrderBy = append(oc.OrderBy, OrderItem{Col: col, Desc: desc})

			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}

	// Parse frame clause (ROWS/RANGE BETWEEN ... AND ...) - optional
	if p.cur.Typ == tKeyword && (p.cur.Val == "ROWS" || p.cur.Val == "RANGE") {
		frame, err := p.parseWindowFrame()
		if err != nil {
			return nil, err
		}
		oc.Frame = frame
	}

	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}

	return oc, nil
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

		// Parse the value as integer
		var n int
		if _, err := fmt.Sscanf(value, "%d", &n); err != nil {
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
