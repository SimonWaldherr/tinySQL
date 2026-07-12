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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Parser holds the lexer and current/peek tokens for recursive-descent parsing.
type Parser struct {
	lx   *lexer
	cur  token
	peek token
	// depth tracks combined expression/subquery recursion nesting. Without
	// a limit, a maliciously deep input like "((((((...1...))))))" or
	// nested "(SELECT * FROM (SELECT * FROM (SELECT ...)))" recurses once
	// per nesting level through the whole precedence chain — enough levels
	// exhausts the goroutine stack. That failure mode is a Go runtime fatal
	// error ("stack overflow"), not a normal panic: it cannot be caught by
	// recover(), so unlike other engine bugs it would kill the whole
	// process outright, not just fail the one query. maxParseDepth keeps
	// it a plain, recoverable parse error instead.
	depth int
}

// maxParseDepth bounds parseExpr/parseSelect recursion. 200 comfortably
// covers any realistic hand-written or generated query while stopping
// pathological nesting well before it threatens the goroutine stack.
const maxParseDepth = 200

func (p *Parser) enterRecursion() error {
	p.depth++
	if p.depth > maxParseDepth {
		return p.errf("expression or subquery nested too deeply (limit %d)", maxParseDepth)
	}
	return nil
}

func (p *Parser) exitRecursion() {
	p.depth--
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

func (p *Parser) parseBareTableSelect() (*Select, error) {
	table := p.parseQualifiedIdentLike()
	if table == "" {
		return nil, p.errf("expected table name")
	}
	if p.cur.Typ == tSymbol && p.cur.Val == ";" {
		p.next()
	}
	if p.cur.Typ != tEOF {
		return nil, p.errf("unexpected token after table name")
	}
	return &Select{
		From:  FromItem{Table: table, Alias: table},
		Projs: []SelectItem{{Star: true}},
	}, nil
}

func newVarRef(name string) *VarRef {
	return &VarRef{Name: name, Lower: strings.ToLower(name)}
}

// ------------------------------ AST ------------------------------

type Expr interface{}

type (
	// VarRef refers to a column (qualified or unqualified) in expressions.
	VarRef struct {
		Name  string
		Lower string
	}
	// Literal holds a constant value (number, string, bool, NULL).
	Literal struct {
		Val       any
		Parameter bool // bound positional parameter; value may change between executions
	}
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
		Expr            Expr
		Pattern         Expr
		Escape          Expr // Optional ESCAPE character
		Negate          bool // For NOT LIKE / NOT ILIKE / NOT GLOB
		CaseInsensitive bool // For ILIKE
		GlobStyle       bool // For GLOB (* and ? wildcards instead of % and _)
	}
	// RegexpExpr represents "expr REGEXP/RLIKE pattern" and "expr SIMILAR TO pattern".
	RegexpExpr struct {
		Expr      Expr
		Pattern   Expr
		Negate    bool // For NOT REGEXP / NOT RLIKE / NOT SIMILAR TO
		SimilarTo bool // Pattern uses SQL SIMILAR TO syntax (% and _ wildcards)
	}
	// BetweenExpr represents "expr [NOT] BETWEEN lo AND hi" when expr is not a
	// plain column or literal. Unlike the desugared form
	// (expr >= lo AND expr <= hi) it evaluates expr exactly once, which is
	// faster for expensive expressions and correct for non-deterministic ones
	// (e.g. RANDOM() BETWEEN 1 AND 10).
	BetweenExpr struct {
		Expr   Expr
		Lo     Expr
		Hi     Expr
		Negate bool
	}
	// ExistsExpr represents "EXISTS (subquery)".
	ExistsExpr struct {
		Select *Select
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

// CallProcedure represents CALL proc_name(arg1, arg2, ...).
type CallProcedure struct {
	Name string
	Args []Expr
}

// Explain represents an EXPLAIN statement around another statement.
type Explain struct {
	Statement Statement
	Analyze   bool
}

// Pragma represents a SQLite-compatible PRAGMA statement.
type Pragma struct {
	Name   string
	Schema string
	Args   []string
	Value  *string
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	Name         string
	Cols         []storage.Column
	IsTemp       bool
	AsSelect     *Select
	IfNotExists  bool     // IF NOT EXISTS clause
	VirtualTable bool     // CREATE VIRTUAL TABLE
	Using        string   // e.g. "fts"
	FTSColumns   []string // columns passed to fts(...)
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
	SQLText     string
	IfNotExists bool
	OrReplace   bool
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Name     string
	IfExists bool
}

// CreateMaterializedView represents a CREATE MATERIALIZED VIEW statement.
type CreateMaterializedView struct {
	Name               string
	Select             *Select
	SQLText            string
	IfNotExists        bool
	OrReplace          bool
	WithData           bool
	StaleAfterMs       int64
	RefreshEveryMs     int64
	DailyAt            string
	Timezone           string
	InvalidateOnChange bool
}

// DropMaterializedView represents a DROP MATERIALIZED VIEW statement.
type DropMaterializedView struct {
	Name     string
	IfExists bool
}

// RefreshMaterializedView represents REFRESH MATERIALIZED VIEW.
type RefreshMaterializedView struct {
	Name         string
	Concurrently bool
}

// AlterViewMaterialize represents ALTER VIEW ... MATERIALIZE.
type AlterViewMaterialize struct {
	Name               string
	WithData           bool
	StaleAfterMs       int64
	RefreshEveryMs     int64
	DailyAt            string
	Timezone           string
	InvalidateOnChange bool
}

// AlterMaterializedViewToView represents ALTER MATERIALIZED VIEW ... TO VIEW.
type AlterMaterializedViewToView struct {
	Name string
}

// CreateJob represents a CREATE JOB statement.
type CreateJob struct {
	Name         string
	ScheduleType string // CRON, INTERVAL, ONCE
	CronExpr     string
	IntervalMs   int64
	RunAt        *time.Time
	Timezone     string
	MaxRuntimeMs int64
	NoOverlap    bool
	CatchUp      bool
	Enabled      bool
	SQLText      string
}

// AlterJob represents ALTER JOB ... ENABLE/DISABLE
type AlterJob struct {
	Name   string
	Enable *bool // nil means no-op
}

// DropJob represents DROP JOB <name>
type DropJob struct {
	Name string
}

// CreateTrigger represents a CREATE TRIGGER statement.
type CreateTrigger struct {
	Name        string
	Timing      string // "BEFORE", "AFTER", "INSTEAD OF"
	Event       string // "INSERT", "UPDATE", "DELETE"
	Table       string
	ForEachRow  bool
	WhenExpr    Expr        // optional WHEN condition
	WhenText    string      // original WHEN expression text, for persisted triggers
	Body        []Statement // trigger body statements, parsed once to validate syntax at CREATE TRIGGER time
	BodyText    string      // verbatim source text of the body (between BEGIN and END), stored for re-parsing on each fire
	IfNotExists bool
}

// DropTrigger represents a DROP TRIGGER statement.
type DropTrigger struct {
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
	Table     string
	Cols      []string
	Rows      [][]Expr
	Returning []SelectItem
}

// Update represents an UPDATE statement.
type Update struct {
	Table     string
	Sets      map[string]Expr
	Where     Expr
	Returning []SelectItem
}

// Delete represents a DELETE statement.
type Delete struct {
	Table     string
	Where     Expr
	Returning []SelectItem
}

type JoinType int

const (
	// JoinInner represents INNER JOIN.
	JoinInner JoinType = iota
	// JoinLeft represents LEFT (OUTER) JOIN.
	JoinLeft
	// JoinRight represents RIGHT (OUTER) JOIN.
	JoinRight
	// JoinFull represents FULL (OUTER) JOIN.
	JoinFull
	// JoinCross represents CROSS JOIN (unconditional Cartesian product; no ON clause).
	JoinCross
)

// String returns the SQL keyword form of the join type, e.g. "LEFT JOIN".
func (t JoinType) String() string {
	switch t {
	case JoinLeft:
		return "LEFT JOIN"
	case JoinRight:
		return "RIGHT JOIN"
	case JoinFull:
		return "FULL OUTER JOIN"
	case JoinCross:
		return "CROSS JOIN"
	case JoinInner:
		return "JOIN"
	default:
		return "JOIN"
	}
}

// Select represents a SELECT query and its clauses.
type Select struct {
	Distinct   bool
	DistinctOn []Expr
	From       FromItem
	Joins      []JoinClause
	Projs      []SelectItem
	Where      Expr
	Pivot      *PivotClause
	GroupBy    []Expr
	Having     Expr
	OrderBy    []OrderItem
	Limit      *int
	Offset     *int
	Union      *UnionClause // For UNION operations
	CTEs       []CTE        // Common Table Expressions
	// simplePlanCache is initialized by the parser and stores only immutable
	// plan shape. Parameter values and index RowIDs are rebound for every run.
	simplePlanCache *simpleSelectPlanCache
}

// PivotClause represents "PIVOT (agg(value_expr) FOR pivot_col IN (v1 [AS a1], v2 [AS a2], ...))".
// It reshapes the WHERE-filtered row set: each distinct value listed in the
// IN-list becomes its own output column (named by its alias, or by the
// value's literal text if no alias is given), holding agg(value_expr) over
// the rows where pivot_col equals that value. Every other selected column
// acts as an implicit GROUP BY key, matching standard SQL PIVOT semantics.
//
// Scope: a single aggregate function and a static (literal) value list —
// no dynamic pivot driven by a subquery. This covers the overwhelmingly
// common case (a known, fixed set of categories to spread into columns)
// without the complexity of a fully dynamic PIVOT.
type PivotClause struct {
	AggFunc   string // e.g. SUM, COUNT, AVG, MIN, MAX
	ValueExpr Expr
	PivotCol  string
	Values    []PivotValue
}

// PivotValue is one entry in a PIVOT's IN (...) list.
type PivotValue struct {
	Expr  Expr
	Alias string // output column name; defaults to the value's text form
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

// String returns the SQL keyword form of the union type, e.g. "UNION ALL".
func (t UnionType) String() string {
	switch t {
	case UnionAll:
		return "UNION ALL"
	case Except:
		return "EXCEPT"
	case Intersect:
		return "INTERSECT"
	case UnionDistinct:
		return "UNION"
	default:
		return "UNION"
	}
}

// UnionClause represents a set operation chaining RIGHT select with current one.
type UnionClause struct {
	Type  UnionType
	Right *Select
	Next  *UnionClause // For chaining multiple UNIONs
}

// FromItem kann eine echte Tabelle oder ein Subselect (Derived Table) sein.
type FromItem struct {
	Table     string         // Tabellenname (wenn echte Tabelle)
	Alias     string         // Alias für Tabelle oder Subselect
	Subquery  *Select        // Falls abgeleitete Tabelle: das Select-Statement
	TableFunc *TableFuncCall // Wenn FROM eine table-valued function ist
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
	if p.cur.Typ == tIdent {
		return p.parseBareTableSelect()
	}
	if p.cur.Typ != tKeyword {
		return nil, p.errf("expected a statement")
	}

	switch p.cur.Val {
	case "EXPLAIN":
		return p.parseExplain()
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
	case "SELECT", "WITH":
		return p.parseSelectWithCTE()
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
	if p.cur.Typ == tSymbol && p.cur.Val == ";" {
		p.next()
	}
	if p.cur.Typ != tEOF {
		return nil, p.errf("unexpected token after CALL")
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
	stmt, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}
	return &Explain{Statement: stmt, Analyze: analyze}, nil
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
	if p.cur.Typ == tSymbol && p.cur.Val == ";" {
		p.next()
	}
	if p.cur.Typ != tEOF {
		return nil, p.errf("unexpected token after PRAGMA")
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

func (p *Parser) parseCreate() (Statement, error) {
	p.next()

	stmt, handled, err := p.parseCreateNonTable()
	if err != nil || handled {
		return stmt, err
	}

	return p.parseCreateTable()
}

func (p *Parser) parseCreateNonTable() (Statement, bool, error) {
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "JOB" {
		stmt, err := p.parseCreateJob()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && (p.cur.Val == "USER" || p.cur.Val == "ROLE") {
		stmt, err := p.parseCreateUserOrRole()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "TRIGGER" {
		stmt, err := p.parseCreateTrigger(false)
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OR" {
		stmt, err := p.parseCreateOrReplace()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
		stmt, err := p.parseCreateMaterializedView(false)
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && (p.cur.Val == "INDEX" || p.cur.Val == "UNIQUE") {
		stmt, err := p.parseCreateIndex()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		stmt, err := p.parseCreateView()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "VIRTUAL" {
		stmt, err := p.parseCreateVirtualTable()
		return stmt, true, err
	}
	return nil, false, nil
}

func (p *Parser) parseCreateOrReplace() (Statement, error) {
	p.next() // consume OR
	if p.cur.Typ == tKeyword && p.cur.Val == "REPLACE" {
		p.next() // consume REPLACE
		if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
			return p.parseCreateMaterializedView(true)
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "TRIGGER" {
			return p.parseCreateTrigger(false)
		}
		return p.parseCreateView()
	}
	return p.parseCreateView()
}

func (p *Parser) parseCreateVirtualTable() (Statement, error) {
	p.next()
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	return p.parseVirtualTable()
}

func (p *Parser) parseCreateTable() (Statement, error) {
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

	// Check for DROP MATERIALIZED VIEW
	if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
		return p.parseDropMaterializedView()
	}

	// Check for DROP INDEX
	if p.cur.Typ == tKeyword && p.cur.Val == "INDEX" {
		return p.parseDropIndex()
	}

	// Check for DROP VIEW
	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		return p.parseDropView()
	}

	// Check for DROP TRIGGER
	if p.cur.Typ == tKeyword && p.cur.Val == "TRIGGER" {
		return p.parseDropTrigger()
	}

	// Check for DROP JOB
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "JOB" {
		p.next()
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected job name")
		}
		return &DropJob{Name: name}, nil
	}

	// Check for DROP USER / DROP ROLE
	if p.cur.Typ == tKeyword && (p.cur.Val == "USER" || p.cur.Val == "ROLE") {
		return p.parseDropUserOrRole()
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

// parseCreateTrigger parses CREATE [OR REPLACE] TRIGGER ...
// Syntax: CREATE TRIGGER [IF NOT EXISTS] name BEFORE|AFTER|INSTEAD OF INSERT|UPDATE|DELETE
//
//	ON table [FOR EACH ROW] [WHEN (expr)] BEGIN stmt; ... END
func (p *Parser) parseCreateTrigger(orReplace bool) (Statement, error) {
	p.next() // consume TRIGGER

	ifNotExists, err := p.parseTriggerIfNotExists()
	if err != nil {
		return nil, err
	}

	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected trigger name")
	}

	timing, err := p.parseTriggerTiming()
	if err != nil {
		return nil, err
	}

	event, err := p.parseTriggerEvent()
	if err != nil {
		return nil, err
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	table := p.parseIdentLike()
	if table == "" {
		return nil, p.errf("expected table name in trigger")
	}

	forEachRow, err := p.parseTriggerForEachRow()
	if err != nil {
		return nil, err
	}

	whenExpr, whenText, err := p.parseTriggerWhen()
	if err != nil {
		return nil, err
	}

	if err := p.expectKeyword("BEGIN"); err != nil {
		return nil, err
	}

	body, bodyText, err := p.parseTriggerBody()
	if err != nil {
		return nil, err
	}

	return &CreateTrigger{
		Name:        name,
		Timing:      timing,
		Event:       event,
		Table:       table,
		ForEachRow:  forEachRow,
		WhenExpr:    whenExpr,
		WhenText:    whenText,
		Body:        body,
		BodyText:    bodyText,
		IfNotExists: ifNotExists,
	}, nil
}

func (p *Parser) parseTriggerIfNotExists() (bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "IF" {
		return false, nil
	}
	p.next()
	if err := p.expectKeyword("NOT"); err != nil {
		return false, err
	}
	if err := p.expectKeyword("EXISTS"); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Parser) parseTriggerTiming() (string, error) {
	if p.cur.Typ != tKeyword {
		return "", p.errf("expected BEFORE, AFTER, or INSTEAD OF in trigger")
	}
	switch p.cur.Val {
	case "BEFORE":
		p.next()
		return "BEFORE", nil
	case "AFTER":
		p.next()
		return "AFTER", nil
	case "INSTEAD":
		p.next()
		if err := p.expectKeyword("OF"); err != nil {
			return "", err
		}
		return "INSTEAD OF", nil
	default:
		return "", p.errf("expected BEFORE, AFTER, or INSTEAD OF in trigger")
	}
}

func (p *Parser) parseTriggerEvent() (string, error) {
	if p.cur.Typ != tKeyword {
		return "", p.errf("expected INSERT, UPDATE, or DELETE in trigger")
	}
	switch p.cur.Val {
	case "INSERT", "UPDATE", "DELETE":
		event := p.cur.Val
		p.next()
		return event, nil
	default:
		return "", p.errf("expected INSERT, UPDATE, or DELETE in trigger")
	}
}

func (p *Parser) parseTriggerForEachRow() (bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "FOR" {
		return false, nil
	}
	p.next()
	if err := p.expectKeyword("EACH"); err != nil {
		return false, err
	}
	if err := p.expectKeyword("ROW"); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Parser) parseTriggerWhen() (Expr, string, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "WHEN" {
		return nil, "", nil
	}
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return nil, "", err
	}
	startPos := p.cur.Pos
	whenExpr, err := p.parseExpr()
	if err != nil {
		return nil, "", err
	}
	endPos := p.cur.Pos
	if err := p.expectSymbol(")"); err != nil {
		return nil, "", err
	}
	return whenExpr, strings.TrimSpace(p.lx.s[startPos:endPos]), nil
}

// parseTriggerBody parses the statements between BEGIN and END, validating
// their syntax immediately (so a malformed trigger body fails at CREATE
// TRIGGER time rather than silently on every future fire). It also returns
// the verbatim source text of the body, sliced out of the original SQL by
// byte offset — this, not a reprint of the parsed AST, is what gets stored
// for re-parsing each time the trigger fires. An AST-to-SQL printer would
// need to precisely reconstruct every expression (including qualified names
// like NEW.col); capturing the original text sidesteps that entirely and
// can't drift from what the user actually wrote.
func (p *Parser) parseTriggerBody() ([]Statement, string, error) {
	startPos := p.cur.Pos
	var body []Statement
	for !(p.cur.Typ == tKeyword && p.cur.Val == "END") && p.cur.Typ != tEOF {
		stmt, err := p.ParseStatement()
		if err != nil {
			return nil, "", fmt.Errorf("trigger body: %w", err)
		}
		body = append(body, stmt)
		if p.cur.Typ == tSymbol && p.cur.Val == ";" {
			p.next()
		}
	}
	endPos := p.cur.Pos
	bodyText := strings.TrimSpace(p.lx.s[startPos:endPos])
	if p.cur.Typ == tKeyword && p.cur.Val == "END" {
		p.next()
	}
	return body, bodyText, nil
}

// parseDropTrigger parses DROP TRIGGER [IF EXISTS] name.
func (p *Parser) parseDropTrigger() (Statement, error) {
	p.next() // consume TRIGGER

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
		return nil, p.errf("expected trigger name")
	}
	return &DropTrigger{Name: name, IfExists: ifExists}, nil
}

// parseVirtualTable parses CREATE VIRTUAL TABLE name USING fts(col1, col2, ...).
func (p *Parser) parseVirtualTable() (Statement, error) {
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

	if err := p.expectKeyword("USING"); err != nil {
		return nil, err
	}

	engine := p.parseIdentLike()
	if engine == "" {
		return nil, p.errf("expected virtual table engine name (e.g. fts)")
	}

	var ftsCols []string
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		for p.cur.Typ != tSymbol || p.cur.Val != ")" {
			col := p.parseIdentLike()
			if col == "" {
				return nil, p.errf("expected column name in USING clause")
			}
			ftsCols = append(ftsCols, col)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
			}
		}
		p.next() // consume )
	}

	return &CreateTable{
		Name:         name,
		VirtualTable: true,
		Using:        strings.ToLower(engine),
		FTSColumns:   ftsCols,
		IfNotExists:  ifNotExists,
	}, nil
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
	} else if err := p.expectKeyword("INDEX"); err != nil {
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

// parseJobSchedule parses the SCHEDULE clause for CREATE JOB
func (p *Parser) parseJobSchedule(job *CreateJob) error {
	p.next() // consume SCHEDULE

	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "CRON" {
		return p.parseJobScheduleCron(job)
	}
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "INTERVAL" {
		return p.parseJobScheduleInterval(job)
	}
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "ONCE" {
		return p.parseJobScheduleOnce(job)
	}
	return p.errf("expected CRON|INTERVAL|ONCE after SCHEDULE")
}

// parseJobScheduleCron parses SCHEDULE CRON clause
func (p *Parser) parseJobScheduleCron(job *CreateJob) error {
	p.next() // consume CRON
	if p.cur.Typ != tString {
		return p.errf("expected CRON string")
	}
	job.ScheduleType = "CRON"
	job.CronExpr = p.cur.Val
	p.next()
	return nil
}

// parseJobScheduleInterval parses SCHEDULE INTERVAL clause
func (p *Parser) parseJobScheduleInterval(job *CreateJob) error {
	p.next() // consume INTERVAL
	if p.cur.Typ != tNumber {
		return p.errf("expected INTERVAL milliseconds number")
	}
	n, _ := strconv.ParseInt(p.cur.Val, 10, 64)
	job.ScheduleType = "INTERVAL"
	job.IntervalMs = n
	p.next()
	return nil
}

// parseJobScheduleOnce parses SCHEDULE ONCE clause
func (p *Parser) parseJobScheduleOnce(job *CreateJob) error {
	p.next() // consume ONCE
	if p.cur.Typ != tString {
		return p.errf("expected ONCE timestamp string")
	}
	job.ScheduleType = "ONCE"
	// parse time in common layout
	if t, err := time.Parse("2006-01-02 15:04:05", p.cur.Val); err == nil {
		job.RunAt = &t
	}
	p.next()
	return nil
}

// parseJobSQLBody extracts the SQL body of a CREATE JOB statement
func (p *Parser) parseJobSQLBody() string {
	bodyStart := p.cur.Pos
	// Advance until semicolon or EOF
	for !(p.cur.Typ == tSymbol && p.cur.Val == ";") && p.cur.Typ != tEOF {
		p.next()
	}
	endPos := p.cur.Pos
	// Extract substring from lexer
	if bodyStart < endPos && endPos <= len(p.lx.s) {
		return p.lx.s[bodyStart:endPos]
	}
	return ""
}

// parseCreateJob handles CREATE JOB statements.
func (p *Parser) parseCreateJob() (Statement, error) {
	// cur is at JOB
	p.next() // consume JOB
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected job name")
	}

	job := &CreateJob{Name: name, Enabled: true}

	// Parse optional clauses until AS
	for p.cur.Typ == tKeyword || p.cur.Typ == tIdent {
		switch p.cur.Val {
		case "SCHEDULE":
			if err := p.parseJobSchedule(job); err != nil {
				return nil, err
			}
		case "TIMEZONE":
			p.next()
			if p.cur.Typ != tString {
				return nil, p.errf("expected timezone string")
			}
			job.Timezone = p.cur.Val
			p.next()
		case "MAX_RUNTIME":
			p.next()
			if p.cur.Typ != tNumber {
				return nil, p.errf("expected number for MAX_RUNTIME")
			}
			n, _ := strconv.ParseInt(p.cur.Val, 10, 64)
			job.MaxRuntimeMs = n
			p.next()
		case "NO_OVERLAP":
			job.NoOverlap = true
			p.next()
		case "CATCH_UP":
			job.CatchUp = true
			p.next()
		case "ENABLED":
			job.Enabled = true
			p.next()
		case "DISABLED":
			job.Enabled = false
			p.next()
		default:
			// stop when we hit AS or other token
			goto afterClauses
		}
	}
afterClauses:
	// If caller provided an explicit AS keyword, consume it; otherwise
	// be permissive and treat the following tokens as the job body.
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "AS" {
		p.next()
	}

	// Capture raw SQL text for the job body
	job.SQLText = p.parseJobSQLBody()

	// consume semicolon if present
	if p.cur.Typ == tSymbol && p.cur.Val == ";" {
		p.next()
	}
	return job, nil
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

	queryStart := p.cur.Pos
	sel, err := p.parseSelectWithCTE()
	if err != nil {
		return nil, err
	}
	sqlText := p.sqlFragment(queryStart, p.cur.Pos)

	return &CreateView{
		Name:        viewName,
		Select:      sel,
		SQLText:     sqlText,
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
	}, nil
}

func (p *Parser) parseCreateMaterializedView(orReplace bool) (Statement, error) {
	p.next() // consume MATERIALIZED
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

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
		return nil, p.errf("expected materialized view name")
	}
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	queryStart := p.cur.Pos
	sel, err := p.parseSelectWithCTE()
	if err != nil {
		return nil, err
	}
	mv := &CreateMaterializedView{
		Name:        viewName,
		Select:      sel,
		SQLText:     p.sqlFragment(queryStart, p.cur.Pos),
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
		WithData:    true,
	}

	for p.cur.Typ == tKeyword {
		switch p.cur.Val {
		case "REFRESH":
			if err := p.parseMaterializedRefreshClause(mv); err != nil {
				return nil, err
			}
		case "WITH":
			if err := p.parseMaterializedWithData(mv); err != nil {
				return nil, err
			}
		case "INVALIDATE":
			if err := p.parseMaterializedInvalidateClause(mv); err != nil {
				return nil, err
			}
		default:
			return mv, nil
		}
	}
	return mv, nil
}

func (p *Parser) parseMaterializedRefreshClause(mv *CreateMaterializedView) error {
	p.next() // consume REFRESH
	if p.cur.Typ == tKeyword && p.cur.Val == "ON" {
		p.next()
		if p.cur.Typ == tKeyword && p.cur.Val == "STALE" {
			p.next()
			if err := p.expectKeyword("AFTER"); err != nil {
				return err
			}
			ms, err := p.parseDurationMillis()
			if err != nil {
				return err
			}
			mv.StaleAfterMs = ms
			return nil
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "DEMAND" {
			p.next()
			return nil
		}
		return p.errf("expected STALE or DEMAND after REFRESH ON")
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "EVERY" {
		p.next()
		ms, err := p.parseDurationMillis()
		if err != nil {
			return err
		}
		mv.RefreshEveryMs = ms
		return nil
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "DAILY" {
		p.next()
		if err := p.expectKeyword("AT"); err != nil {
			return err
		}
		if p.cur.Typ != tString {
			return p.errf("expected daily refresh time string")
		}
		mv.DailyAt = p.cur.Val
		p.next()
		if p.cur.Typ == tKeyword && p.cur.Val == "TIMEZONE" {
			p.next()
			if p.cur.Typ != tString {
				return p.errf("expected timezone string")
			}
			mv.Timezone = p.cur.Val
			p.next()
		}
		return nil
	}
	return p.errf("expected ON, EVERY, or DAILY after REFRESH")
}

func (p *Parser) parseMaterializedWithData(mv *CreateMaterializedView) error {
	p.next() // consume WITH
	if p.cur.Typ == tKeyword && p.cur.Val == "NO" {
		p.next()
		if err := p.expectKeyword("DATA"); err != nil {
			return err
		}
		mv.WithData = false
		return nil
	}
	if err := p.expectKeyword("DATA"); err != nil {
		return err
	}
	mv.WithData = true
	return nil
}

func (p *Parser) parseMaterializedInvalidateClause(mv *CreateMaterializedView) error {
	p.next() // consume INVALIDATE
	if err := p.expectKeyword("ON"); err != nil {
		return err
	}
	if err := p.expectKeyword("CHANGE"); err != nil {
		return err
	}
	mv.InvalidateOnChange = true
	return nil
}

func (p *Parser) parseDurationMillis() (int64, error) {
	if p.cur.Typ != tNumber {
		return 0, p.errf("expected duration number")
	}
	n, err := strconv.ParseFloat(p.cur.Val, 64)
	if err != nil {
		return 0, p.errf("invalid duration number")
	}
	p.next()
	unit := p.parseIdentLike()
	if unit == "" {
		return 0, p.errf("expected duration unit")
	}
	switch strings.ToUpper(unit) {
	case "MILLISECOND", "MILLISECONDS", "MS":
		return int64(n), nil
	case "SECOND", "SECONDS":
		return int64(n * float64(time.Second/time.Millisecond)), nil
	case "MINUTE", "MINUTES":
		return int64(n * float64(time.Minute/time.Millisecond)), nil
	case "HOUR", "HOURS":
		return int64(n * float64(time.Hour/time.Millisecond)), nil
	case "DAY", "DAYS":
		return int64(n * float64((24*time.Hour)/time.Millisecond)), nil
	default:
		return 0, p.errf("unknown duration unit %q", unit)
	}
}

func (p *Parser) sqlFragment(start, end int) string {
	if start < 0 {
		start = 0
	}
	if end < start || end > len(p.lx.s) {
		end = len(p.lx.s)
	}
	return strings.TrimSpace(p.lx.s[start:end])
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

func (p *Parser) parseDropMaterializedView() (Statement, error) {
	p.next() // consume MATERIALIZED
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

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
		return nil, p.errf("expected materialized view name")
	}
	return &DropMaterializedView{Name: name, IfExists: ifExists}, nil
}

func (p *Parser) parseRefresh() (Statement, error) {
	p.next() // consume REFRESH
	if err := p.expectKeyword("MATERIALIZED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	concurrently := false
	if p.cur.Typ == tKeyword && p.cur.Val == "CONCURRENTLY" {
		concurrently = true
		p.next()
	}
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected materialized view name")
	}
	if p.cur.Typ == tSymbol && p.cur.Val == ";" {
		p.next()
	}
	return &RefreshMaterializedView{Name: name, Concurrently: concurrently}, nil
}

func (p *Parser) parseAlter() (Statement, error) {
	p.next()

	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		return p.parseAlterView()
	}

	if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
		return p.parseAlterMaterializedView()
	}

	if p.cur.Typ == tKeyword && p.cur.Val == "USER" {
		return p.parseAlterUser()
	}

	// Support ALTER JOB <name> ENABLE|DISABLE
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "JOB" {
		p.next()
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected job name")
		}
		if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && (p.cur.Val == "ENABLE" || p.cur.Val == "DISABLE") {
			enable := p.cur.Val == "ENABLE"
			p.next()
			return &AlterJob{Name: name, Enable: &enable}, nil
		}
		return nil, p.errf("expected ENABLE or DISABLE after JOB name")
	}

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

	colType, err := p.parseColumnType()
	if err != nil {
		return nil, p.errf("unknown column type")
	}

	col := storage.Column{
		Name:         colName,
		Type:         colType.typ,
		DeclaredType: colType.declared,
		Affinity:     colType.affinity,
	}

	return &AlterTable{
		Table:     tableName,
		AddColumn: &col,
	}, nil
}

func (p *Parser) parseAlterView() (Statement, error) {
	p.next() // consume VIEW
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected view name")
	}
	if (p.cur.Typ != tKeyword && p.cur.Typ != tIdent) || p.cur.Val != "MATERIALIZE" {
		return nil, p.errf("expected MATERIALIZE")
	}
	p.next()
	mv := &CreateMaterializedView{Name: name, WithData: true}
	for p.cur.Typ == tKeyword {
		switch p.cur.Val {
		case "REFRESH":
			if err := p.parseMaterializedRefreshClause(mv); err != nil {
				return nil, err
			}
		case "WITH":
			if err := p.parseMaterializedWithData(mv); err != nil {
				return nil, err
			}
		case "INVALIDATE":
			if err := p.parseMaterializedInvalidateClause(mv); err != nil {
				return nil, err
			}
		default:
			return &AlterViewMaterialize{
				Name:               name,
				WithData:           mv.WithData,
				StaleAfterMs:       mv.StaleAfterMs,
				RefreshEveryMs:     mv.RefreshEveryMs,
				DailyAt:            mv.DailyAt,
				Timezone:           mv.Timezone,
				InvalidateOnChange: mv.InvalidateOnChange,
			}, nil
		}
	}
	return &AlterViewMaterialize{
		Name:               name,
		WithData:           mv.WithData,
		StaleAfterMs:       mv.StaleAfterMs,
		RefreshEveryMs:     mv.RefreshEveryMs,
		DailyAt:            mv.DailyAt,
		Timezone:           mv.Timezone,
		InvalidateOnChange: mv.InvalidateOnChange,
	}, nil
}

func (p *Parser) parseAlterMaterializedView() (Statement, error) {
	p.next() // consume MATERIALIZED
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected materialized view name")
	}
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	return &AlterMaterializedViewToView{Name: name}, nil
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
	returning, err := p.parseReturningClause()
	if err != nil {
		return nil, err
	}
	return &Insert{Table: tname, Cols: cols, Rows: rows, Returning: returning}, nil
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
	if err := p.enterRecursion(); err != nil {
		return nil, err
	}
	defer p.exitRecursion()
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &Select{simplePlanCache: &simpleSelectPlanCache{}}

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

	// Parse PIVOT
	if err := p.parsePivotClause(sel); err != nil {
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

// parsePivotClause parses an optional
// "PIVOT (agg(expr) FOR col IN (v1 [AS a1], v2 [AS a2], ...))" clause.
func (p *Parser) parsePivotClause(sel *Select) error {
	if p.cur.Typ != tKeyword || p.cur.Val != "PIVOT" {
		return nil
	}
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return err
	}

	if p.cur.Typ != tIdent && p.cur.Typ != tKeyword {
		return p.errf("expected aggregate function name in PIVOT")
	}
	aggFunc := strings.ToUpper(p.cur.Val)
	p.next()

	if err := p.expectSymbol("("); err != nil {
		return err
	}
	valueExpr, err := p.parseExpr()
	if err != nil {
		return err
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}

	if err := p.expectKeyword("FOR"); err != nil {
		return err
	}
	pivotCol := p.parseIdentLike()
	if pivotCol == "" {
		return p.errf("expected column name after FOR in PIVOT")
	}

	if err := p.expectKeyword("IN"); err != nil {
		return err
	}
	if err := p.expectSymbol("("); err != nil {
		return err
	}
	var values []PivotValue
	for {
		ve, err := p.parseExpr()
		if err != nil {
			return err
		}
		alias := ""
		if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
			p.next()
			alias = p.parseIdentLike()
			if alias == "" {
				return p.errf("expected alias after AS in PIVOT value list")
			}
		}
		values = append(values, PivotValue{Expr: ve, Alias: alias})
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}
	if len(values) == 0 {
		return p.errf("PIVOT IN (...) requires at least one value")
	}

	if err := p.expectSymbol(")"); err != nil {
		return err
	}

	sel.Pivot = &PivotClause{
		AggFunc:   aggFunc,
		ValueExpr: valueExpr,
		PivotCol:  pivotCol,
		Values:    values,
	}
	return nil
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

	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		return p.parseFromSubselect(sel)
	}
	return p.parseFromTableOrFunction(sel)
}

func (p *Parser) parseFromSubselect(sel *Select) error {
	p.next()
	subSel, err := p.parseSelect()
	if err != nil {
		return err
	}
	if p.cur.Typ != tSymbol || p.cur.Val != ")" {
		return p.errf("expected ) after subselect in FROM")
	}
	p.next()
	alias, err := p.parseRequiredAlias("expected alias after AS for subselect", "expected alias for subselect in FROM")
	if err != nil {
		return err
	}
	sel.From = FromItem{Subquery: subSel, Alias: alias}
	return nil
}

func (p *Parser) parseFromTableOrFunction(sel *Select) error {
	from := p.parseQualifiedIdentLike()
	if from == "" {
		return p.errf("expected table or table-valued function")
	}

	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		fcExpr, err := p.parseFuncCallWithName(from)
		if err != nil {
			return err
		}
		fc, ok := fcExpr.(*FuncCall)
		if !ok {
			return p.errf("internal: expected FuncCall for table function %q", from)
		}
		if fc.Over != nil {
			return p.errf("OVER clause not allowed for table-valued functions in FROM")
		}
		alias, err := p.parseOptionalAlias(from, "expected alias")
		if err != nil {
			return err
		}
		sel.From = FromItem{TableFunc: &TableFuncCall{Name: from, Args: fc.Args, Alias: alias}, Alias: alias}
		return nil
	}

	alias, err := p.parseOptionalAlias(from, "expected alias")
	if err != nil {
		return err
	}
	sel.From = FromItem{Table: from, Alias: alias}
	return nil
}

func (p *Parser) parseRequiredAlias(asMsg, missingMsg string) (string, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias := p.parseIdentLike()
		if alias == "" {
			return "", p.errf("%s", asMsg)
		}
		return alias, nil
	}
	if p.cur.Typ == tIdent {
		alias := p.cur.Val
		p.next()
		return alias, nil
	}
	return "", p.errf("%s", missingMsg)
}

func (p *Parser) parseOptionalAlias(defaultAlias, asMsg string) (string, error) {
	alias := defaultAlias
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		alias = p.parseIdentLike()
		if alias == "" {
			return "", p.errf("%s", asMsg)
		}
		return alias, nil
	}
	if p.cur.Typ == tIdent {
		alias = p.cur.Val
		p.next()
	}
	return alias, nil
}

func (p *Parser) parseJoinClauses(sel *Select) error {
	for {
		if p.cur.Typ == tKeyword && p.cur.Val == "JOIN" {
			p.next()
			right, on, err := p.parseJoinTail()
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: JoinInner, Right: right, On: on})
			continue
		}
		if p.cur.Typ == tKeyword && (p.cur.Val == "LEFT" || p.cur.Val == "RIGHT" || p.cur.Val == "FULL") {
			var jt JoinType
			switch p.cur.Val {
			case "LEFT":
				jt = JoinLeft
			case "RIGHT":
				jt = JoinRight
			case "FULL":
				jt = JoinFull
			}
			p.next()
			if p.cur.Typ == tKeyword && p.cur.Val == "OUTER" {
				p.next()
			}
			if err := p.expectKeyword("JOIN"); err != nil {
				return err
			}
			right, on, err := p.parseJoinTail()
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: jt, Right: right, On: on})
			continue
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "CROSS" {
			p.next()
			if err := p.expectKeyword("JOIN"); err != nil {
				return err
			}
			// CROSS JOIN is an unconditional Cartesian product: no ON clause,
			// so it can't reuse parseJoinTail (which always requires one).
			rt := p.parseQualifiedIdentLike()
			if rt == "" {
				return p.errf("expected table name after CROSS JOIN")
			}
			alias, err := p.parseOptionalAlias(rt, "expected alias")
			if err != nil {
				return err
			}
			sel.Joins = append(sel.Joins, JoinClause{Type: JoinCross, Right: FromItem{Table: rt, Alias: alias}, On: nil})
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
		n, err := p.parseLimitOffsetValue("LIMIT")
		if err != nil {
			return err
		}
		sel.Limit = n
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OFFSET" {
		p.next()
		n, err := p.parseLimitOffsetValue("OFFSET")
		if err != nil {
			return err
		}
		sel.Offset = n
	}
	// SQL:2008 alternate syntax: OFFSET n ROWS [FETCH {FIRST|NEXT} m {ROW|ROWS} ONLY].
	// The bare "ROWS" after a numeric OFFSET is optional noise words.
	if p.cur.Typ == tKeyword && p.cur.Val == "ROWS" && sel.Offset != nil {
		p.next()
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "FETCH" {
		p.next()
		if p.cur.Typ != tKeyword || (p.cur.Val != "FIRST" && p.cur.Val != "NEXT") {
			return p.errf("expected FIRST or NEXT after FETCH")
		}
		p.next()
		n, err := p.parseLimitOffsetValue("FETCH")
		if err != nil {
			return err
		}
		sel.Limit = n
		if p.cur.Typ != tKeyword || (p.cur.Val != "ROW" && p.cur.Val != "ROWS") {
			return p.errf("expected ROW or ROWS after FETCH count")
		}
		p.next()
		if err := p.expectKeyword("ONLY"); err != nil {
			return err
		}
	}
	return nil
}

// parseLimitOffsetValue parses a LIMIT/OFFSET/FETCH value: either the
// SQL-standard "ALL" (no limit — returns nil, nil), or a constant integer
// expression evaluated immediately (LIMIT/OFFSET are resolved before
// execution, not per-row), e.g. a bare literal or arithmetic like "2 + 3".
// Non-constant expressions (column references, subqueries) are rejected
// with a clear error.
func (p *Parser) parseLimitOffsetValue(clause string) (*int, error) {
	if p.cur.Typ == tKeyword && p.cur.Val == "ALL" {
		p.next()
		return nil, nil
	}
	expr, err := p.parseAddSub()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", clause, err)
	}
	val, err := evalExpr(ExecEnv{}, expr, Row{})
	if err != nil {
		return nil, fmt.Errorf("%s must be a constant expression: %w", clause, err)
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("%s expects an integer, got %T", clause, val)
	}
	if f != math.Trunc(f) {
		return nil, fmt.Errorf("%s expects an integer, got %v", clause, f)
	}
	n := int(f)
	if n < 0 {
		return nil, fmt.Errorf("%s must be non-negative, got %d", clause, n)
	}
	return &n, nil
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

func (p *Parser) parseJoinTail() (FromItem, Expr, error) {
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		return p.parseJoinSubselectTail()
	}
	return p.parseJoinTableOrFunctionTail()
}

func (p *Parser) parseJoinSubselectTail() (FromItem, Expr, error) {
	p.next()
	subSel, err := p.parseSelect()
	if err != nil {
		return FromItem{}, nil, err
	}
	if p.cur.Typ != tSymbol || p.cur.Val != ")" {
		return FromItem{}, nil, p.errf("expected ) after subselect in JOIN")
	}
	p.next()
	alias, err := p.parseRequiredAlias("expected alias after AS for subselect", "expected alias for subselect in JOIN")
	if err != nil {
		return FromItem{}, nil, err
	}
	on, err := p.parseJoinOnExpr()
	if err != nil {
		return FromItem{}, nil, err
	}
	return FromItem{Subquery: subSel, Alias: alias}, on, nil
}

func (p *Parser) parseJoinTableOrFunctionTail() (FromItem, Expr, error) {
	rt := p.parseQualifiedIdentLike()
	if rt == "" {
		return FromItem{}, nil, p.errf("expected table or table-valued function")
	}

	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		fcExpr, err := p.parseFuncCallWithName(rt)
		if err != nil {
			return FromItem{}, nil, err
		}
		fc, ok := fcExpr.(*FuncCall)
		if !ok {
			return FromItem{}, nil, p.errf("internal: expected FuncCall for table function %q", rt)
		}
		if fc.Over != nil {
			return FromItem{}, nil, p.errf("OVER clause not allowed for table-valued functions in JOIN")
		}
		alias, err := p.parseOptionalAlias(rt, "expected alias")
		if err != nil {
			return FromItem{}, nil, err
		}
		on, err := p.parseJoinOnExpr()
		if err != nil {
			return FromItem{}, nil, err
		}
		return FromItem{TableFunc: &TableFuncCall{Name: rt, Args: fc.Args, Alias: alias}, Alias: alias}, on, nil
	}

	alias, err := p.parseOptionalAlias(rt, "expected alias")
	if err != nil {
		return FromItem{}, nil, err
	}
	on, err := p.parseJoinOnExpr()
	if err != nil {
		return FromItem{}, nil, err
	}
	return FromItem{Table: rt, Alias: alias}, on, nil
}

func (p *Parser) parseJoinOnExpr() (Expr, error) {
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	on, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return on, nil
}

func (p *Parser) parseColumnDefs() ([]storage.Column, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	cols := make([]storage.Column, 0, 8) // Pre-allocate for typical table
	for {
		// A comma-separated item starting with FOREIGN is a table-level
		// constraint ("FOREIGN KEY (col) REFERENCES tbl(col) ..."), not a
		// column definition — apply it to the already-parsed column it
		// names instead of appending a new column.
		if p.cur.Typ == tKeyword && p.cur.Val == "FOREIGN" {
			if err := p.parseTableLevelForeignKey(cols); err != nil {
				return nil, err
			}
		} else {
			col, err := p.parseSingleColumnDef()
			if err != nil {
				return nil, err
			}
			cols = append(cols, col)
		}

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

// parseTableLevelForeignKey parses "FOREIGN KEY (col) REFERENCES tbl(col)
// [ON DELETE action] [ON UPDATE action]" and attaches the result to the
// named column within cols (which must already have been parsed — table
// constraints are written after the columns they reference in every
// realistic schema). Only a single local column is supported per
// constraint; composite (multi-column) foreign keys are not.
func (p *Parser) parseTableLevelForeignKey(cols []storage.Column) error {
	p.next() // consume FOREIGN
	if err := p.expectKeyword("KEY"); err != nil {
		return err
	}
	if err := p.expectSymbol("("); err != nil {
		return err
	}
	localCol := p.parseIdentLike()
	if localCol == "" {
		return p.errf("expected column name in FOREIGN KEY (...)")
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}
	if err := p.expectKeyword("REFERENCES"); err != nil {
		return err
	}
	table := p.parseIdentLike()
	if table == "" {
		return p.errf("expected table name after REFERENCES")
	}
	if err := p.expectSymbol("("); err != nil {
		return err
	}
	refCol := p.parseIdentLike()
	if refCol == "" {
		return p.errf("expected column name in REFERENCES")
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}
	onDelete, onUpdate, err := p.parseOnDeleteOnUpdateClauses()
	if err != nil {
		return err
	}
	for i := range cols {
		if strings.EqualFold(cols[i].Name, localCol) {
			cols[i].Constraint = storage.ForeignKey
			cols[i].ForeignKey = &storage.ForeignKeyRef{Table: table, Column: refCol, OnDelete: onDelete, OnUpdate: onUpdate}
			return nil
		}
	}
	return p.errf("FOREIGN KEY (%s): no such column in this table", localCol)
}

func (p *Parser) parseSingleColumnDef() (storage.Column, error) {
	name := p.parseIdentLike()
	if name == "" {
		return storage.Column{}, p.errf("expected column name")
	}
	typ, err := p.parseColumnType()
	if err != nil {
		return storage.Column{}, p.errf("unknown type for column %q", name)
	}

	col := storage.Column{
		Name:         name,
		Type:         typ.typ,
		DeclaredType: typ.declared,
		Affinity:     typ.affinity,
		Constraint:   storage.NoConstraint,
	}

	// Parse constraints
	err = p.parseColumnConstraints(&col)
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
		return p.parseReferencesClauseInto(col)
	}
	return nil
}

func (p *Parser) parseUniqueConstraint(col *storage.Column) error {
	p.next()
	col.Constraint = storage.Unique
	return nil
}

// parseReferencesConstraint handles a column constraint that starts directly
// with REFERENCES (i.e. "col TYPE REFERENCES table(col)", the common SQL
// shorthand that omits "FOREIGN KEY"). For POINTER-typed columns this instead
// records a graph-traversal target table (an unrelated, pre-existing
// tinySQL-specific feature, not a real foreign key constraint).
func (p *Parser) parseReferencesConstraint(col *storage.Column) error {
	if col.Type == storage.PointerType {
		p.next()
		table := p.parseIdentLike()
		if table != "" {
			col.PointerTable = table
		}
		return nil
	}
	return p.parseReferencesClauseInto(col)
}

// parseReferencesClauseInto parses "REFERENCES table(column) [ON DELETE
// action] [ON UPDATE action]" (in either order) starting at the REFERENCES
// keyword, and records the result as col's foreign key. Shared by both
// column-level spellings: "col TYPE REFERENCES ..." and
// "col TYPE FOREIGN KEY REFERENCES ...".
func (p *Parser) parseReferencesClauseInto(col *storage.Column) error {
	if !(p.cur.Typ == tKeyword && p.cur.Val == "REFERENCES") {
		return nil
	}
	p.next()
	table := p.parseIdentLike()
	if table == "" {
		return p.errf("expected table name after REFERENCES")
	}
	if err := p.expectSymbol("("); err != nil {
		return err
	}
	column := p.parseIdentLike()
	if column == "" {
		return p.errf("expected column name in REFERENCES")
	}
	if err := p.expectSymbol(")"); err != nil {
		return err
	}
	onDelete, onUpdate, err := p.parseOnDeleteOnUpdateClauses()
	if err != nil {
		return err
	}
	col.Constraint = storage.ForeignKey
	col.ForeignKey = &storage.ForeignKeyRef{Table: table, Column: column, OnDelete: onDelete, OnUpdate: onUpdate}
	return nil
}

// parseOnDeleteOnUpdateClauses parses zero or more "ON DELETE <action>" /
// "ON UPDATE <action>" clauses, in either order (standard SQL allows both
// orderings), stopping at the first token that isn't a leading ON.
func (p *Parser) parseOnDeleteOnUpdateClauses() (onDelete, onUpdate storage.ReferentialAction, err error) {
	for p.cur.Typ == tKeyword && p.cur.Val == "ON" {
		p.next()
		switch {
		case p.cur.Typ == tKeyword && p.cur.Val == "DELETE":
			p.next()
			onDelete, err = p.parseReferentialAction()
		case p.cur.Typ == tKeyword && p.cur.Val == "UPDATE":
			p.next()
			onUpdate, err = p.parseReferentialAction()
		default:
			return onDelete, onUpdate, p.errf("expected DELETE or UPDATE after ON")
		}
		if err != nil {
			return onDelete, onUpdate, err
		}
	}
	return onDelete, onUpdate, nil
}

// parseReferentialAction parses CASCADE | SET NULL | RESTRICT | NO ACTION,
// the token(s) following ON DELETE/ON UPDATE.
func (p *Parser) parseReferentialAction() (storage.ReferentialAction, error) {
	if p.cur.Typ != tKeyword {
		return storage.NoAction, p.errf("expected a referential action (CASCADE, SET NULL, RESTRICT, or NO ACTION)")
	}
	switch p.cur.Val {
	case "CASCADE":
		p.next()
		return storage.Cascade, nil
	case "RESTRICT":
		p.next()
		return storage.Restrict, nil
	case "SET":
		p.next()
		if err := p.expectKeyword("NULL"); err != nil {
			return storage.NoAction, err
		}
		return storage.SetNull, nil
	case "NO":
		p.next()
		if err := p.expectKeyword("ACTION"); err != nil {
			return storage.NoAction, err
		}
		return storage.NoAction, nil
	}
	return storage.NoAction, p.errf("expected a referential action (CASCADE, SET NULL, RESTRICT, or NO ACTION)")
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
	// Vector types (for RAG / embedding storage)
	"VECTOR":    storage.VectorType,
	"EMBEDDING": storage.VectorType,
	// Extra data types
	"YAML":   storage.YAMLType,
	"URL":    storage.URLType,
	"HASH":   storage.HASHType,
	"BITMAP": storage.BitmapType,
	// Existing native types which were previously only usable through the
	// storage API. They also make ordinary SQLite schemas more portable.
	"DECIMAL":  storage.DecimalType,
	"NUMERIC":  storage.DecimalType,
	"MONEY":    storage.MoneyType,
	"UUID":     storage.UUIDType,
	"XML":      storage.XMLType,
	"INTERVAL": storage.IntervalType,
	// Binary data. BLOB is the canonical SQL spelling; the aliases make it
	// practical to import SQLite/PostgreSQL-ish schemas without weakening the
	// runtime invariant that BlobType values are always []byte.
	"BLOB":      storage.BlobType,
	"BYTEA":     storage.BlobType,
	"BINARY":    storage.BlobType,
	"VARBINARY": storage.BlobType,
}

type parsedColumnType struct {
	typ      storage.ColType
	declared string
	affinity storage.SQLiteAffinity
}

// parseColumnType accepts both tinySQL's native names and SQLite's permissive
// type declarations. SQLite's type system is affinity-based, so declarations
// such as VARCHAR(255), DOUBLE PRECISION and UNSIGNED BIG INT must not force
// the engine to invent a separate physical value type.
func (p *Parser) parseColumnType() (parsedColumnType, error) {
	// SQLite allows a column without a declared type. It has BLOB affinity and
	// accepts every storage class, represented here by InterfaceType.
	if p.isColumnTypeTerminator() {
		return parsedColumnType{typ: storage.InterfaceType, affinity: storage.AffinityBlob}, nil
	}
	if p.cur.Typ != tKeyword && p.cur.Typ != tIdent {
		return parsedColumnType{}, p.errf("expected column type")
	}

	words := make([]string, 0, 3)
	var arguments string
	for (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && !p.isColumnTypeTerminator() {
		words = append(words, upper(p.cur.Val))
		p.next()
		// A SQLite type name may be followed by one parenthesized length or
		// precision list. Its values are schema decoration, not runtime limits.
		if p.cur.Typ == tSymbol && p.cur.Val == "(" {
			var err error
			arguments, err = p.skipTypeArguments()
			if err != nil {
				return parsedColumnType{}, err
			}
			break
		}
	}
	if len(words) == 0 {
		return parsedColumnType{}, p.errf("expected column type")
	}

	declared := strings.Join(words, " ") + arguments
	// ANY is the SQLite STRICT-table escape hatch. It has no coercion and
	// preserves the value's original storage class, so InterfaceType is the
	// existing tinySQL representation rather than a new value type.
	if declared == "ANY" {
		return parsedColumnType{typ: storage.InterfaceType, declared: declared, affinity: storage.AffinityBlob}, nil
	}
	if typ, ok := typeKeywordMap[declared]; ok {
		// These names have SQLite-defined affinity semantics. Keep native
		// tinySQL spellings such as INT8, JSON and TIMESTAMP strict.
		switch declared {
		case "INT", "FLOAT", "DOUBLE", "TEXT", "NUMERIC", "DECIMAL", "BOOL", "BOOLEAN":
			return parsedColumnType{typ: typ, declared: declared, affinity: sqliteAffinity(declared)}, nil
		default:
			return parsedColumnType{typ: typ, declared: declared}, nil
		}
	}
	// A one-token native type retains existing tinySQL coercion semantics.
	if len(words) == 1 && arguments == "" {
		if typ, ok := typeKeywordMap[words[0]]; ok {
			return parsedColumnType{typ: typ, declared: declared}, nil
		}
	}

	affinity := sqliteAffinity(declared)
	typ := storage.InterfaceType
	switch affinity {
	case storage.AffinityInteger:
		typ = storage.IntType
	case storage.AffinityReal:
		typ = storage.FloatType
	case storage.AffinityText:
		typ = storage.TextType
	case storage.AffinityNumeric:
		typ = storage.DecimalType
	case storage.AffinityBlob:
		// A declared BLOB is a binary tinySQL column. A type-less column is
		// represented above as InterfaceType so it remains fully dynamic.
		if declared == "BLOB" {
			typ = storage.BlobType
			affinity = storage.AffinityDefault
		}
	}
	return parsedColumnType{typ: typ, declared: declared, affinity: affinity}, nil
}

func (p *Parser) isColumnTypeTerminator() bool {
	if p.cur.Typ == tSymbol && (p.cur.Val == "," || p.cur.Val == ")") {
		return true
	}
	if p.cur.Typ != tKeyword {
		return false
	}
	switch p.cur.Val {
	case "PRIMARY", "FOREIGN", "UNIQUE", "REFERENCES", "NOT", "NULL":
		return true
	default:
		return false
	}
}

func (p *Parser) skipTypeArguments() (string, error) {
	depth := 0
	var b strings.Builder
	for p.cur.Typ == tSymbol && p.cur.Val == "(" || depth > 0 {
		if p.cur.Typ == tEOF {
			return "", p.errf("unterminated type arguments")
		}
		b.WriteString(p.cur.Val)
		if p.cur.Typ == tSymbol {
			switch p.cur.Val {
			case "(":
				depth++
			case ")":
				depth--
			}
		}
		p.next()
		if depth == 0 {
			return b.String(), nil
		}
	}
	return "", nil
}

func sqliteAffinity(declared string) storage.SQLiteAffinity {
	// This is SQLite's documented affinity algorithm, applied to the upper
	// case declaration. Order matters: "FLOATING POINT" contains "INT".
	base := declared
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	switch {
	case strings.Contains(base, "INT"):
		return storage.AffinityInteger
	case strings.Contains(base, "CHAR"), strings.Contains(base, "CLOB"), strings.Contains(base, "TEXT"):
		return storage.AffinityText
	case base == "", strings.Contains(base, "BLOB"):
		return storage.AffinityBlob
	case strings.Contains(base, "REAL"), strings.Contains(base, "FLOA"), strings.Contains(base, "DOUB"):
		return storage.AffinityReal
	default:
		return storage.AffinityNumeric
	}
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

func (p *Parser) parseQualifiedIdentLike() string {
	name := p.parseIdentLike()
	if name == "" {
		return ""
	}
	parts := []string{name}
	for p.cur.Typ == tSymbol && p.cur.Val == "." {
		p.next()
		part := p.parseIdentLike()
		if part == "" {
			return strings.Join(parts, ".")
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ".")
}

// Expressions
func (p *Parser) parseExpr() (Expr, error) {
	if err := p.enterRecursion(); err != nil {
		return nil, err
	}
	defer p.exitRecursion()
	return p.parseOr()
}
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
		negate := p.consumeCmpNot()
		next, matched, err := p.parseCmpTail(l, negate)
		if err != nil {
			return nil, err
		}
		if !matched {
			break
		}
		l = next
	}

	return l, nil
}

func (p *Parser) consumeCmpNot() bool {
	if p.cur.Typ == tKeyword && p.cur.Val == "NOT" {
		p.next()
		return true
	}
	return false
}

func (p *Parser) parseCmpTail(l Expr, negate bool) (Expr, bool, error) {
	if expr, ok, err := p.parseCmpBetween(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpIn(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpLike(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpRegexp(l, negate); ok || err != nil {
		return expr, ok, err
	}
	if expr, ok, err := p.parseCmpSimilar(l, negate); ok || err != nil {
		return expr, ok, err
	}
	return p.parseCmpSymbol(l)
}

func (p *Parser) parseCmpBetween(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "BETWEEN" {
		return nil, false, nil
	}
	p.next()
	lo, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	if p.cur.Typ != tKeyword || p.cur.Val != "AND" {
		return nil, true, p.errf("expected AND after BETWEEN lower bound")
	}
	p.next()
	hi, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	switch l.(type) {
	case *VarRef, *Literal:
		// Desugar plain column/literal comparands: re-evaluating them is
		// free and the raw fast paths compile these Binary trees into
		// tight filters.
		if negate {
			return &Binary{Op: "OR",
				Left:  &Binary{Op: "<", Left: l, Right: lo},
				Right: &Binary{Op: ">", Left: l, Right: hi},
			}, true, nil
		}
		return &Binary{Op: "AND",
			Left:  &Binary{Op: ">=", Left: l, Right: lo},
			Right: &Binary{Op: "<=", Left: l, Right: hi},
		}, true, nil
	}
	// Complex comparand (function call, subquery, arithmetic, ...):
	// evaluate it once via a dedicated node.
	return &BetweenExpr{Expr: l, Lo: lo, Hi: hi, Negate: negate}, true, nil
}

func (p *Parser) parseCmpIn(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "IN" {
		return nil, false, nil
	}
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return nil, true, err
	}
	var values []Expr
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, true, err
		}
		values = append(values, e)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, true, err
	}
	return &InExpr{Expr: l, Values: values, Negate: negate}, true, nil
}

func (p *Parser) parseCmpLike(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword {
		return nil, false, nil
	}
	switch p.cur.Val {
	case "LIKE":
		p.next()
		pattern, escape, err := p.parseCmpPatternAndEscape()
		if err != nil {
			return nil, true, err
		}
		return &LikeExpr{Expr: l, Pattern: pattern, Escape: escape, Negate: negate}, true, nil
	case "ILIKE":
		p.next()
		pattern, escape, err := p.parseCmpPatternAndEscape()
		if err != nil {
			return nil, true, err
		}
		return &LikeExpr{Expr: l, Pattern: pattern, Escape: escape, Negate: negate, CaseInsensitive: true}, true, nil
	case "GLOB":
		p.next()
		pattern, err := p.parseAddSub()
		if err != nil {
			return nil, true, err
		}
		return &LikeExpr{Expr: l, Pattern: pattern, Negate: negate, GlobStyle: true}, true, nil
	default:
		return nil, false, nil
	}
}

func (p *Parser) parseCmpPatternAndEscape() (Expr, Expr, error) {
	pattern, err := p.parseAddSub()
	if err != nil {
		return nil, nil, err
	}
	var escape Expr
	if p.cur.Typ == tKeyword && p.cur.Val == "ESCAPE" {
		p.next()
		escape, err = p.parseAddSub()
		if err != nil {
			return nil, nil, err
		}
	}
	return pattern, escape, nil
}

func (p *Parser) parseCmpRegexp(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || (p.cur.Val != "REGEXP" && p.cur.Val != "RLIKE") {
		return nil, false, nil
	}
	p.next()
	pattern, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	return &RegexpExpr{Expr: l, Pattern: pattern, Negate: negate}, true, nil
}

func (p *Parser) parseCmpSimilar(l Expr, negate bool) (Expr, bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "SIMILAR" {
		return nil, false, nil
	}
	p.next()
	if err := p.expectKeyword("TO"); err != nil {
		return nil, true, err
	}
	pattern, err := p.parseAddSub()
	if err != nil {
		return nil, true, err
	}
	return &RegexpExpr{Expr: l, Pattern: pattern, Negate: negate, SimilarTo: true}, true, nil
}

func (p *Parser) parseCmpSymbol(l Expr) (Expr, bool, error) {
	if p.cur.Typ != tSymbol {
		return nil, false, nil
	}
	switch p.cur.Val {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		op := p.cur.Val
		p.next()
		r, err := p.parseAddSub()
		if err != nil {
			return nil, true, err
		}
		return &Binary{Op: op, Left: l, Right: r}, true, nil
	default:
		return nil, false, nil
	}
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

	return foldConstFuncCall(&FuncCall{Name: name, Args: args, Distinct: distinct, Over: overClause}), nil
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
