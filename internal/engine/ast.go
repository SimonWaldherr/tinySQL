// The abstract syntax tree: one type per statement and per expression form. The
// parser builds these, the executor switches on them, and nothing else needs to
// know how SQL text is spelled.
package engine

import (
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

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

// Analyze refreshes persisted planner statistics for one table, or every
// table in the current tenant when Table is empty.
type Analyze struct {
	Table string
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
	Name    string
	Columns []string
	Select  *Select
	// Recursive is true only when this CTE actually references itself. WITH
	// RECURSIVE permits recursion; it does not make every CTE recursive.
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
