// Parsing a column definition and its constraints: NOT NULL, DEFAULT, PRIMARY
// KEY, UNIQUE, REFERENCES and the referential actions on a foreign key.
package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func (p *Parser) parseColumnDefs() ([]storage.Column, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	cols := make([]storage.Column, 0, 8) // Pre-allocate for typical table
	for {
		// A comma-separated item can be a table-level constraint rather than
		// a column definition, in which case it is applied to the
		// already-parsed column it names instead of appending a new column.
		// Only FOREIGN used to be recognised here, so "PRIMARY KEY (a, b)"
		// parsed as a column named "PRIMARY" of type "KEY(a,b)" — a phantom
		// column that reached storage and the stored schema text, and whose
		// only symptom was a later "INSERT expects 3 values".
		if p.startsTableLevelConstraint() {
			if err := p.parseTableLevelConstraint(cols); err != nil {
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

// startsTableLevelConstraint reports whether the item about to be parsed is a
// table-level constraint rather than a column definition.
//
// The distinction cannot be made from the leading word alone: PRIMARY, UNIQUE,
// CHECK and CONSTRAINT are also legal column names (and CHECK/CONSTRAINT are
// not even lexer keywords, so they arrive as ordinary identifiers), and
// "CREATE TABLE t (check INT)" parses today. Each case therefore confirms
// itself against what follows the word.
func (p *Parser) startsTableLevelConstraint() bool {
	switch p.constraintWord() {
	case "FOREIGN":
		// Unconditional, as it has always been here: FOREIGN is a keyword and
		// no realistic schema declares a column with that name.
		return true
	case "PRIMARY":
		return p.peek.Typ == tKeyword && p.peek.Val == "KEY"
	case "UNIQUE", "CHECK":
		// A table constraint opens its column list or predicate immediately
		// with "(", where a column named "unique" or "check" is followed by
		// its declared type or by "," / ")".
		return p.peek.Typ == tSymbol && p.peek.Val == "("
	case "CONSTRAINT":
		// "CONSTRAINT <name> <kind>" hides the deciding word behind the
		// constraint's name, one token further out than p.peek reaches, so
		// this case reads one token past it (see tokenAfterPeek).
		if p.peek.Typ != tIdent && p.peek.Typ != tKeyword {
			return false
		}
		kind := p.tokenAfterPeek()
		if kind.Typ != tIdent && kind.Typ != tKeyword {
			return false
		}
		switch upper(kind.Val) {
		case "PRIMARY", "UNIQUE", "CHECK", "FOREIGN":
			return true
		}
		return false
	}
	return false
}

// tokenAfterPeek returns the token one position past p.peek, consuming
// nothing. The parser otherwise carries exactly one token of lookahead, which
// is one short of telling "CONSTRAINT pk_t PRIMARY KEY (a)" apart from a
// column named "constraint" — both are three name-ish tokens in a row, and
// guessing wrong turns one of them into a parse error.
//
// The lexer is a pure forward scanner over an immutable string, so its entire
// state is its position: saving the struct, reading one token and restoring it
// is exact rather than heuristic. (scratch is a reusable buffer whose contents
// are never read across calls, so restoring an older slice header only risks
// re-growing it later.) The extra tokenization happens solely for items whose
// first word is CONSTRAINT.
func (p *Parser) tokenAfterPeek() token {
	saved := *p.lx
	tok := p.lx.nextToken()
	*p.lx = saved
	return tok
}

// parseTableLevelConstraint parses one table-level constraint item and applies
// it to the column it names, mirroring parseTableLevelForeignKey: tinySQL
// records constraints per column (storage.Column.Constraint), so a table-level
// constraint over exactly one column is expressible and one over several is
// not. The unsupported cases are reported by name rather than approximated,
// because a dropped PRIMARY KEY or UNIQUE silently admits duplicate rows.
func (p *Parser) parseTableLevelConstraint(cols []storage.Column) error {
	if p.constraintWord() == "CONSTRAINT" {
		p.next()
		if name := p.parseIdentLike(); name == "" {
			return p.errf("expected constraint name after CONSTRAINT")
		}
		// The name itself is dropped; see parseColumnConstraints for why that
		// is safe where dropping the constraint would not be.
	}
	switch p.constraintWord() {
	case "FOREIGN":
		return p.parseTableLevelForeignKey(cols)
	case "PRIMARY":
		p.next()
		if err := p.expectKeyword("KEY"); err != nil {
			return err
		}
		return p.applyTableLevelKeyConstraint("PRIMARY KEY", storage.PrimaryKey, cols)
	case "UNIQUE":
		p.next()
		return p.applyTableLevelKeyConstraint("UNIQUE", storage.Unique, cols)
	case "CHECK":
		return p.errf("table-level CHECK constraints are not supported; enforce the predicate in the application")
	}
	return p.errf("unsupported table-level constraint; expected PRIMARY KEY, UNIQUE, CHECK or FOREIGN KEY")
}

// applyTableLevelKeyConstraint parses the "(col, ...)" list of a table-level
// PRIMARY KEY / UNIQUE constraint and records it on the named column.
func (p *Parser) applyTableLevelKeyConstraint(kind string, constraint storage.ConstraintType, cols []storage.Column) error {
	names, err := p.parseParenColumnList(kind)
	if err != nil {
		return err
	}
	if len(names) != 1 {
		// Composite keys are ubiquitous in migrated schemas, so the message
		// names the limitation outright instead of leaving the author to guess
		// which part of the clause the parser objected to.
		list := strings.Join(names, ", ")
		if constraint == storage.PrimaryKey {
			return p.errf("composite primary keys are not supported: PRIMARY KEY (%s); tinySQL records one constraint per column", list)
		}
		return p.errf("composite UNIQUE constraints are not supported: UNIQUE (%s); tinySQL records one constraint per column", list)
	}
	for i := range cols {
		if !strings.EqualFold(cols[i].Name, names[0]) {
			continue
		}
		if cols[i].Constraint == constraint {
			// Already stated as a column constraint ("id INT PRIMARY KEY,
			// PRIMARY KEY (id)"). Restating it is redundant, not an error.
			return nil
		}
		if cols[i].Constraint != storage.NoConstraint {
			return p.errf("%s (%s): column already has a %s constraint and only one constraint per column is supported",
				kind, names[0], cols[i].Constraint)
		}
		cols[i].Constraint = constraint
		return nil
	}
	return p.errf("%s (%s): no such column in this table", kind, names[0])
}

// parseParenColumnList parses "(name [, name]...)", the column list shared by
// the table-level PRIMARY KEY and UNIQUE constraints.
func (p *Parser) parseParenColumnList(kind string) ([]string, error) {
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	names := make([]string, 0, 2)
	for {
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected column name in %s (...)", kind)
		}
		names = append(names, name)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}
	return names, nil
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
	for {
		switch p.constraintWord() {
		case "":
			return nil
		case "NOT":
			p.next()
			if err := p.expectKeyword("NULL"); err != nil {
				return err
			}
			col.NotNull = true
		case "NULL":
			// Explicit NULL is the default in ordinary SQLite tables.
			p.next()
			col.NotNull = false
		case "DEFAULT":
			if err := p.parseColumnDefault(col); err != nil {
				return err
			}
		case "PRIMARY":
			if err := p.parsePrimaryKeyConstraint(col); err != nil {
				return err
			}
		case "FOREIGN":
			if err := p.parseForeignKeyConstraint(col); err != nil {
				return err
			}
		case "UNIQUE":
			if err := p.parseUniqueConstraint(col); err != nil {
				return err
			}
		case "REFERENCES":
			if err := p.parseReferencesConstraint(col); err != nil {
				return err
			}
		case "CONSTRAINT":
			// A named column constraint ("CONSTRAINT nn NOT NULL"). The name
			// is metadata with nowhere to live (storage.Column has no field
			// for it) and nothing that looks it up, so it is skipped and the
			// constraint behind it is parsed normally. Dropping a
			// constraint's NAME changes nothing about which rows the table
			// accepts; dropping the constraint itself would, which is why
			// the clauses below are errors instead.
			p.next()
			if name := p.parseIdentLike(); name == "" {
				return p.errf("expected constraint name after CONSTRAINT on column %q", col.Name)
			}
		case "CHECK", "GENERATED", "AS", "COLLATE", "AUTOINCREMENT":
			// These parse cleanly but cannot be honoured, and each one
			// changes what the column MEANS. Before the type parser
			// terminated on them they were absorbed into the declared type
			// string and then ignored, which is the worst of the three
			// options: a CHECK that never rejects a row, or a generated
			// column that reads NULL forever, is wrong data that looks like
			// a working schema. Failing at CREATE TABLE keeps the mistake
			// where its author can still see it.
			return p.unsupportedColumnConstraint(col.Name)
		default:
			return nil
		}
	}
}

// unsupportedColumnConstraint reports the column constraint at the current
// token as unsupported, naming the clause so the message points at the exact
// text to remove. Errors are returned rather than skipped because tinySQL
// cannot enforce any of these, and silently dropping a constraint changes
// which rows the table accepts.
func (p *Parser) unsupportedColumnConstraint(colName string) error {
	switch p.constraintWord() {
	case "CHECK":
		return p.errf("CHECK constraints are not supported (column %q); enforce the predicate in the application", colName)
	case "GENERATED", "AS":
		return p.errf("generated columns are not supported (column %q); compute the value in the query or on insert", colName)
	case "COLLATE":
		return p.errf("COLLATE is not supported (column %q); tinySQL compares text with its default collation", colName)
	case "AUTOINCREMENT":
		return p.errf("AUTOINCREMENT is not supported (column %q); declare it PRIMARY KEY and supply the values explicitly", colName)
	}
	return p.errf("unsupported constraint on column %q", colName)
}

// parseColumnDefault accepts deterministic literal defaults. Dynamic defaults
// (for example CURRENT_TIMESTAMP) intentionally remain unsupported until they
// can be represented consistently in snapshots and read-only replicas.
func (p *Parser) parseColumnDefault(col *storage.Column) error {
	p.next() // DEFAULT
	expr, err := p.parseExpr()
	if err != nil {
		return err
	}
	v, ok := defaultLiteralValue(expr)
	if !ok {
		return p.errf("DEFAULT for column %q must be a literal", col.Name)
	}
	col.HasDefault = true
	col.DefaultValue = v
	return nil
}

func defaultLiteralValue(expr Expr) (any, bool) {
	switch e := expr.(type) {
	case *Literal:
		if b, ok := e.Val.([]byte); ok {
			return append([]byte(nil), b...), true
		}
		return e.Val, true
	case *Unary:
		lit, ok := e.Expr.(*Literal)
		if !ok {
			return nil, false
		}
		switch n := lit.Val.(type) {
		case int:
			if e.Op == "-" {
				return -n, true
			}
			if e.Op == "+" {
				return n, true
			}
		case float64:
			if e.Op == "-" {
				return -n, true
			}
			if e.Op == "+" {
				return n, true
			}
		}
	}
	return nil, false
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
	if p.cur.Typ != tKeyword || p.cur.Val != "REFERENCES" {
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
