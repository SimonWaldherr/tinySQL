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
	for p.cur.Typ == tKeyword {
		switch p.cur.Val {
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
		default:
			return nil
		}
	}
	return nil
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
