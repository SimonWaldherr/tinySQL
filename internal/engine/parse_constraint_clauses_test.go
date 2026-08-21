// Tests for the column- and table-level constraint clauses a CREATE TABLE can
// carry: the ones tinySQL applies, and the ones it now rejects by name instead
// of absorbing into the declared type or into a phantom column.
package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// parseCreateTable parses a CREATE TABLE and returns its column list.
func parseCreateTable(t *testing.T, sql string) []storage.Column {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	create, ok := stmt.(*CreateTable)
	if !ok {
		t.Fatalf("parse %q: statement = %T, want *CreateTable", sql, stmt)
	}
	return create.Cols
}

// expectParseError requires sql to fail to parse with a message containing
// want. The message text is asserted because "it errors somehow" is not the
// fix here: the clause that cannot be honoured has to be named, or the author
// has no way to tell which part of their schema tinySQL refused.
func expectParseError(t *testing.T, sql, want string) {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err == nil {
		t.Fatalf("parse %q succeeded (%#v), want error mentioning %q", sql, stmt, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("parse %q error = %v, want it to mention %q", sql, err, want)
	}
}

// TestUnsupportedColumnConstraintsAreRejected pins the behaviour change for
// the clauses that used to be swallowed into the declared type. Previously
// "b INT CHECK (b > 0)" produced a column of declared type "INT CHECK(b>0)"
// and an unenforced predicate, so "INSERT INTO ck VALUES (-5)" succeeded;
// "b INT GENERATED ALWAYS AS (a+1)" produced a plain nullable column that
// read NULL forever. Both are now parse errors: enforcing them is a separate
// project, but accepting them and doing nothing stores wrong data.
func TestUnsupportedColumnConstraintsAreRejected(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{`CREATE TABLE ck (b INT CHECK (b > 0))`, "CHECK constraints are not supported"},
		{`CREATE TABLE ck (b CHECK (b > 0))`, "CHECK constraints are not supported"},
		{`CREATE TABLE gc (a INT, b INT GENERATED ALWAYS AS (a+1))`, "generated columns are not supported"},
		// The bare "AS (expr)" spelling of a generated column, without the
		// GENERATED ALWAYS prefix.
		{`CREATE TABLE gc (a INT, b INT AS (a+1))`, "generated columns are not supported"},
		{`CREATE TABLE cl (name TEXT COLLATE NOCASE)`, "COLLATE is not supported"},
		// AUTOINCREMENT already failed before this change, but with a bare
		// "parse error near AUTOINCREMENT" that named no cause and offered no
		// alternative.
		{`CREATE TABLE ai (id INTEGER PRIMARY KEY AUTOINCREMENT)`, "AUTOINCREMENT is not supported"},
	}
	for _, tc := range cases {
		expectParseError(t, tc.sql, tc.want)
	}
}

// TestAlterTableAddColumnRejectsUnsupportedClauses covers the other caller of
// parseColumnType. ALTER TABLE ADD COLUMN used to absorb these clauses into
// the declared type exactly as CREATE TABLE did; now the type stops at the
// clause and the leftover token fails the statement. The message is the
// parser's generic "unexpected token after statement" rather than a named one
// because the ALTER path never runs parseColumnConstraints — the invariant
// worth pinning here is that the clause is not silently accepted.
func TestAlterTableAddColumnRejectsUnsupportedClauses(t *testing.T) {
	for _, sql := range []string{
		`ALTER TABLE t ADD COLUMN b INT CHECK (b > 0)`,
		`ALTER TABLE t ADD COLUMN b TEXT COLLATE NOCASE`,
		`ALTER TABLE t ADD COLUMN b INT GENERATED ALWAYS AS (a+1)`,
	} {
		if stmt, err := NewParser(sql).ParseStatement(); err == nil {
			t.Errorf("parse %q succeeded (%#v), want an error", sql, stmt)
		}
	}
}

// TestNamedColumnConstraintIsApplied covers the one clause of that family that
// is honoured rather than rejected: the CONSTRAINT <name> prefix carries no
// behaviour of its own, so the name is dropped and the constraint behind it
// takes effect.
func TestNamedColumnConstraintIsApplied(t *testing.T) {
	cols := parseCreateTable(t, `CREATE TABLE nc (id INT CONSTRAINT nn NOT NULL, email TEXT CONSTRAINT uq UNIQUE)`)
	if len(cols) != 2 {
		t.Fatalf("columns = %d (%#v), want 2", len(cols), cols)
	}
	if !cols[0].NotNull {
		t.Fatalf("CONSTRAINT nn NOT NULL did not set NotNull: %#v", cols[0])
	}
	if cols[1].Constraint != storage.Unique {
		t.Fatalf("CONSTRAINT uq UNIQUE = %v, want UNIQUE", cols[1].Constraint)
	}
	expectParseError(t, `CREATE TABLE nc (id INT CONSTRAINT ck CHECK (id > 0))`, "CHECK constraints are not supported")
}

// TestConstraintWordsRemainUsableAsColumnNames guards the cost of recognising
// CHECK/CONSTRAINT/UNIQUE at the start of a table item: none of them is a
// lexer keyword (CHECK and CONSTRAINT are plain identifiers), and a table with
// a column of that name parsed before this change. It still must.
func TestConstraintWordsRemainUsableAsColumnNames(t *testing.T) {
	// VARCHAR(20) on the "constraint" column is the case that needs the second
	// lookahead token: "constraint VARCHAR" and "CONSTRAINT pk_t PRIMARY" are
	// indistinguishable until the third token is read.
	cols := parseCreateTable(t, `CREATE TABLE t (check INT, constraint VARCHAR(20), unique INT)`)
	if len(cols) != 3 {
		t.Fatalf("columns = %d (%#v), want 3", len(cols), cols)
	}
	for i, want := range []string{"check", "constraint", "unique"} {
		if !strings.EqualFold(cols[i].Name, want) {
			t.Fatalf("column %d = %q, want %q", i, cols[i].Name, want)
		}
	}
	if cols[1].DeclaredType != "VARCHAR(20)" {
		t.Fatalf("column named \"constraint\" declared type = %q, want VARCHAR(20)", cols[1].DeclaredType)
	}
}

// TestTableLevelSingleColumnKeyConstraints covers the table-level spellings of
// PRIMARY KEY and UNIQUE over one column. They used to produce a phantom third
// column named "PRIMARY" of type "KEY(a)", which reached storage and the stored
// schema text and only surfaced later as "INSERT expects 3 values".
func TestTableLevelSingleColumnKeyConstraints(t *testing.T) {
	cols := parseCreateTable(t, `CREATE TABLE sp (a INT, b TEXT, PRIMARY KEY (a), UNIQUE (b))`)
	if len(cols) != 2 {
		t.Fatalf("columns = %d (%#v), want exactly the 2 declared columns", len(cols), cols)
	}
	if cols[0].Constraint != storage.PrimaryKey {
		t.Fatalf("PRIMARY KEY (a) → %v, want PRIMARY KEY", cols[0].Constraint)
	}
	if cols[1].Constraint != storage.Unique {
		t.Fatalf("UNIQUE (b) → %v, want UNIQUE", cols[1].Constraint)
	}

	// The named form is equivalent; the constraint name is not retained.
	cols = parseCreateTable(t, `CREATE TABLE sp (a INT, b TEXT, CONSTRAINT pk_sp PRIMARY KEY (a), CONSTRAINT uq_sp UNIQUE (b))`)
	if len(cols) != 2 || cols[0].Constraint != storage.PrimaryKey || cols[1].Constraint != storage.Unique {
		t.Fatalf("named table constraints → %#v", cols)
	}

	// A named FOREIGN KEY reaches the pre-existing FOREIGN KEY parser through
	// the same dispatch, which previously only saw the unnamed spelling.
	cols = parseCreateTable(t, `CREATE TABLE sp (a INT, CONSTRAINT fk_sp FOREIGN KEY (a) REFERENCES o(id) ON DELETE CASCADE)`)
	if len(cols) != 1 || cols[0].Constraint != storage.ForeignKey || cols[0].ForeignKey == nil {
		t.Fatalf("CONSTRAINT fk_sp FOREIGN KEY (a) → %#v", cols)
	}
	if fk := cols[0].ForeignKey; fk.Table != "o" || fk.Column != "id" || fk.OnDelete != storage.Cascade {
		t.Fatalf("named foreign key = %#v", cols[0].ForeignKey)
	}

	// Restating a column constraint at table level is redundant, not an error.
	cols = parseCreateTable(t, `CREATE TABLE sp (a INT PRIMARY KEY, PRIMARY KEY (a))`)
	if len(cols) != 1 || cols[0].Constraint != storage.PrimaryKey {
		t.Fatalf("restated PRIMARY KEY (a) → %#v", cols)
	}
}

// TestUnsupportedTableLevelConstraintsAreRejected pins the errors for the
// table-level constraints tinySQL cannot represent. Composite keys get their
// own message because they are ubiquitous in migrated schemas and the reason
// is structural (one constraint per column), not a parser gap.
func TestUnsupportedTableLevelConstraintsAreRejected(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{`CREATE TABLE mc (a INT, b INT, PRIMARY KEY (a,b))`, "composite primary keys are not supported"},
		{`CREATE TABLE mc (a INT, b INT, CONSTRAINT pk_mc PRIMARY KEY (a,b))`, "composite primary keys are not supported"},
		{`CREATE TABLE mc (a INT, b INT, UNIQUE (a,b))`, "composite UNIQUE constraints are not supported"},
		{`CREATE TABLE mc (a INT, b INT, CHECK (a < b))`, "table-level CHECK constraints are not supported"},
		{`CREATE TABLE mc (a INT, PRIMARY KEY (zz))`, "no such column"},
		// PRIMARY KEY and UNIQUE cannot both be recorded on one column, so
		// the conflict is reported instead of one silently replacing the other.
		{`CREATE TABLE mc (a INT UNIQUE, PRIMARY KEY (a))`, "already has a UNIQUE constraint"},
	}
	for _, tc := range cases {
		expectParseError(t, tc.sql, tc.want)
	}
}
