// Tests for FULL OUTER JOIN and CROSS JOIN. Before this, "FULL" and "CROSS"
// were not lexer keywords at all: "FROM a FULL OUTER JOIN b ON ..." silently
// mis-parsed as "FROM a" aliased "FULL", with "OUTER JOIN b ON ..." dropped
// on the floor — no parse error, just a query that silently ran as a
// one-table scan instead of a two-table join. "CROSS JOIN" failed to parse
// entirely ("expected keyword ON"). The README already (incorrectly)
// advertised both as supported.
package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupJoinDemoTables(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE dept (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO dept VALUES (1, 'Engineering')`)
	execSQL(t, db, `INSERT INTO dept VALUES (2, 'Sales')`)
	execSQL(t, db, `INSERT INTO dept VALUES (3, 'Marketing')`) // no employees

	execSQL(t, db, `CREATE TABLE emp (id INT, name TEXT, dept_id INT)`)
	execSQL(t, db, `INSERT INTO emp VALUES (1, 'Alice', 1)`)
	execSQL(t, db, `INSERT INTO emp VALUES (2, 'Bob', 1)`)
	execSQL(t, db, `INSERT INTO emp VALUES (3, 'Carol', 2)`)
	execSQL(t, db, `INSERT INTO emp VALUES (4, 'Dave', 99)`) // orphaned dept_id
	return db
}

func TestFullOuterJoinParsesAsRealJoin(t *testing.T) {
	db := setupJoinDemoTables(t)
	// Regression guard for the silent mis-parse: this must actually
	// reference both tables, not collapse into "FROM dept".
	rs := execSQL(t, db, `SELECT dept.name, emp.name FROM dept FULL OUTER JOIN emp ON dept.id = emp.dept_id`)
	// 3 matched (Eng-Alice, Eng-Bob, Sales-Carol) + 1 unmatched dept
	// (Marketing) + 1 unmatched emp (Dave) = 5 rows.
	if len(rs.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d: %+v", len(rs.Rows), rs.Rows)
	}
}

func TestFullOuterJoinUnmatchedLeftGetsNullRight(t *testing.T) {
	db := setupJoinDemoTables(t)
	rs := execSQL(t, db, `SELECT dept.name as dname, emp.name as ename FROM dept FULL OUTER JOIN emp ON dept.id = emp.dept_id`)
	var marketingRow Row
	found := false
	for _, r := range rs.Rows {
		if r["dname"] == "Marketing" {
			marketingRow = r
			found = true
		}
	}
	if !found {
		t.Fatalf("Marketing (unmatched dept) row missing entirely: %+v", rs.Rows)
	}
	if marketingRow["ename"] != nil {
		t.Errorf("expected NULL ename for unmatched dept, got %v", marketingRow["ename"])
	}
}

func TestFullOuterJoinUnmatchedRightGetsNullLeft(t *testing.T) {
	db := setupJoinDemoTables(t)
	rs := execSQL(t, db, `SELECT dept.name as dname, emp.name as ename FROM dept FULL OUTER JOIN emp ON dept.id = emp.dept_id`)
	var daveRow Row
	found := false
	for _, r := range rs.Rows {
		if r["ename"] == "Dave" {
			daveRow = r
			found = true
		}
	}
	if !found {
		t.Fatalf("Dave (unmatched emp, orphaned dept_id) row missing entirely: %+v", rs.Rows)
	}
	if daveRow["dname"] != nil {
		t.Errorf("expected NULL dname for unmatched emp, got %v", daveRow["dname"])
	}
}

func TestFullJoinIsSynonymForFullOuterJoin(t *testing.T) {
	db := setupJoinDemoTables(t)
	rs := execSQL(t, db, `SELECT dept.name, emp.name FROM dept FULL JOIN emp ON dept.id = emp.dept_id`)
	if len(rs.Rows) != 5 {
		t.Fatalf("FULL JOIN (without OUTER): expected 5 rows, got %d", len(rs.Rows))
	}
}

func TestCrossJoinProducesCartesianProduct(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE colors (name TEXT)`)
	execSQL(t, db, `INSERT INTO colors VALUES ('red')`)
	execSQL(t, db, `INSERT INTO colors VALUES ('blue')`)
	execSQL(t, db, `CREATE TABLE sizes (name TEXT)`)
	execSQL(t, db, `INSERT INTO sizes VALUES ('S')`)
	execSQL(t, db, `INSERT INTO sizes VALUES ('M')`)
	execSQL(t, db, `INSERT INTO sizes VALUES ('L')`)

	rs := execSQL(t, db, `SELECT colors.name as color, sizes.name as size FROM colors CROSS JOIN sizes`)
	if len(rs.Rows) != 6 {
		t.Fatalf("CROSS JOIN: expected 2*3=6 rows, got %d: %+v", len(rs.Rows), rs.Rows)
	}
}
