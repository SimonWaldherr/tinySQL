// Tests for LIMIT/OFFSET hardening: LIMIT ALL, constant-expression
// LIMIT/OFFSET (e.g. "LIMIT 2+3"), the SQL:2008 OFFSET ... FETCH syntax, and
// clearer rejection of negative/non-constant values.
package engine

import (
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupLimitTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	for i := 1; i <= 10; i++ {
		execSQL(t, db, `INSERT INTO t VALUES (`+strconv.Itoa(i)+`)`)
	}
	return db
}

func TestLimitAll(t *testing.T) {
	db := setupLimitTable(t)
	rs := execSQL(t, db, `SELECT * FROM t ORDER BY id LIMIT ALL`)
	if len(rs.Rows) != 10 {
		t.Fatalf("LIMIT ALL: expected 10 rows, got %d", len(rs.Rows))
	}
}

func TestLimitConstantExpression(t *testing.T) {
	db := setupLimitTable(t)
	rs := execSQL(t, db, `SELECT * FROM t ORDER BY id LIMIT 2 + 3`)
	if len(rs.Rows) != 5 {
		t.Fatalf("LIMIT 2+3: expected 5 rows, got %d", len(rs.Rows))
	}

	rs = execSQL(t, db, `SELECT * FROM t ORDER BY id LIMIT 10 - 8 OFFSET 1 + 1`)
	if len(rs.Rows) != 2 {
		t.Fatalf("LIMIT 10-8 OFFSET 1+1: expected 2 rows, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["id"], 3, "first row after offset 2")
}

func TestLimitZeroStillWorks(t *testing.T) {
	db := setupLimitTable(t)
	rs := execSQL(t, db, `SELECT * FROM t LIMIT 0`)
	if len(rs.Rows) != 0 {
		t.Fatalf("LIMIT 0: expected 0 rows, got %d", len(rs.Rows))
	}
}

func TestLimitNegativeRejected(t *testing.T) {
	p := NewParser(`SELECT * FROM t LIMIT -5`)
	if _, err := p.ParseStatement(); err == nil {
		t.Fatal("expected parse error for LIMIT -5")
	}
	p = NewParser(`SELECT * FROM t OFFSET -5`)
	if _, err := p.ParseStatement(); err == nil {
		t.Fatal("expected parse error for OFFSET -5")
	}
}

func TestLimitNonConstantRejected(t *testing.T) {
	// A bare column reference is not a constant expression at parse time.
	p := NewParser(`SELECT * FROM t LIMIT id`)
	if _, err := p.ParseStatement(); err == nil {
		t.Fatal("expected parse error for LIMIT referencing a column")
	}
}

func TestOffsetFetchSQL2008Syntax(t *testing.T) {
	db := setupLimitTable(t)
	rs := execSQL(t, db, `SELECT * FROM t ORDER BY id OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["id"], 3, "first row")
	expectInt(t, rs.Rows[2]["id"], 5, "last row")

	// FETCH NEXT is a synonym for FETCH FIRST.
	rs = execSQL(t, db, `SELECT * FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}

	// Singular ROW is also accepted.
	rs = execSQL(t, db, `SELECT * FROM t ORDER BY id FETCH FIRST 1 ROW ONLY`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["id"], 1, "only row")
}
