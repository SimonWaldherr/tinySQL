package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestOnConflictDoUpdateAndTargetedDoNothing(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	defer db.Close()

	exec := func(sql string) (*ResultSet, error) {
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			return nil, err
		}
		return Execute(ctx, db, "default", stmt)
	}
	mustExec := func(sql string) *ResultSet {
		t.Helper()
		rs, err := exec(sql)
		if err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
		return rs
	}

	mustExec(`CREATE TABLE inventory (
		id INT PRIMARY KEY,
		sku TEXT UNIQUE,
		qty INT NOT NULL CHECK (qty >= 0),
		note TEXT
	)`)

	mustExec(`INSERT INTO inventory VALUES (1, 'A1', 10, 'init')`)

	// Targeted DO NOTHING on PK.
	mustExec(`INSERT INTO inventory VALUES (1, 'A1', 99, 'skip') ON CONFLICT (id) DO NOTHING`)
	rs := mustExec(`SELECT qty, note FROM inventory WHERE id = 1`)
	if rs.Rows[0]["qty"] != 10 || rs.Rows[0]["note"] != "init" {
		t.Fatalf("expected row untouched after DO NOTHING, got %+v", rs.Rows[0])
	}

	// DO UPDATE on PK using excluded.* and existing column, plus RETURNING.
	rs = mustExec(`INSERT INTO inventory VALUES (1, 'A1', 5, 'restock'), (2, 'B2', 20, 'new')
		ON CONFLICT (id) DO UPDATE SET qty = inventory.qty + excluded.qty, note = excluded.note
		RETURNING id, qty, note`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 RETURNING rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 1 || rs.Rows[0]["qty"] != 15 || rs.Rows[0]["note"] != "restock" {
		t.Fatalf("unexpected upserted row 0: %+v", rs.Rows[0])
	}
	if rs.Rows[1]["id"] != 2 || rs.Rows[1]["qty"] != 20 || rs.Rows[1]["note"] != "new" {
		t.Fatalf("unexpected inserted row 1: %+v", rs.Rows[1])
	}

	// DO UPDATE with WHERE filter that evaluates FALSE skips the update.
	rs = mustExec(`INSERT INTO inventory VALUES (1, 'A1', 999, 'ignored')
		ON CONFLICT (id) DO UPDATE SET qty = excluded.qty WHERE inventory.qty > 100
		RETURNING id`)
	if len(rs.Rows) != 0 {
		t.Fatalf("expected 0 RETURNING rows when DO UPDATE WHERE is false, got %d", len(rs.Rows))
	}
	rs = mustExec(`SELECT qty FROM inventory WHERE id = 1`)
	if rs.Rows[0]["qty"] != 15 {
		t.Fatalf("expected qty=15 unchanged, got %v", rs.Rows[0]["qty"])
	}

	// DO UPDATE on UNIQUE column (sku) with column list INSERT.
	mustExec(`INSERT INTO inventory (id, sku, qty, note) VALUES (99, 'B2', 3, 'via_sku')
		ON CONFLICT (sku) DO UPDATE SET qty = inventory.qty + excluded.qty, note = excluded.note`)
	rs = mustExec(`SELECT id, qty, note FROM inventory WHERE sku = 'B2'`)
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 2 || rs.Rows[0]["qty"] != 23 || rs.Rows[0]["note"] != "via_sku" {
		t.Fatalf("unexpected row after UNIQUE conflict update: %+v", rs.Rows)
	}

	// DO UPDATE violating CHECK constraint fails and rolls back the statement.
	if _, err := exec(`INSERT INTO inventory VALUES (1, 'A1', -100, 'bad'), (3, 'C3', 1, 'good')
		ON CONFLICT (id) DO UPDATE SET qty = excluded.qty`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("expected CHECK constraint failure on DO UPDATE, got %v", err)
	}
	rs = mustExec(`SELECT COUNT(*) AS c FROM inventory`)
	if rs.Rows[0]["c"] != 2 {
		t.Fatalf("expected rollback leaving 2 rows, got %v", rs.Rows[0]["c"])
	}

	// Invalid conflict target (non-unique column) is rejected.
	if _, err := exec(`INSERT INTO inventory VALUES (4, 'D4', 1, 'x') ON CONFLICT (note) DO NOTHING`); err == nil {
		t.Fatal("expected error for non-unique ON CONFLICT target")
	}

	// Composite unique index conflict target.
	mustExec(`CREATE TABLE pairs (a INT, b INT, val TEXT)`)
	mustExec(`CREATE UNIQUE INDEX idx_pairs_ab ON pairs (a, b)`)
	mustExec(`INSERT INTO pairs VALUES (1, 2, 'first')`)
	mustExec(`INSERT INTO pairs VALUES (1, 2, 'second') ON CONFLICT (a, b) DO UPDATE SET val = excluded.val`)
	rs = mustExec(`SELECT val FROM pairs WHERE a = 1 AND b = 2`)
	if len(rs.Rows) != 1 || rs.Rows[0]["val"] != "second" {
		t.Fatalf("unexpected composite upsert result: %+v", rs.Rows)
	}
}

// Unqualified column names in DO UPDATE SET and WHERE refer to the existing
// row, while excluded.* refers to the proposed one.
func TestOnConflictDoUpdateUnqualifiedColumns(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	defer db.Close()

	exec := func(sql string) (*ResultSet, error) {
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			return nil, err
		}
		return Execute(ctx, db, "default", stmt)
	}
	mustExec := func(sql string) *ResultSet {
		t.Helper()
		rs, err := exec(sql)
		if err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
		return rs
	}

	mustExec(`CREATE TABLE stock (id INT PRIMARY KEY, qty INT, note TEXT)`)
	mustExec(`INSERT INTO stock VALUES (1, 5, 'a')`)
	mustExec(`INSERT INTO stock VALUES (1, 3, 'b') ON CONFLICT (id) DO UPDATE SET qty = qty + excluded.qty, note = excluded.note`)
	rs := mustExec(`SELECT qty, note FROM stock WHERE id = 1`)
	if rs.Rows[0]["qty"] != 8 || rs.Rows[0]["note"] != "b" {
		t.Fatalf("unqualified SET: got %+v, want qty=8 note=b", rs.Rows[0])
	}

	// The WHERE clause reads the existing row; FALSE leaves it unchanged.
	mustExec(`INSERT INTO stock VALUES (1, 100, 'c') ON CONFLICT (id) DO UPDATE SET qty = excluded.qty WHERE qty > 1000`)
	// Qualified and unqualified references to the same column agree.
	mustExec(`INSERT INTO stock VALUES (1, 1, 'd') ON CONFLICT (id) DO UPDATE SET note = note || '!' WHERE stock.qty < 100`)
	rs = mustExec(`SELECT qty, note FROM stock WHERE id = 1`)
	if rs.Rows[0]["qty"] != 8 || rs.Rows[0]["note"] != "b!" {
		t.Fatalf("unqualified WHERE: got %+v, want qty=8 note=b!", rs.Rows[0])
	}
}

// The proposed row is checked against CHECK constraints before the conflict
// lookup. A malformed row is therefore rejected even when DO NOTHING would
// otherwise skip it, the same way NOT NULL is already enforced first.
func TestOnConflictChecksProposedRowBeforeLookup(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	defer db.Close()

	exec := func(sql string) (*ResultSet, error) {
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			return nil, err
		}
		return Execute(ctx, db, "default", stmt)
	}
	if _, err := exec(`CREATE TABLE guarded (id INT PRIMARY KEY, qty INT CHECK (qty >= 0))`); err != nil {
		t.Fatal(err)
	}
	if _, err := exec(`INSERT INTO guarded VALUES (1, 5)`); err != nil {
		t.Fatal(err)
	}
	if _, err := exec(`INSERT INTO guarded VALUES (1, -1) ON CONFLICT (id) DO NOTHING`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("expected CHECK failure for a conflicting malformed row, got %v", err)
	}
	if _, err := exec(`INSERT INTO guarded VALUES (1, 9) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("well-formed conflicting row should be skipped: %v", err)
	}
}

// A DO UPDATE conflict is an UPDATE for trigger purposes: AFTER UPDATE sees the
// existing row as OLD and the upserted row as NEW, and the plain insert of a new
// key still fires AFTER INSERT.
func TestOnConflictDoUpdateFiresUpdateTriggers(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	defer db.Close()
	exec := func(sql string) (*ResultSet, error) {
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			return nil, err
		}
		return Execute(ctx, db, "default", stmt)
	}
	for _, q := range []string{
		`CREATE TABLE acct (id INT PRIMARY KEY, bal INT)`,
		`CREATE TABLE audit (id INT, old_bal INT, new_bal INT)`,
		`CREATE TRIGGER acct_upd AFTER UPDATE ON acct BEGIN INSERT INTO audit VALUES (NEW.id, OLD.bal, NEW.bal); END`,
		`CREATE TRIGGER acct_ins AFTER INSERT ON acct BEGIN INSERT INTO audit VALUES (NEW.id, 0, NEW.bal); END`,
		`INSERT INTO acct VALUES (1, 10)`,
		`INSERT INTO acct VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET bal = acct.bal + excluded.bal`,
	} {
		if _, err := exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	rs, err := exec(`SELECT id, old_bal, new_bal FROM audit`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 2 ||
		rs.Rows[0]["old_bal"] != 0 || rs.Rows[0]["new_bal"] != 10 ||
		rs.Rows[1]["old_bal"] != 10 || rs.Rows[1]["new_bal"] != 15 {
		t.Fatalf("unexpected audit rows: %+v", rs.Rows)
	}
}
