package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCheckConstraintsInsertUpdateAndThreeValuedLogic(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	defer db.Close()

	exec := func(sql string) (*ResultSet, error) {
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		return Execute(ctx, db, "default", stmt)
	}
	mustExec := func(sql string) {
		t.Helper()
		if _, err := exec(sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	mustExec(`CREATE TABLE items (
		id INT PRIMARY KEY,
		qty INT CHECK (qty > 0),
		min_qty INT,
		max_qty INT,
		CONSTRAINT range_ok CHECK (min_qty <= max_qty)
	)`)

	// Valid row passes.
	mustExec(`INSERT INTO items VALUES (1, 5, 1, 10)`)
	// NULL (UNKNOWN) passes SQL CHECK constraints.
	mustExec(`INSERT INTO items (id, qty, min_qty, max_qty) VALUES (2, NULL, NULL, 10)`)

	// Column-level CHECK failure on INSERT.
	if _, err := exec(`INSERT INTO items VALUES (3, 0, 1, 10)`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("expected CHECK constraint failure for qty=0, got %v", err)
	}
	// Table-level named CHECK failure on INSERT.
	if _, err := exec(`INSERT INTO items VALUES (4, 5, 20, 10)`); err == nil || !strings.Contains(err.Error(), "range_ok") {
		t.Fatalf("expected named CHECK constraint failure 'range_ok', got %v", err)
	}
	// ON CONFLICT DO NOTHING must still reject CHECK violations.
	if _, err := exec(`INSERT INTO items VALUES (5, -1, 1, 10) ON CONFLICT DO NOTHING`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("expected CHECK failure with ON CONFLICT DO NOTHING, got %v", err)
	}

	// Point UPDATE (fast path) and bulk UPDATE must enforce CHECK and roll back on failure.
	if _, err := exec(`UPDATE items SET qty = -3 WHERE id = 1`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("expected CHECK failure on point UPDATE, got %v", err)
	}
	if _, err := exec(`UPDATE items SET min_qty = 50`); err == nil || !strings.Contains(err.Error(), "range_ok") {
		t.Fatalf("expected CHECK failure on bulk UPDATE, got %v", err)
	}
	// Valid UPDATE succeeds.
	mustExec(`UPDATE items SET qty = 10 WHERE id = 1`)
	rs, err := exec(`SELECT qty FROM items WHERE id = 1`)
	if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["qty"] != 10 {
		t.Fatalf("unexpected rows after valid UPDATE: %+v, err=%v", rs, err)
	}
}

func TestCheckConstraintsSchemaValidationAndAlterTable(t *testing.T) {
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

	for _, badSQL := range []string{
		`CREATE TABLE bad1 (a INT, CHECK (missing_col > 0))`,
		`CREATE TABLE bad2 (a INT CHECK (a > (SELECT 1)))`,
		`CREATE TABLE bad3 (a INT CHECK (COUNT(*) > 0))`,
	} {
		if _, err := exec(badSQL); err == nil {
			t.Fatalf("expected error for %q, got nil", badSQL)
		}
	}

	if _, err := exec(`CREATE TABLE t (id INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	// ALTER TABLE ADD COLUMN with a CHECK that fails on existing rows is rejected atomically.
	if _, err := exec(`ALTER TABLE t ADD COLUMN score INT CHECK (id > 10)`); err == nil {
		t.Fatal("expected ALTER TABLE ADD COLUMN with failing CHECK on existing rows to fail")
	}
	// ALTER TABLE ADD COLUMN with a valid CHECK on existing rows (backfilled NULL passes) succeeds.
	if _, err := exec(`ALTER TABLE t ADD COLUMN score INT CHECK (score >= 0)`); err != nil {
		t.Fatalf("expected ALTER TABLE ADD COLUMN with valid CHECK to succeed, got %v", err)
	}
	if _, err := exec(`INSERT INTO t VALUES (2, -1)`); err == nil {
		t.Fatal("expected INSERT violating ALTER-added CHECK to fail")
	}
}

func BenchmarkCheckConstraintRow(b *testing.B) {
	table := storage.NewTable("stock", []storage.Column{
		{Name: "qty", Type: storage.IntType},
		{Name: "reserved", Type: storage.IntType},
	}, false)
	table.CheckConstraints = []storage.CheckConstraint{
		{Expr: "qty >= 0"},
		{Expr: "stock.reserved BETWEEN 0 AND qty"},
	}
	env := ExecEnv{ctx: b.Context()}
	row := []any{100, 5}
	for b.Loop() {
		if err := validateCheckConstraints(env, table, row); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTableCheckPlanTracksCurrentSchema(t *testing.T) {
	table := storage.NewTable("stock", []storage.Column{{Name: "qty", Type: storage.IntType}}, false)
	table.CheckConstraints = []storage.CheckConstraint{{Expr: "stock.qty > 0"}}
	env := ExecEnv{ctx: t.Context()}
	require := func(pass bool) {
		t.Helper()
		if err := validateCheckConstraints(env, table, []any{2}); (err == nil) != pass {
			t.Fatalf("check %q: err = %v, want pass = %v", table.CheckConstraints[0].Expr, err, pass)
		}
	}
	require(true)
	table.CheckConstraints[0].Expr = "qty > 2"
	require(false)
	if err := table.RenameColumn("qty", "count"); err != nil {
		t.Fatal(err)
	}
	table.CheckConstraints[0].Expr = "stock.count > 0"
	require(true)
	table.Name = "inventory"
	require(false)
}
