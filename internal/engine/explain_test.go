package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestExplainSelectPlan(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		EXPLAIN
		WITH recent AS (
			SELECT id, amount FROM orders WHERE amount > 10
		)
		SELECT id, amount
		FROM recent
		WHERE amount < 100
		ORDER BY amount
		LIMIT 5
	`))
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}

	ops := explainOps(rs)
	for _, want := range []string{"PLAN", "CTE", "SCAN", "FILTER", "SORT", "LIMIT", "PROJECT"} {
		if !ops[want] {
			t.Fatalf("missing EXPLAIN operation %q in rows %#v", want, rs.Rows)
		}
	}
	if len(rs.Rows) == 0 || rs.Rows[0]["step"] != 1 {
		t.Fatalf("unexpected step numbering: %#v", rs.Rows)
	}
}

func TestExplainMaterializedViewPlanIncludesInvalidation(t *testing.T) {
	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`
		EXPLAIN CREATE MATERIALIZED VIEW sales_mv AS
		SELECT SUM(amount) AS total FROM sales
		INVALIDATE ON CHANGE
		WITH DATA
	`))
	if err != nil {
		t.Fatalf("EXPLAIN materialized view failed: %v", err)
	}

	ops := explainOps(rs)
	for _, want := range []string{"CREATE MATERIALIZED VIEW", "MATERIALIZE", "INVALIDATE", "SCAN", "PROJECT"} {
		if !ops[want] {
			t.Fatalf("missing EXPLAIN operation %q in rows %#v", want, rs.Rows)
		}
	}
}

func TestExplainAnalyzeMutatingStatementSharesStatementLifecycle(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT PRIMARY KEY)`)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// A re-entrant Execute call used to deadlock here: outer EXPLAIN held a
	// read lock while inner INSERT needed a write lock. Keep a timeout around
	// the regression so a future reintroduction fails as a clear deadlock.
	runWithTimeout(t, time.Second, func() error {
		_, err := Execute(ctx, db, "default", mustParse(`EXPLAIN ANALYZE INSERT INTO t VALUES (1)`))
		return err
	})
	rows := execSQL(t, db, `SELECT id FROM t`)
	if len(rows.Rows) != 1 || rows.Rows[0]["id"] != 1 {
		t.Fatalf("EXPLAIN ANALYZE INSERT rows = %#v", rows.Rows)
	}

	// The outer EXPLAIN ANALYZE lifecycle must also roll back a failed inner
	// DML statement rather than leaving its first row behind.
	_, err := Execute(ctx, db, "default", mustParse(`EXPLAIN ANALYZE INSERT INTO t VALUES (2), (2)`))
	if err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("duplicate EXPLAIN ANALYZE INSERT error = %v, want PRIMARY KEY", err)
	}
	rows = execSQL(t, db, `SELECT id FROM t ORDER BY id`)
	if len(rows.Rows) != 1 || rows.Rows[0]["id"] != 1 {
		t.Fatalf("failed EXPLAIN ANALYZE INSERT was not rolled back: %#v", rows.Rows)
	}
}

func TestExplainAnalyzeMutationRespectsReadOnlyMode(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	db.SetReadOnly(true)
	_, err := Execute(ctx, db, "default", mustParse(`EXPLAIN ANALYZE INSERT INTO t VALUES (1)`))
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only EXPLAIN ANALYZE INSERT error = %v, want read-only error", err)
	}
}

func explainOps(rs *ResultSet) map[string]bool {
	ops := make(map[string]bool, len(rs.Rows))
	for _, row := range rs.Rows {
		if op, ok := row["operation"].(string); ok {
			ops[op] = true
		}
	}
	return ops
}
