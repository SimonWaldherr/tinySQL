package engine

import (
	"context"
	"testing"

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

func explainOps(rs *ResultSet) map[string]bool {
	ops := make(map[string]bool, len(rs.Rows))
	for _, row := range rs.Rows {
		if op, ok := row["operation"].(string); ok {
			ops[op] = true
		}
	}
	return ops
}
