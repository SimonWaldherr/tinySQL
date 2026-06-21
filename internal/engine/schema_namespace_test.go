package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSchemaQualifiedTablesViewsAndMaterializedViews(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	execSchemaSQL(t, ctx, db, "CREATE TABLE sales.orders (id INT PRIMARY KEY, amount INT)")
	execSchemaSQL(t, ctx, db, "INSERT INTO sales.orders VALUES (1, 10), (2, 5)")
	execSchemaSQL(t, ctx, db, `
		CREATE VIEW sales.big_orders AS
		SELECT id, amount FROM sales.orders WHERE amount >= 10
	`)

	rs := querySchemaSQL(t, ctx, db, "SELECT id FROM sales.big_orders")
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 1 {
		t.Fatalf("schema-qualified view rows = %#v", rs.Rows)
	}

	rs = querySchemaSQL(t, ctx, db, "SELECT schema, name, full_name FROM sys.tables WHERE full_name = 'sales.orders'")
	if len(rs.Rows) != 1 || rs.Rows[0]["schema"] != "sales" || rs.Rows[0]["name"] != "orders" {
		t.Fatalf("sys.tables schema row = %#v", rs.Rows)
	}
	rs = querySchemaSQL(t, ctx, db, "SELECT schema, name FROM sys.views WHERE schema = 'sales' AND name = 'big_orders'")
	if len(rs.Rows) != 1 {
		t.Fatalf("sys.views schema row = %#v", rs.Rows)
	}

	execSchemaSQL(t, ctx, db, `
		CREATE MATERIALIZED VIEW sales.order_total AS
		SELECT SUM(amount) AS total FROM sales.orders
		INVALIDATE ON CHANGE
		WITH DATA
	`)
	rs = querySchemaSQL(t, ctx, db, "SELECT schema, name, invalidate_on_change FROM catalog.materialized_views WHERE schema = 'sales' AND name = 'order_total'")
	if len(rs.Rows) != 1 || rs.Rows[0]["invalidate_on_change"] != true {
		t.Fatalf("catalog.materialized_views schema row = %#v", rs.Rows)
	}

	execSchemaSQL(t, ctx, db, "INSERT INTO sales.orders VALUES (3, 2)")
	rs = querySchemaSQL(t, ctx, db, "SELECT status, is_stale FROM sys.objects WHERE schema = 'sales' AND name = 'order_total'")
	if len(rs.Rows) != 1 || rs.Rows[0]["status"] != "STALE" || rs.Rows[0]["is_stale"] != true {
		t.Fatalf("schema-qualified materialized view should be stale: %#v", rs.Rows)
	}

	rs = querySchemaSQL(t, ctx, db, "SELECT total FROM sales.order_total")
	if len(rs.Rows) != 1 || rs.Rows[0]["total"] != float64(17) {
		t.Fatalf("schema-qualified materialized view rows = %#v", rs.Rows)
	}
}

func execSchemaSQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) {
	t.Helper()
	if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
		t.Fatalf("SQL failed: %s\n  error: %v", sql, err)
	}
}

func querySchemaSQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(ctx, db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("SQL failed: %s\n  error: %v", sql, err)
	}
	return rs
}
