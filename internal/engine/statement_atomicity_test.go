package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFailedStatementRollsBackTriggerWritesAndHeldTablePointers(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE orders (id INT)`,
		`CREATE TABLE audit (id INT PRIMARY KEY)`,
		`CREATE TRIGGER audit_orders AFTER INSERT ON orders
			FOR EACH ROW BEGIN
				INSERT INTO audit VALUES (NEW.id);
			END`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	orders, err := db.Get("default", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (7), (7)`)); err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("duplicate trigger write error = %v, want PRIMARY KEY error", err)
	}

	// The original pointer proves restoration happens in-place, not merely by
	// replacing DB's table map with a clone.
	if len(orders.Rows) != 0 {
		t.Fatalf("held orders table retained failed rows: %#v", orders.Rows)
	}
	for _, table := range []string{"orders", "audit"} {
		rs, err := Execute(ctx, db, "default", mustParse(`SELECT COUNT(*) AS n FROM `+table))
		if err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if got := expectAsInt(t, rs.Rows[0]["n"]); got != 0 {
			t.Fatalf("%s rows after failed statement = %d, want 0", table, got)
		}
	}
}

func TestFailedFastUpdateRestoresSecondaryIndexAndStatistics(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE users (id INT, email TEXT)`,
		`INSERT INTO users VALUES (1, 'one@example.test'), (2, 'two@example.test')`,
		`CREATE UNIQUE INDEX idx_users_email ON users(email)`,
		`ANALYZE users`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	if _, err := Execute(ctx, db, "default", mustParse(`UPDATE users SET email = 'one@example.test' WHERE id = 2`)); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate indexed update error = %v, want unique index error", err)
	}

	table, err := db.Get("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	stats := table.Statistics()
	if stats == nil || stats.Stale || stats.RowCount != 2 {
		t.Fatalf("statistics after rolled-back update = %#v", stats)
	}
	index := table.FindSecondaryIndex([]string{"email"})
	rowIDs, err := table.LookupSecondaryIndexPoint(index, []any{"two@example.test"})
	if err != nil || len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Fatalf("secondary index after rolled-back update = %#v, %v", rowIDs, err)
	}
}
