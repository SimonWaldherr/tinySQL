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

func TestPrimaryKeyUpdateUsesBoundedRowsWhenSecondaryIndexIsUntouched(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE users (id INT PRIMARY KEY, score FLOAT, bucket INT)`,
		`INSERT INTO users VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)`,
		`CREATE INDEX idx_users_bucket ON users(bucket)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	update := mustParse(`UPDATE users SET score = 42 WHERE id = 2`).(*Update)
	tableName, rowIDs, ok := rowUpdateSnapshotTarget(newDMLPlan(&dmlPlan{}, db, "default", update))
	if !ok || tableName != "users" || len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Fatalf("bounded update target = %q, %#v, %v; want users, [1], true", tableName, rowIDs, ok)
	}
	plan, ok, err := buildSimpleUpdatePlan(ExecEnv{ctx: ctx, tenant: "default", db: db}, update)
	if err != nil || !ok || len(plan.rowIDs) != 1 || plan.rowIDs[0] != 1 {
		t.Fatalf("bounded update plan = %#v, %v, %v", plan, ok, err)
	}

	residual := mustParse(`UPDATE users SET score = 99 WHERE id = 2 AND bucket = 999`).(*Update)
	if _, err := Execute(ctx, db, "default", residual); err != nil {
		t.Fatal(err)
	}
	table, err := db.Get("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	if got := table.Rows[1][1]; got != float64(2) {
		t.Fatalf("failed residual predicate updated score to %v", got)
	}

	if _, err := Execute(ctx, db, "default", update); err != nil {
		t.Fatal(err)
	}
	table, err = db.Get("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	if got := table.Rows[1][1]; got != float64(42) {
		t.Fatalf("updated score = %v, want 42", got)
	}

	indexedUpdate := mustParse(`UPDATE users SET bucket = 99 WHERE id = 2`).(*Update)
	if tableName, rowIDs, ok := rowUpdateSnapshotTarget(newDMLPlan(&dmlPlan{}, db, "default", indexedUpdate));
		!ok || tableName != "users" || len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Fatalf("indexed bounded update target = %q, %#v, %v; want users, [1], true", tableName, rowIDs, ok)
	}
}

func TestPrimaryKeyDeleteUsesBoundedRowsAndMaintainsSecondaryIndexes(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE users (id INT PRIMARY KEY, bucket INT)`,
		`INSERT INTO users VALUES (1, 10), (2, 20), (3, 30)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	residual := mustParse(`DELETE FROM users WHERE id = 2 AND bucket = 999`).(*Delete)
	tableName, rowIDs, ok := rowDeleteSnapshotTarget(newDMLPlan(&dmlPlan{}, db, "default", residual))
	if !ok || tableName != "users" || len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Fatalf("bounded delete target = %q, %#v, %v; want users, [1], true", tableName, rowIDs, ok)
	}
	rs, err := Execute(ctx, db, "default", residual)
	if err != nil {
		t.Fatal(err)
	}
	if got := expectAsInt(t, rs.Rows[0]["deleted"]); got != 0 {
		t.Fatalf("residual delete count = %d, want 0", got)
	}

	if _, err := Execute(ctx, db, "default", mustParse(`CREATE INDEX idx_users_bucket ON users(bucket)`)); err != nil {
		t.Fatal(err)
	}
	pointDelete := mustParse(`DELETE FROM users WHERE id = 2`).(*Delete)
	if _, _, ok := rowDeleteSnapshotTarget(newDMLPlan(&dmlPlan{}, db, "default", pointDelete)); ok {
		t.Fatal("secondary-indexed table used the compact delete snapshot")
	}
	rs, err = Execute(ctx, db, "default", pointDelete)
	if err != nil {
		t.Fatal(err)
	}
	if got := expectAsInt(t, rs.Rows[0]["deleted"]); got != 1 {
		t.Fatalf("point delete count = %d, want 1", got)
	}

	table, err := db.Get("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 2 || !rawEqual(table.Rows[0][0], 1) || !rawEqual(table.Rows[1][0], 3) {
		t.Fatalf("rows after point delete = %#v", table.Rows)
	}
	index := table.FindSecondaryIndex([]string{"bucket"})
	indexRows, err := table.LookupSecondaryIndexPoint(index, []any{30})
	if err != nil || len(indexRows) != 1 || indexRows[0] != 1 {
		t.Fatalf("secondary index after point delete = %#v, %v", indexRows, err)
	}
	constraintRows := lookupConstraintIndexRows(getConstraintIndex(table, 0), 3)
	if len(constraintRows) != 1 || constraintRows[0] != 1 {
		t.Fatalf("constraint index after point delete = %#v, want [1]", constraintRows)
	}
}
