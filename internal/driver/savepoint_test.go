package driver

import (
	"context"
	"database/sql"
	"testing"
)

func TestDriverSavepointsNestedRollbackAndRelease(t *testing.T) {
	db, err := sql.Open("tinysql", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	mustExec(`CREATE TABLE sp_items (id INT PRIMARY KEY, val TEXT)`)

	// Savepoint outside a transaction must fail.
	if _, err := conn.ExecContext(ctx, `SAVEPOINT sp0`); err == nil {
		t.Fatal("expected SAVEPOINT outside transaction to fail")
	}

	mustExec(`BEGIN`)
	mustExec(`INSERT INTO sp_items VALUES (1, 'before_sp1')`)
	mustExec(`SAVEPOINT sp1`)
	mustExec(`INSERT INTO sp_items VALUES (2, 'after_sp1')`)
	mustExec(`SAVEPOINT sp2`)
	mustExec(`INSERT INTO sp_items VALUES (3, 'after_sp2')`)

	// Rollback to sp2 discards row 3 but keeps row 2 and sp2 active.
	mustExec(`ROLLBACK TO SAVEPOINT sp2`)
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sp_items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows after ROLLBACK TO sp2, got %d", count)
	}

	// Re-using sp2 after ROLLBACK TO sp2 still works.
	mustExec(`INSERT INTO sp_items VALUES (4, 'retry_sp2')`)
	mustExec(`ROLLBACK TO sp2`)
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sp_items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows after second ROLLBACK TO sp2, got %d", count)
	}

	// Rollback to sp1 discards row 2 and pops sp2.
	mustExec(`ROLLBACK TO sp1`)
	if _, err := conn.ExecContext(ctx, `RELEASE sp2`); err == nil {
		t.Fatal("expected RELEASE sp2 after ROLLBACK TO sp1 to fail")
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sp_items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after ROLLBACK TO sp1, got %d", count)
	}

	mustExec(`INSERT INTO sp_items VALUES (5, 'kept')`)
	mustExec(`RELEASE SAVEPOINT sp1`)
	mustExec(`COMMIT`)

	if err := db.QueryRow(`SELECT COUNT(*) FROM sp_items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 committed rows, got %d", count)
	}
}
