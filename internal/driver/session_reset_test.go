// Regression tests for transaction state outliving a pooled connection.
//
// database/sql only knows about a transaction started through BeginTx. An
// application that runs Exec("BEGIN") — which the driver accepts, and which the
// README documents — starts one the pool cannot see, so the connection goes
// back into the pool still inside it. Every later write handed that connection
// then ran against the abandoned shadow database and was silently discarded:
// no error, no rows, and no other connection could see any of it. The
// connection stayed poisoned for the life of the process.
//
// conn.ResetSession (driver.SessionResetter) and conn.Close now roll back an
// abandoned transaction, so a recycled connection starts clean.
package driver

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// TestAbandonedSQLBeginDoesNotPoisonPooledConnection is the core case: BEGIN
// without COMMIT, then the connection is returned to the pool and reused.
func TestAbandonedSQLBeginDoesNotPoisonPooledConnection(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// One connection, so the next query is guaranteed to reuse the same one.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}

	// Take a connection, start a transaction through SQL, and hand the
	// connection back without committing.
	func() {
		c, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := c.ExecContext(context.Background(), `BEGIN`); err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := c.ExecContext(context.Background(), `INSERT INTO t VALUES (1)`); err != nil {
			t.Fatalf("insert in abandoned tx: %v", err)
		}
	}()

	// The abandoned transaction was never committed, so its row must not be
	// visible — and, more importantly, the recycled connection must be usable.
	if _, err := db.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("insert after the abandoned transaction: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (only the committed insert); "+
			"a count of 0 means the write after the abandoned transaction was discarded", count)
	}
	var id int
	if err := db.QueryRow(`SELECT id FROM t`).Scan(&id); err != nil {
		t.Fatalf("select id: %v", err)
	}
	if id != 2 {
		t.Errorf("surviving row id = %d, want 2 (the rolled-back row must not survive)", id)
	}
}

// TestAbandonedTxIsRolledBackOnConnClose covers the same leak on the close path
// rather than the reuse path, and checks it against durable storage.
func TestAbandonedTxIsRolledBackOnConnClose(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "abandoned") + "?tenant=default&mode=wal"
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}

	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(context.Background(), `BEGIN`); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), `INSERT INTO t VALUES (7)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var count int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&count); err != nil {
		t.Fatalf("count after restart: %v", err)
	}
	if count != 0 {
		t.Errorf("%d row(s) from an abandoned transaction survived the restart, want 0", count)
	}
}

// TestBeginTxStillWorksAfterAbandonedSQLBegin checks that cleaning up the
// abandoned transaction leaves the connection able to start a real one.
func TestBeginTxStillWorksAfterAbandonedSQLBegin(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}

	func() {
		c, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := c.ExecContext(context.Background(), `BEGIN`); err != nil {
			t.Fatalf("begin: %v", err)
		}
	}()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BeginTx on the recycled connection: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (3)`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert in real tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1", count)
	}
}
