package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestDriverModeWALNormalSyncPersistsAndReopens proves that the opt-in
// ordinary-fsync policy remains a synchronous, recoverable WAL mode. The
// large checkpoint threshold keeps the data in the log, exercising replay on
// reopen rather than only checkpoint persistence.
func TestDriverModeWALNormalSyncPersistsAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "normal")
	dsn := "file:" + path + "?tenant=default&mode=wal&wal_sync=normal&checkpoint_every=1000000&checkpoint_interval=1h&checkpoint_max_bytes=-1"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)`); err != nil {
		_ = db.Close()
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO accounts VALUES (?, ?)`, 1, 10); err != nil {
		_ = db.Close()
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(`UPDATE accounts SET balance = ? WHERE id = ?`, 25, 1); err != nil {
		_ = db.Close()
		t.Fatalf("update: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	var balance int
	if err := reopened.QueryRow(`SELECT balance FROM accounts WHERE id = ?`, 1).Scan(&balance); err != nil {
		t.Fatalf("query after recovery: %v", err)
	}
	if balance != 25 {
		t.Fatalf("recovered balance = %d, want 25", balance)
	}
}
