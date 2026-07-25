// Regression tests for catalog visibility inside explicit transactions.
//
// A transaction runs against a private shadow database. That shadow used to be
// built by storage.SnapshotForTx as a bare NewDB(), which starts with an empty
// CatalogManager, so inside BEGIN…COMMIT the transaction saw no views and no
// triggers: selecting from a view failed with "no such table", and AFTER/BEFORE
// triggers silently did not fire, leaving trigger-maintained tables quietly
// wrong. Catalog objects a transaction created were also discarded at COMMIT,
// because only table changes were merged.
//
// Constraint enforcement (FOREIGN KEY, PRIMARY KEY) is checked here too: it
// lives on the table schema rather than in the catalog, so it was unaffected,
// and these tests keep it that way.
package driver

import (
	"database/sql"
	"testing"
)

func TestTxViewVisibleInsideTx(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		`CREATE TABLE t (id INT, val TEXT)`,
		`INSERT INTO t VALUES (1, 'a')`,
		`CREATE VIEW v AS SELECT id FROM t`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM v`).Scan(&n); err != nil {
		t.Fatalf("view outside tx: %v", err)
	}
	t.Logf("outside tx: view rows = %d", n)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var m int
	if err := tx.QueryRow(`SELECT COUNT(*) AS c FROM v`).Scan(&m); err != nil {
		t.Fatalf("regression: view NOT visible inside transaction: %v", err)
	}
	t.Logf("inside tx: view rows = %d", m)
}

func TestTxTriggerFiresInsideTx(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		`CREATE TABLE t (id INT)`,
		`CREATE TABLE audit (id INT)`,
		`CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM audit`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	t.Logf("autocommit insert -> audit rows = %d (want 1)", n)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var m int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM audit`).Scan(&m); err != nil {
		t.Fatal(err)
	}
	if m != 2 {
		t.Errorf("regression: trigger did not fire inside transaction: audit rows = %d, want 2", m)
	}
}

func TestTxCreateViewInsideTxSurvivesCommit(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE VIEW v2 AS SELECT id FROM t`); err != nil {
		t.Fatalf("create view in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM v2`).Scan(&n); err != nil {
		t.Errorf("regression: view created inside a committed transaction is lost: %v", err)
	} else {
		t.Logf("view rows after commit = %d", n)
	}
}

// FK enforcement is schema-level, so it worked inside a transaction all along.
func TestTxForeignKeyInsideTx(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		`CREATE TABLE parent (id INT PRIMARY KEY)`,
		`CREATE TABLE child (id INT, pid INT REFERENCES parent(id))`,
		`INSERT INTO parent VALUES (1)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO child VALUES (1, 99)`); err == nil {
		t.Error("autocommit: FK violation accepted (want error)")
	} else {
		t.Logf("autocommit FK violation rejected: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO child VALUES (2, 99)`); err == nil {
		t.Error("regression: FK violation accepted inside a transaction (want error)")
	} else {
		t.Logf("in-tx FK violation rejected: %v", err)
	}
}

// PRIMARY KEY / UNIQUE enforcement is likewise schema-level.
func TestTxUniqueInsideTx(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		`CREATE TABLE u (id INT PRIMARY KEY, name TEXT)`,
		`INSERT INTO u VALUES (1, 'a')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO u VALUES (1, 'dup')`); err == nil {
		t.Error("regression: duplicate primary key accepted inside a transaction (want error)")
	} else {
		t.Logf("in-tx PK violation rejected: %v", err)
	}
}
