package driver

import (
	"database/sql"
	"strings"
	"testing"
)

func TestDriverCachedInserts(t *testing.T) {
	ctx := t.Context()
	co, err := (&drv{}).OpenConnector("mem://?tenant=cached_inserts")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(co)
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TABLE documents (id INT PRIMARY KEY, body TEXT)`); err != nil {
		t.Fatal(err)
	}
	srv := co.(*connector).srv
	initialSyncs := srv.persistSyncCount

	checkAffected := func(result sql.Result, err error, want int64) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		got, err := result.RowsAffected()
		if err != nil || got != want {
			t.Fatalf("RowsAffected() = %d, %v; want %d", got, err, want)
		}
	}
	checkSyncs := func(want uint64) {
		t.Helper()
		srv.persistMu.Lock()
		got := srv.persistSyncCount
		srv.persistMu.Unlock()
		if got != want {
			t.Fatalf("syncs = %d, want %d", got, want)
		}
	}
	checkRows := func(want int) {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`).Scan(&n); err != nil || n != want {
			t.Fatalf("visible rows = %d, %v; want %d", n, err, want)
		}
	}

	stmt, err := db.PrepareContext(ctx, `INSERT CACHED INTO documents VALUES (?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	result, err := stmt.ExecContext(ctx, 1, "first")
	checkAffected(result, err, 1)
	result, err = stmt.ExecContext(ctx, 2, "second")
	checkAffected(result, err, 1)
	checkRows(0)
	checkSyncs(initialSyncs)

	result, err = db.ExecContext(ctx, `FLUSH INSERTS INTO documents`)
	checkAffected(result, err, 2)
	checkRows(2)
	checkSyncs(initialSyncs + 1)
	var body string
	if err := db.QueryRowContext(ctx, `SELECT body FROM documents WHERE id = 1`).Scan(&body); err != nil || body != "first" {
		t.Fatalf("first staged body = %q, %v", body, err)
	}

	result, err = db.ExecContext(ctx, `INSERT CACHED INTO documents SELECT ?, ?`, 3, "discarded")
	checkAffected(result, err, 1)
	result, err = db.ExecContext(ctx, `DISCARD INSERTS FROM documents`)
	checkAffected(result, err, 1)
	checkSyncs(initialSyncs + 1)
	result, err = db.ExecContext(ctx, `FLUSH INSERTS documents`)
	checkAffected(result, err, 0)
	checkRows(2)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, query := range []string{
		`INSERT CACHED INTO documents VALUES (4, 'transaction')`,
		`FLUSH INSERTS INTO documents`,
		`DISCARD INSERTS FROM documents`,
	} {
		if _, err := tx.ExecContext(ctx, query); err == nil || !strings.Contains(err.Error(), "require autocommit") {
			t.Errorf("transactional %q error = %v, want autocommit error", query, err)
		}
	}
}

func TestCachedInsertsFollowTransactionalTableLifetime(t *testing.T) {
	ctx := t.Context()
	co, err := (&drv{}).OpenConnector("mem://?tenant=cached_lifetime")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(co)
	defer db.Close()
	exec := func(query string) sql.Result {
		t.Helper()
		rs, err := db.ExecContext(ctx, query)
		if err != nil {
			t.Fatal(query, err)
		}
		return rs
	}
	transact := func(commit bool, queries ...string) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for _, query := range queries {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				t.Fatal(query, err)
			}
		}
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
	}
	checkFlush := func(want int64) {
		t.Helper()
		n, err := exec(`FLUSH INSERTS docs`).RowsAffected()
		if err != nil || n != want {
			t.Fatalf("flush inserted %d rows, want %d, err %v", n, want, err)
		}
	}
	exec(`CREATE TABLE docs (id INT PRIMARY KEY)`)
	exec(`INSERT CACHED INTO docs VALUES (10)`)
	transact(true, `INSERT INTO docs VALUES (1)`)
	transact(false, `DROP TABLE docs`)
	checkFlush(1)
	exec(`INSERT CACHED INTO docs VALUES (11)`)
	transact(true, `DROP TABLE docs`, `CREATE TABLE docs (id INT PRIMARY KEY)`, `INSERT INTO docs VALUES (2)`, `INSERT INTO docs VALUES (3)`)
	checkFlush(0)
	var min, max int
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM docs`).Scan(&min, &max); err != nil || min != 2 || max != 3 {
		t.Fatalf("replacement rows from %d to %d, err %v", min, max, err)
	}
}
