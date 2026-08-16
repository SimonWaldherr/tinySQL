package driver

import (
	"database/sql"
	"reflect"
	"testing"
)

// TestDriverQueryForwardsReturningAndPragmaRows guards the distinction between
// executing a statement and exposing its ResultSet through database/sql. DML
// RETURNING must retain the normal writer/durability path, while read PRAGMAs
// must not be silently turned into empty rows.
func TestDriverQueryForwardsReturningAndPragmaRows(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=query_result_rows")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`CREATE TABLE returned (id INT PRIMARY KEY, name TEXT, qty INT)`); err != nil {
		t.Fatalf("create returned table: %v", err)
	}

	var id, qty int
	var name string
	if err := db.QueryRow(`INSERT INTO returned VALUES (?, ?, ?) RETURNING id, name, qty`, 1, "apple", 3).Scan(&id, &name, &qty); err != nil {
		t.Fatalf("INSERT RETURNING: %v", err)
	}
	if id != 1 || name != "apple" || qty != 3 {
		t.Fatalf("INSERT RETURNING = (%d, %q, %d), want (1, apple, 3)", id, name, qty)
	}

	if err := db.QueryRow(`UPDATE returned SET qty = qty + ? WHERE id = ? RETURNING id, qty`, 2, 1).Scan(&id, &qty); err != nil {
		t.Fatalf("UPDATE RETURNING: %v", err)
	}
	if id != 1 || qty != 5 {
		t.Fatalf("UPDATE RETURNING = (%d, %d), want (1, 5)", id, qty)
	}

	if err := db.QueryRow(`DELETE FROM returned WHERE id = ? RETURNING id, name`, 1).Scan(&id, &name); err != nil {
		t.Fatalf("DELETE RETURNING: %v", err)
	}
	if id != 1 || name != "apple" {
		t.Fatalf("DELETE RETURNING = (%d, %q), want (1, apple)", id, name)
	}

	// QueryContext in an explicit transaction must use the shadow/writer path
	// too; this catches a regression that only works for autocommit RETURNING.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.QueryRow(`INSERT INTO returned VALUES (?, ?, ?) RETURNING id`, 2, "pear", 7).Scan(&id); err != nil {
		_ = tx.Rollback()
		t.Fatalf("transactional INSERT RETURNING: %v", err)
	}
	if id != 2 {
		_ = tx.Rollback()
		t.Fatalf("transactional INSERT RETURNING id = %d, want 2", id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit transactional RETURNING: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE pragma_probe (label TEXT NOT NULL DEFAULT 'general', retries INT DEFAULT 3)`); err != nil {
		t.Fatalf("create pragma probe: %v", err)
	}
	rows, err := db.Query(`PRAGMA table_info(pragma_probe)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info: %v", err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("PRAGMA columns: %v", err)
	}
	if got, want := columns, []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PRAGMA columns = %v, want %v", got, want)
	}
	var names []string
	for rows.Next() {
		var cid, notNull, pk int
		var columnName, declaredType string
		var defaultValue any
		if err := rows.Scan(&cid, &columnName, &declaredType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan PRAGMA row: %v", err)
		}
		names = append(names, columnName)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PRAGMA rows: %v", err)
	}
	if want := []string{"label", "retries"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("PRAGMA table_info names = %v, want %v", names, want)
	}
}

func TestDriverInsertRowsAffected(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=insert_rows_affected")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (id INT PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`INSERT INTO t VALUES (?, ?), (?, ?)`, 1, "one", 2, "two")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := result.RowsAffected(); err != nil || got != 2 {
		t.Fatalf("INSERT RowsAffected() = %d (err %v), want 2", got, err)
	}
}
