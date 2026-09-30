package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

// A transaction's shadow shares immutable rows with the live table instead of
// copying them (see storage.cloneRowsShared). These tests pin the isolation
// guarantees that sharing must not weaken: nothing a transaction does to its
// shadow may reach the live table, and nothing the live table does afterwards
// may reach the transaction's snapshot.

func openIsolationDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", fmt.Sprintf("mem://?tenant=tx_iso_%s", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`CREATE TABLE t (id INT PRIMARY KEY, n INT, label TEXT, meta JSON)`,
		`INSERT INTO t VALUES (1, 10, 'a', '{"k":"v1"}')`,
		`INSERT INTO t VALUES (2, 20, 'b', '{"k":"v2"}')`,
		`INSERT INTO t VALUES (3, 30, 'c', '{"k":"v3"}')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return db
}

type isoRow struct {
	id, n int
	label string
	k     string
}

func readIsoRows(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) []isoRow {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), `SELECT id, n, label, JSON_GET(meta, 'k') FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []isoRow
	for rows.Next() {
		var r isoRow
		if err := rows.Scan(&r.id, &r.n, &r.label, &r.k); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sameIsoRows(a, b []isoRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func columnCount(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) int {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), `SELECT * FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return len(cols)
}

func TestTxRollbackLeavesLiveRowsUntouched(t *testing.T) {
	db := openIsolationDB(t)
	want := readIsoRows(t, db)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`UPDATE t SET n = n + 100`,
		`UPDATE t SET meta = JSON_SET(meta, 'k', 'changed')`,
		`UPDATE t SET label = 'zzz' WHERE id = 2`,
		`ALTER TABLE t ADD COLUMN extra INT`,
		`INSERT INTO t (id, n, label, meta) VALUES (4, 40, 'd', '{"k":"v4"}')`,
		`DELETE FROM t WHERE id = 1`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if inTx := readIsoRows(t, tx); sameIsoRows(inTx, want) {
		t.Fatalf("transaction did not see its own writes: %v", inTx)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if got := readIsoRows(t, db); !sameIsoRows(got, want) {
		t.Fatalf("rollback leaked into live rows:\n got %v\nwant %v", got, want)
	}
	if n := columnCount(t, db); n != 4 {
		t.Fatalf("rolled-back ALTER TABLE widened live rows: %d columns, want 4", n)
	}
}

func TestTxSnapshotIgnoresConcurrentLiveWrites(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("readOnly=%v", readOnly), func(t *testing.T) {
			db := openIsolationDB(t)
			ctx := context.Background()
			want := readIsoRows(t, db)

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
			if err != nil {
				t.Fatal(err)
			}

			// Another connection rewrites the live table after the snapshot.
			for _, stmt := range []string{
				`UPDATE t SET n = n + 1000`,
				`UPDATE t SET meta = JSON_SET(meta, 'k', 'live')`,
				`ALTER TABLE t ADD COLUMN extra INT`,
				`INSERT INTO t (id, n, label, meta) VALUES (9, 90, 'z', '{"k":"v9"}')`,
				`DELETE FROM t WHERE id = 1`,
			} {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}

			if got := readIsoRows(t, tx); !sameIsoRows(got, want) {
				t.Fatalf("snapshot saw later live writes:\n got %v\nwant %v", got, want)
			}
			if n := columnCount(t, tx); n != 4 {
				t.Fatalf("snapshot saw a column added after BEGIN: %d columns, want 4", n)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTxCommitPublishesWritesAndLaterTxStayIsolated(t *testing.T) {
	db := openIsolationDB(t)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`UPDATE t SET n = 99 WHERE id = 2`,
		`UPDATE t SET meta = JSON_SET(meta, 'k', 'committed') WHERE id = 3`,
		`INSERT INTO t (id, n, label, meta) VALUES (4, 40, 'd', '{"k":"v4"}')`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	committed := []isoRow{{1, 10, "a", "v1"}, {2, 99, "b", "v2"}, {3, 30, "c", "committed"}, {4, 40, "d", "v4"}}
	if got := readIsoRows(t, db); !sameIsoRows(got, committed) {
		t.Fatalf("commit result:\n got %v\nwant %v", got, committed)
	}

	// The committed table was the shadow itself; a later rolled-back
	// transaction must still not be able to disturb it.
	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`UPDATE t SET n = 0`,
		`UPDATE t SET meta = JSON_SET(meta, 'k', 'gone')`,
		`DELETE FROM t WHERE id = 4`,
	} {
		if _, err := tx2.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := readIsoRows(t, db); !sameIsoRows(got, committed) {
		t.Fatalf("second rollback leaked:\n got %v\nwant %v", got, committed)
	}
}

func TestTxWriteConflictDetectionUnchangedBySharedRows(t *testing.T) {
	db := openIsolationDB(t)
	ctx := context.Background()
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	tx1, err := c1.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := c2.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx1.Exec(`UPDATE t SET n = 111 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(`UPDATE t SET n = 222 WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("second commit = %v, want ErrTransactionConflict", err)
	}
	got := readIsoRows(t, db)
	want := []isoRow{{1, 111, "a", "v1"}, {2, 20, "b", "v2"}, {3, 30, "c", "v3"}}
	if !sameIsoRows(got, want) {
		t.Fatalf("after conflict:\n got %v\nwant %v", got, want)
	}
}
