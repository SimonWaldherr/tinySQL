package driver

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

// BEGIN no longer copies tables: the transaction shares the live ones and copies
// a table only when it first writes it (storage.SnapshotForTxShared). These tests
// pin what sharing must keep true from the application's side.

func openLazyTxDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", fmt.Sprintf("mem://?tenant=lazytx_%s", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`CREATE TABLE a (id INT PRIMARY KEY, label TEXT, n INT)`,
		`CREATE INDEX a_label ON a (label)`,
		`CREATE TABLE b (id INT PRIMARY KEY, label TEXT)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec(`INSERT INTO a VALUES (?, ?, ?)`, i, fmt.Sprintf("a%d", i), i*10); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO b VALUES (?, ?)`, i, fmt.Sprintf("b%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func lazyCount(t *testing.T, q interface {
	QueryRow(string, ...any) *sql.Row
}, query string, args ...any) int {
	t.Helper()
	var n int
	if err := q.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// A transaction keeps reading what existed at BEGIN, whichever tables other
// connections write afterwards and whether or not it has written anything
// itself.
func TestTxReadsBeginSnapshotWhileOthersCommit(t *testing.T) {
	db := openLazyTxDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	for _, stmt := range []string{
		`INSERT INTO a VALUES (6, 'a6', 60)`,
		`UPDATE a SET n = n + 1000 WHERE id = 1`,
		`DELETE FROM a WHERE id = 2`,
		`INSERT INTO b VALUES (6, 'b6')`,
		`DELETE FROM b`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	if got := lazyCount(t, tx, `SELECT COUNT(*) FROM a`); got != 5 {
		t.Fatalf("a rows inside tx = %d, want the BEGIN-time 5", got)
	}
	if got := lazyCount(t, tx, `SELECT COUNT(*) FROM b`); got != 5 {
		t.Fatalf("b rows inside tx = %d, want the BEGIN-time 5", got)
	}
	if got := lazyCount(t, tx, `SELECT n FROM a WHERE id = 1`); got != 10 {
		t.Fatalf("a.n for id 1 inside tx = %d, want 10", got)
	}
	if got := lazyCount(t, tx, `SELECT COUNT(*) FROM a WHERE label = 'a2'`); got != 1 {
		t.Fatalf("indexed lookup inside tx found %d rows for the deleted a2, want 1", got)
	}
	if got := lazyCount(t, db, `SELECT COUNT(*) FROM a`); got != 5 {
		t.Fatalf("live a rows = %d, want 5 (one added, one deleted)", got)
	}
	if got := lazyCount(t, db, `SELECT COUNT(*) FROM b`); got != 0 {
		t.Fatalf("live b rows = %d, want 0", got)
	}
}

// The same holds once the transaction has written one of the tables: the one it
// copied is its own, the other is still the BEGIN-time version.
func TestTxWritesOneTableWhileOthersChangeBoth(t *testing.T) {
	db := openLazyTxDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`UPDATE a SET n = 0 WHERE id = 3`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE a SET n = 7 WHERE id = 4`); err == nil {
		// The writer is allowed to proceed: the transaction has not committed.
	} else {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO b VALUES (9, 'b9')`); err != nil {
		t.Fatal(err)
	}

	if got := lazyCount(t, tx, `SELECT n FROM a WHERE id = 3`); got != 0 {
		t.Fatalf("tx lost its own write: n = %d", got)
	}
	if got := lazyCount(t, tx, `SELECT n FROM a WHERE id = 4`); got != 40 {
		t.Fatalf("tx saw a concurrent commit to a table it had copied: n = %d, want 40", got)
	}
	if got := lazyCount(t, tx, `SELECT COUNT(*) FROM b`); got != 5 {
		t.Fatalf("tx saw a concurrent insert into a table it shares: %d rows, want 5", got)
	}
	if got := lazyCount(t, db, `SELECT n FROM a WHERE id = 3`); got != 30 {
		t.Fatalf("uncommitted write reached the live table: n = %d", got)
	}
}

// Writing a table the transaction never touched is no conflict; writing one it
// did touch still is.
func TestTxCommitConflictOnlyForTablesItWrote(t *testing.T) {
	db := openLazyTxDB(t)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO a VALUES (10, 'a10', 100)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO b VALUES (10, 'b10')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed although only an untouched table changed: %v", err)
	}
	if got := lazyCount(t, db, `SELECT COUNT(*) FROM a`); got != 6 {
		t.Fatalf("a rows = %d, want 6", got)
	}
	if got := lazyCount(t, db, `SELECT COUNT(*) FROM b`); got != 6 {
		t.Fatalf("b rows = %d, want 6: the commit overwrote a concurrent change to a table it only read", got)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE a SET n = 1 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE a SET n = 2 WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("commit error = %v, want ErrTransactionConflict", err)
	}
}

// The indexes the transaction's copy borrowed and the ones the live table lent
// out both keep enforcing constraints and answering lookups, whichever way the
// transaction ends.
func TestTxEndsWithConsistentIndexes(t *testing.T) {
	for _, outcome := range []string{"commit", "rollback"} {
		t.Run(outcome, func(t *testing.T) {
			db := openLazyTxDB(t)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			for _, stmt := range []string{
				`INSERT INTO a VALUES (20, 'new', 200)`,
				`UPDATE a SET label = 'moved' WHERE id = 1`,
				`DELETE FROM a WHERE id = 5`,
			} {
				if _, err := tx.Exec(stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			if _, err := tx.Exec(`INSERT INTO a VALUES (20, 'dup', 0)`); err == nil {
				t.Fatal("transaction accepted a duplicate primary key against its own insert")
			}
			if _, err := tx.Exec(`INSERT INTO a VALUES (2, 'dup', 0)`); err == nil {
				t.Fatal("transaction accepted a duplicate primary key against a BEGIN-time row")
			}
			if outcome == "commit" {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}

			committed := outcome == "commit"
			wantNew, wantMoved, wantOld5, wantLabel1 := 0, 0, 1, 1
			if committed {
				wantNew, wantMoved, wantOld5, wantLabel1 = 1, 1, 0, 0
			}
			if got := lazyCount(t, db, `SELECT COUNT(*) FROM a WHERE label = 'new'`); got != wantNew {
				t.Fatalf("label 'new': %d rows, want %d", got, wantNew)
			}
			if got := lazyCount(t, db, `SELECT COUNT(*) FROM a WHERE label = 'moved'`); got != wantMoved {
				t.Fatalf("label 'moved': %d rows, want %d", got, wantMoved)
			}
			if got := lazyCount(t, db, `SELECT COUNT(*) FROM a WHERE id = 5`); got != wantOld5 {
				t.Fatalf("id 5: %d rows, want %d", got, wantOld5)
			}
			if got := lazyCount(t, db, `SELECT COUNT(*) FROM a WHERE label = 'a1'`); got != wantLabel1 {
				t.Fatalf("label 'a1': %d rows, want %d", got, wantLabel1)
			}

			// The live table, whichever instance it now is, still enforces its keys
			// and still maintains its indexes for new writes.
			if _, err := db.Exec(`INSERT INTO a VALUES (2, 'dup', 0)`); err == nil {
				t.Fatal("live table accepted a duplicate primary key after the transaction")
			}
			if _, err := db.Exec(`INSERT INTO a VALUES (30, 'later', 300)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE a SET label = 'later2' WHERE id = 30`); err != nil {
				t.Fatal(err)
			}
			if got := lazyCount(t, db, `SELECT COUNT(*) FROM a WHERE label = 'later2'`); got != 1 {
				t.Fatalf("indexed lookup after a post-transaction write found %d rows, want 1", got)
			}
			if got := lazyCount(t, db, `SELECT COUNT(*) FROM a WHERE label = 'later'`); got != 0 {
				t.Fatalf("stale index entry survived an update: %d rows", got)
			}
		})
	}
}

// Statements that are not plain DML on a named table (ANALYZE here) must not
// reach through a shared table into the live database.
func TestTxAnalyzeDoesNotTouchLiveTables(t *testing.T) {
	db := openLazyTxDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`ANALYZE`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := lazyCount(t, db, `SELECT COUNT(*) FROM a`); got != 5 {
		t.Fatalf("a rows = %d after a rolled-back ANALYZE", got)
	}
}

// Every way a transaction can end releases what BEGIN pinned, so later live
// writes are not forced to copy the table on behalf of a transaction that is
// gone.
func TestTxEndReleasesLivePins(t *testing.T) {
	db := openLazyTxDB(t)
	sc, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	pinned := func() int {
		var n int
		if err := sc.Raw(func(dc any) error {
			c := dc.(*conn)
			c.srv.db.LockContentForRead()
			defer c.srv.db.UnlockContentForRead()
			for _, tbl := range c.srv.db.ListTables(c.tenant) {
				if c.srv.db.IsTablePinnedForStream(tbl) {
					n++
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}

	for _, end := range []string{"commit", "rollback", "commit-after-write", "rollback-after-write"} {
		tx, err := sc.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := pinned(); got != 2 {
			t.Fatalf("%s: %d tables pinned during the transaction, want 2", end, got)
		}
		if end == "commit-after-write" || end == "rollback-after-write" {
			if _, err := tx.Exec(`UPDATE a SET n = n + 1`); err != nil {
				t.Fatal(err)
			}
			if got := pinned(); got != 1 {
				t.Fatalf("%s: %d tables pinned after writing one, want 1", end, got)
			}
		}
		if end == "commit" || end == "commit-after-write" {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatalf("%s: %v", end, err)
		}
		if got := pinned(); got != 0 {
			t.Fatalf("%s: %d tables still pinned after the transaction ended", end, got)
		}
	}
}
