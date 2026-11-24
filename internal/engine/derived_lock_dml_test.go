package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The derived-state lock is held by a Sync for the whole of its search-image copy
// (seconds at a million chunks). A statement on a table without constraint
// indexes must not wait for it: it holds the content write lock, so every other
// statement would queue behind it.
func TestDMLOnAKeylessTableDoesNotWaitForTheDerivedLock(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE ch (id INT, src TEXT, load TEXT, loaded_at INT, content TEXT)`)
	execSQL(t, db, `CREATE INDEX ch_src ON ch (src, id)`)
	var values []string
	for i := 0; i < 200; i++ {
		values = append(values, fmt.Sprintf("(%d, 's%d', 'a', 1, 'text %d')", i, i%10, i))
	}
	execSQL(t, db, `INSERT INTO ch VALUES `+strings.Join(values, ","))
	table, err := db.Get("default", "ch")
	if err != nil {
		t.Fatal(err)
	}

	statements := []struct {
		sql  string
		fail bool
	}{
		{sql: `INSERT INTO ch VALUES (1000, 's1', 'b', 1, 'new')`},
		{sql: `UPDATE ch SET loaded_at = 5 WHERE src IN ('s1', 's2') AND COALESCE(loaded_at, 0) < 5`},
		{sql: `UPDATE ch SET content = 'changed' WHERE id = 7`},
		{sql: `DELETE FROM ch WHERE src = 's3' AND load <> 'b'`},
		{sql: `DELETE FROM ch WHERE src IN ('s4', 's5')`},
		{sql: `DELETE FROM ch WHERE id = -1`},
		// A failing statement rolls back through the statement snapshot.
		{sql: `UPDATE ch SET loaded_at = 1 / (id - id) WHERE src = 's6'`, fail: true},
		{sql: `DELETE FROM ch WHERE 1 / (id - id) = 1`, fail: true},
	}
	table.DerivedLock() // what a Sync's image copy holds
	locked := true
	unlock := func() {
		if locked {
			locked = false
			table.DerivedUnlock()
		}
	}
	defer unlock()
	done := make(chan error, 1)
	go func() {
		for _, st := range statements {
			_, err := tryExec(db, st.sql)
			if (err != nil) != st.fail {
				done <- fmt.Errorf("%s: error %v, want failure %v", st.sql, err, st.fail)
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		unlock()
		t.Fatal("a statement waited for the derived-state lock")
	}
}

// A statement on a table WITH a constraint index still takes the lock (its
// cache lives behind it); this documents that the fast path does not skip it.
func TestDMLOnAConstrainedTableStillSeesItsConstraintIndex(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE k (id INT PRIMARY KEY, v INT)`)
	execSQL(t, db, `INSERT INTO k VALUES (1, 1), (2, 2), (3, 3)`)
	execSQL(t, db, `INSERT INTO k VALUES (4, 4)`) // builds the constraint index
	execSQL(t, db, `UPDATE k SET id = 9 WHERE id = 1`)
	if _, err := tryExec(db, `INSERT INTO k VALUES (9, 0)`); err == nil {
		t.Fatal("the constraint index missed the UPDATE of the key")
	}
	execSQL(t, db, `DELETE FROM k WHERE v = 1`)
	if _, err := tryExec(db, `INSERT INTO k VALUES (9, 0)`); err != nil {
		t.Fatalf("the constraint index kept a deleted key: %v", err)
	}
}
