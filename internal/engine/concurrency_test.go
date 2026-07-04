// Tests for Execute's content lock (internal/engine/exec.go, storage.DB's
// contentMu in internal/storage/db.go). Before this, INSERT/UPDATE/DELETE
// mutated Table.Rows with no lock at all, so two goroutines calling Execute
// concurrently on the same table raced on that slice — a real data race
// (concurrent unsynchronized append + range), not just a stale-read risk.
//
// runWithTimeout guards the deadlock-prone cases: CREATE TABLE AS SELECT,
// materialized view refresh, and trigger bodies all recursively dispatch a
// nested statement on the same goroutine that holds Execute's write lock.
// If that recursion were wired through Execute instead of the unlocked
// execStmt, it would self-deadlock (sync.RWMutex is not reentrant) and hang
// forever instead of failing — so these tests enforce a hard timeout rather
// than relying on the test framework's own timeout to eventually kill it.
package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// runWithTimeout runs fn on its own goroutine and fails if it doesn't finish
// within timeout. fn must not call t.Fatal/t.Fatalf itself — those call
// runtime.Goexit() and are only valid from the goroutine running the test,
// so calling them from fn's goroutine would silently abort fn without
// closing done, misreporting a real error (e.g. a SQL parse failure) as a
// timeout/deadlock. Return the error instead and let the caller fail on the
// main test goroutine.
func runWithTimeout(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fn failed: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("timed out after %s — likely a self-deadlock on DB's content lock", timeout)
	}
}

// TestConcurrentInsertAndSelectDoesNotPanic hammers a single table with
// concurrent INSERTs and SELECTs. Before the content lock, this reliably
// panicked or produced corrupted results under `go test -race`-style
// scheduling perturbation (append() reallocating Table.Rows' backing array
// while a SELECT ranged over it). With the lock in place, every SELECT sees
// a consistent snapshot and every INSERT lands cleanly.
func TestConcurrentInsertAndSelectDoesNotPanic(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT, val INT)`)

	const writers = 8
	const rowsPerWriter = 50
	const readers = 8
	const readIters = 100

	var wg sync.WaitGroup
	errCh := make(chan error, writers+readers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < rowsPerWriter; i++ {
				stmt, err := NewParser(fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", w*rowsPerWriter+i, i)).ParseStatement()
				if err != nil {
					errCh <- err
					return
				}
				if _, err := Execute(ctx, db, "default", stmt); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < readIters; i++ {
				stmt, err := NewParser("SELECT COUNT(*) AS cnt FROM t").ParseStatement()
				if err != nil {
					errCh <- err
					return
				}
				if _, err := Execute(ctx, db, "default", stmt); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent Execute failed: %v", err)
	}

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM t`)
	got := expectAsInt(t, rs.Rows[0]["cnt"])
	if want := writers * rowsPerWriter; got != want {
		t.Errorf("expected %d rows after concurrent inserts, got %d", want, got)
	}
}

// TestConcurrentUpdateDeleteDoesNotPanic exercises the write/write and
// write/read interleavings for UPDATE and DELETE, not just INSERT.
func TestConcurrentUpdateDeleteDoesNotPanic(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT, val INT)`)
	for i := 0; i < 200; i++ {
		execSQL(t, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 0)", i))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < 25; i++ {
				id := g*25 + i
				stmt, err := NewParser(fmt.Sprintf("UPDATE t SET val = %d WHERE id = %d", id, id)).ParseStatement()
				if err != nil {
					errCh <- err
					return
				}
				if _, err := Execute(ctx, db, "default", stmt); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < 50; i++ {
				stmt, err := NewParser("SELECT * FROM t WHERE val > -1").ParseStatement()
				if err != nil {
					errCh <- err
					return
				}
				if _, err := Execute(ctx, db, "default", stmt); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent Execute failed: %v", err)
	}

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM t`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 200 {
		t.Errorf("expected 200 rows to survive, got %d", got)
	}
}

// TestTriggerBodyDoesNotDeadlock guards the fireTriggers -> execStmt
// recursion path (internal/engine/triggers.go). If that were wired through
// Execute instead of execStmt, an INSERT into a triggering table would try
// to re-acquire the already-held write lock on the same goroutine and hang
// forever.
func TestTriggerBodyDoesNotDeadlock(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE orders (id INT, total FLOAT64)`)
	execSQL(t, db, `CREATE TABLE audit_log (order_id INT, msg TEXT)`)
	execSQL(t, db, `
		CREATE TRIGGER orders_after_insert
		AFTER INSERT ON orders
		FOR EACH ROW
		BEGIN
			INSERT INTO audit_log VALUES (NEW.id, 'created');
		END
	`)

	runWithTimeout(t, 5*time.Second, func() error {
		stmt, err := NewParser(`INSERT INTO orders VALUES (1, 99.99)`).ParseStatement()
		if err != nil {
			return err
		}
		_, err = Execute(context.Background(), db, "default", stmt)
		return err
	})

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM audit_log`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 1 {
		t.Errorf("expected trigger to insert 1 audit row, got %d", got)
	}
}

// TestCreateTableAsSelectDoesNotDeadlock guards the executeCreateTable ->
// execStmt recursion for "CREATE TABLE ... AS SELECT ...".
func TestCreateTableAsSelectDoesNotDeadlock(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE src (id INT, val INT)`)
	execSQL(t, db, `INSERT INTO src VALUES (1, 10)`)
	execSQL(t, db, `INSERT INTO src VALUES (2, 20)`)

	runWithTimeout(t, 5*time.Second, func() error {
		stmt, err := NewParser(`CREATE TABLE dst AS SELECT * FROM src`).ParseStatement()
		if err != nil {
			return err
		}
		_, err = Execute(context.Background(), db, "default", stmt)
		return err
	})

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM dst`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 2 {
		t.Errorf("expected CTAS to copy 2 rows, got %d", got)
	}
}
