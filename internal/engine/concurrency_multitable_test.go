// Concurrency coverage for writes that target *different* tables and
// different tenants.
//
// concurrency_test.go already hammers a single table, which is what the
// content lock was added for. These tests cover the other half: state that is
// shared across tables — the constraint-value index, the catalog, the
// statement rollback point — where a bug shows up as a data race or as one
// statement clobbering another's committed work rather than as a torn row
// slice. They are cheap here and are the cases the race detector in CI
// (.github/workflows/ci.yml runs `go test ./... -race`) has to see, since
// `-race` needs cgo and cannot run on every developer machine.
package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestConcurrentWritesToDistinctTables drives constrained INSERTs and point
// UPDATEs against eight separate tables at once. The per-table constraint
// index each of them maintains used to live in one process-global map behind
// one mutex; this is the test that fails, under -race, if that state ever goes
// back to being shared without synchronization.
func TestConcurrentWritesToDistinctTables(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	const tables = 8
	const rowsPerTable = 40

	for i := 0; i < tables; i++ {
		execSQL(t, db, fmt.Sprintf(`CREATE TABLE t%d (id INT PRIMARY KEY, val INT)`, i))
	}

	var wg sync.WaitGroup
	errs := make(chan error, tables*2)
	for i := 0; i < tables; i++ {
		wg.Add(1)
		go func(table int) {
			defer wg.Done()
			for r := 0; r < rowsPerTable; r++ {
				stmt := mustParse(fmt.Sprintf(`INSERT INTO t%d VALUES (%d, %d)`, table, r, r))
				if _, err := Execute(ctx, db, "default", stmt); err != nil {
					errs <- fmt.Errorf("insert t%d row %d: %w", table, r, err)
					return
				}
				stmt = mustParse(fmt.Sprintf(`UPDATE t%d SET val = %d WHERE id = %d`, table, r*2, r))
				if _, err := Execute(ctx, db, "default", stmt); err != nil {
					errs <- fmt.Errorf("update t%d row %d: %w", table, r, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for i := 0; i < tables; i++ {
		table, err := db.Get("default", fmt.Sprintf("t%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if len(table.Rows) != rowsPerTable {
			t.Fatalf("t%d has %d rows, want %d", i, len(table.Rows), rowsPerTable)
		}
	}
}

// TestConcurrentWritesToDistinctTenants is the same shape with full tenant
// isolation, which additionally exercises the tenant map and the per-statement
// foreign-key probe that walks it.
func TestConcurrentWritesToDistinctTenants(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	const tenants = 8
	const rows = 30

	for i := 0; i < tenants; i++ {
		tenant := fmt.Sprintf("tenant%d", i)
		if _, err := Execute(ctx, db, tenant, mustParse(`CREATE TABLE t (id INT PRIMARY KEY, val INT)`)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, tenants)
	for i := 0; i < tenants; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tenant := fmt.Sprintf("tenant%d", n)
			for r := 0; r < rows; r++ {
				if _, err := Execute(ctx, db, tenant, mustParse(
					fmt.Sprintf(`INSERT INTO t VALUES (%d, %d)`, r, r))); err != nil {
					errs <- fmt.Errorf("%s insert %d: %w", tenant, r, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for i := 0; i < tenants; i++ {
		table, err := db.Get(fmt.Sprintf("tenant%d", i), "t")
		if err != nil {
			t.Fatal(err)
		}
		if len(table.Rows) != rows {
			t.Fatalf("tenant%d has %d rows, want %d", i, len(table.Rows), rows)
		}
	}
}

// TestFailingStatementsDoNotDisturbConcurrentSuccesses is the rollback race:
// half the goroutines run INSERTs that violate a PRIMARY KEY and therefore
// roll back, while the other half write different tables successfully. A
// rollback that reached beyond its own statement — reinstating a stale catalog
// over a concurrent statement's committed one, say — shows up here as lost
// rows or a lost view.
func TestFailingStatementsDoNotDisturbConcurrentSuccesses(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	const pairs = 4
	const iterations = 25

	for i := 0; i < pairs; i++ {
		execSQL(t, db, fmt.Sprintf(`CREATE TABLE fail%d (id INT PRIMARY KEY)`, i))
		execSQL(t, db, fmt.Sprintf(`INSERT INTO fail%d VALUES (1)`, i))
		execSQL(t, db, fmt.Sprintf(`CREATE TABLE ok%d (id INT PRIMARY KEY)`, i))
	}
	execSQL(t, db, `CREATE VIEW committed_view AS SELECT 1 AS one`)

	var wg sync.WaitGroup
	errs := make(chan error, pairs*2)
	for i := 0; i < pairs; i++ {
		wg.Add(1)
		go func(n int) { // always fails: duplicate primary key
			defer wg.Done()
			stmt := mustParse(fmt.Sprintf(`INSERT INTO fail%d VALUES (1)`, n))
			for r := 0; r < iterations; r++ {
				if _, err := Execute(ctx, db, "default", stmt); err == nil {
					errs <- fmt.Errorf("fail%d: duplicate primary key was accepted", n)
					return
				}
			}
		}(i)
		wg.Add(1)
		go func(n int) { // always succeeds
			defer wg.Done()
			for r := 0; r < iterations; r++ {
				if _, err := Execute(ctx, db, "default", mustParse(
					fmt.Sprintf(`INSERT INTO ok%d VALUES (%d)`, n, r))); err != nil {
					errs <- fmt.Errorf("ok%d row %d: %w", n, r, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for i := 0; i < pairs; i++ {
		failed, err := db.Get("default", fmt.Sprintf("fail%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if len(failed.Rows) != 1 {
			t.Fatalf("fail%d has %d rows after %d rejected inserts, want 1", i, len(failed.Rows), iterations)
		}
		ok, err := db.Get("default", fmt.Sprintf("ok%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if len(ok.Rows) != iterations {
			t.Fatalf("ok%d has %d rows, want %d", i, len(ok.Rows), iterations)
		}
	}
	if _, found := db.Catalog().GetView("main", "committed_view"); !found {
		t.Fatal("a rolled-back statement discarded a view committed before it ran")
	}
}

// TestRollbackPurgesOnlyItsOwnTablesCaches pins the scoping of the post-
// rollback cache purge. A statement whose rollback point covers one table
// cannot have written any other, so dropping every table's derived state was
// both wasted work proportional to the database and a way for one caller's
// rejected INSERT to slow down every other caller in the process.
func TestRollbackPurgesOnlyItsOwnTablesCaches(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execSQL(t, db, `CREATE TABLE target (id INT PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE bystander (id INT PRIMARY KEY)`)
	execSQL(t, db, `INSERT INTO target VALUES (1)`)
	execSQL(t, db, `INSERT INTO bystander VALUES (1), (2), (3)`)

	bystander, err := db.Get("default", "bystander")
	if err != nil {
		t.Fatal(err)
	}
	// Warm the bystander's constraint index by asking it a question.
	if idx := getConstraintIndex(bystander, 0); idx == nil {
		t.Fatal("could not warm the bystander's constraint index")
	}

	// A statement that fails on an unrelated table.
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO target VALUES (1)`)); err == nil {
		t.Fatal("duplicate primary key was accepted")
	}

	if idx := currentConstraintIndex(bystander, 0); idx == nil {
		t.Fatal("a failed statement on `target` dropped `bystander`'s constraint index")
	}
}
