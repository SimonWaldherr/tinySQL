// Regression coverage for a gap found while fixing the read/write blocking
// issue in server.mu/writeMu: BeginTx's SnapshotForTx/DeepClone (conn.go)
// and commitTx's conflict-detection/ApplyWALChanges/AdoptCatalog
// (tx.go's commitTxApply) read and mutate the live *storage.DB's table
// content directly, bypassing engine.Execute entirely -- so unlike an
// ordinary statement, they never took storage.DB.contentMu. That matters
// specifically because internal/storage/scheduler.go's job executor
// (SQLJobExecutor.ExecuteSQL, tinysql.go) calls engine.Execute directly on
// a shared *storage.DB when an application uses SetDefaultDB +
// StartJobScheduler (cmd/studio, cmd/accessweb, deployment.go) -- bypassing
// server.mu/writeMu entirely and relying solely on contentMu. Before this
// fix, a scheduled job's write and a driver connection's BeginTx/commitTx
// on the same live database had no lock in common at all.
//
// This machine has no cgo, so `go test -race` cannot run locally (see the
// windows-dev-gotchas memory) -- this is a best-effort empirical stress
// check pending CI's race detector as the real gate, not a proof by itself.
package driver

import (
	"context"
	stdDriver "database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestBeginCommitTxNoRaceWithDirectSchedulerLikeExecute(t *testing.T) {
	db := storage.NewDB()
	srv := newServer(db, cfg{tenant: "default"})
	setup := &conn{srv: srv, tenant: "default"}
	ctx := context.Background()

	if _, err := setup.execSQL(ctx, "CREATE TABLE shared_items (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const schedulerIterations = 300
	const txIterations = 150
	var wg sync.WaitGroup
	errs := make(chan error, schedulerIterations+txIterations)

	// Simulates SQLJobExecutor.ExecuteSQL (tinysql.go): direct engine.Execute
	// against the shared *storage.DB, exactly what StartJobScheduler +
	// SetDefaultDB produces -- never touching server.mu/writeMu.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < schedulerIterations; i++ {
			stmt, err := engine.NewParser(
				fmt.Sprintf("INSERT INTO shared_items (id, name) VALUES (%d, 'scheduled-%d')", 100000+i, i),
			).ParseStatement()
			if err != nil {
				errs <- err
				return
			}
			if _, err := engine.Execute(ctx, db, "default", stmt); err != nil {
				errs <- err
				return
			}
		}
	}()

	// Concurrently drive BeginTx/INSERT/COMMIT through the driver on the
	// same shared db, from a second connection. The scheduler-like writer
	// above touches the same table throughout, so detectTxConflicts
	// (tx.go) is *expected* to reject some commits with
	// ErrTransactionConflict -- that is correct optimistic-concurrency
	// behavior, not a race, and a real caller retries; this loop does the
	// same. What must never happen is any other error, or a crash.
	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := &conn{srv: srv, tenant: "default"}
		for i := 0; i < txIterations; i++ {
			for {
				if _, err := writer.execSQL(ctx, "BEGIN"); err != nil {
					errs <- fmt.Errorf("begin %d: %w", i, err)
					return
				}
				query := fmt.Sprintf("INSERT INTO shared_items (id, name) VALUES (%d, 'tx-%d')", i, i)
				if _, err := writer.execSQL(ctx, query); err != nil {
					errs <- fmt.Errorf("tx insert %d: %w", i, err)
					return
				}
				_, err := writer.execSQL(ctx, "COMMIT")
				if err == nil {
					break
				}
				if errors.Is(err, ErrTransactionConflict) {
					continue // retry with a fresh BEGIN, as a real caller would
				}
				errs <- fmt.Errorf("commit %d: %w", i, err)
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every row from both sides should have landed exactly once: no torn
	// read/write should have silently dropped or duplicated a row.
	reader := &conn{srv: srv, tenant: "default"}
	rows, err := reader.querySQL(ctx, "SELECT COUNT(*) AS n FROM shared_items")
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	defer rows.Close()
	dest := make([]stdDriver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("count scan: %v", err)
	}
	var got int64
	switch n := dest[0].(type) {
	case int64:
		got = n
	case int:
		got = int64(n)
	case float64:
		got = int64(n)
	default:
		t.Fatalf("unexpected COUNT(*) result type %T (%v)", dest[0], dest[0])
	}
	want := int64(schedulerIterations + txIterations)
	if got != want {
		t.Fatalf("row count = %d, want %d (scheduler-like writes and BeginTx/COMMIT writes raced)", got, want)
	}
}
