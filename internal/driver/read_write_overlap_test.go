// Regression coverage for the write/read overlap fix: server.mu.Lock() used
// to wrap a write's persist() (or, for commitTx, its WAL checkpoint) as well
// as its mutation, so every SELECT was blocked for a write's full durability
// I/O, not just the in-memory step that actually needs exclusivity. See
// server.writeMu's doc comment (server.go) and commitTxApply's (tx.go).
package driver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestAutocommitWriteDoesNotBlockConcurrentReadDuringPersist proves the fix
// with a timing assertion, not just an absence-of-error check: a read
// started while a write is inside its (artificially slow) persist phase
// must complete in a small fraction of that delay. Before the fix this read
// would have blocked on server.mu.RLock() for the whole delay.
func TestAutocommitWriteDoesNotBlockConcurrentReadDuringPersist(t *testing.T) {
	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	writer := &conn{srv: srv, tenant: "default"}
	reader := &conn{srv: srv, tenant: "default"}
	ctx := context.Background()

	if _, err := writer.execSQL(ctx, "CREATE TABLE items (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := writer.execSQL(ctx, "INSERT INTO items (id, name) VALUES (0, 'seed')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	const persistDelay = 200 * time.Millisecond
	persisting := make(chan struct{})
	srv.persistDelayForTest = func() {
		close(persisting)
		time.Sleep(persistDelay)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.execSQL(ctx, "INSERT INTO items (id, name) VALUES (1, 'second')")
		writeDone <- err
	}()

	select {
	case <-persisting:
	case <-time.After(5 * time.Second):
		t.Fatal("write never reached its persist phase")
	}

	readStart := time.Now()
	if _, err := reader.execSQL(ctx, "SELECT COUNT(*) FROM items"); err != nil {
		t.Fatalf("concurrent read failed: %v", err)
	}
	readElapsed := time.Since(readStart)

	if err := <-writeDone; err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if readElapsed >= persistDelay/2 {
		t.Fatalf("read during write's persist phase took %v, want well under the %v persist delay -- "+
			"reads are still blocking on write persistence", readElapsed, persistDelay)
	}
}

// TestCommitTxDoesNotBlockConcurrentReadDuringPersist is the same proof for
// an explicit transaction's commit path (commitTxApply / commitTx in
// tx.go), which has its own mutation-then-checkpoint-then-persist sequence
// distinct from the autocommit path above.
func TestCommitTxDoesNotBlockConcurrentReadDuringPersist(t *testing.T) {
	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	writer := &conn{srv: srv, tenant: "default"}
	reader := &conn{srv: srv, tenant: "default"}
	ctx := context.Background()

	if _, err := writer.execSQL(ctx, "CREATE TABLE tx_items (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const persistDelay = 200 * time.Millisecond
	persisting := make(chan struct{})
	srv.persistDelayForTest = func() {
		close(persisting)
		time.Sleep(persistDelay)
	}

	writeDone := make(chan error, 1)
	go func() {
		if _, err := writer.execSQL(ctx, "BEGIN"); err != nil {
			writeDone <- fmt.Errorf("begin: %w", err)
			return
		}
		if _, err := writer.execSQL(ctx, "INSERT INTO tx_items (id, name) VALUES (1, 'a')"); err != nil {
			writeDone <- fmt.Errorf("insert: %w", err)
			return
		}
		_, err := writer.execSQL(ctx, "COMMIT")
		writeDone <- err
	}()

	select {
	case <-persisting:
	case <-time.After(5 * time.Second):
		t.Fatal("commit never reached its persist phase")
	}

	readStart := time.Now()
	if _, err := reader.execSQL(ctx, "SELECT COUNT(*) FROM tx_items"); err != nil {
		t.Fatalf("concurrent read failed: %v", err)
	}
	readElapsed := time.Since(readStart)

	if err := <-writeDone; err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	if readElapsed >= persistDelay/2 {
		t.Fatalf("read during commit's persist phase took %v, want well under the %v persist delay -- "+
			"reads are still blocking on commit persistence", readElapsed, persistDelay)
	}
}

// TestConcurrentReadsAndWritesNoRace is a broader stress test: many readers
// and writers hammering the same server concurrently, with an artificial
// (short) persist delay on every write so the read/write overlap this
// session introduced is actually exercised on most iterations, not just
// occasionally. It asserts only the absence of errors/crashes -- this
// machine has no cgo, so `go test -race` cannot run locally; this is a
// best-effort empirical check pending CI's race detector as the real gate.
func TestConcurrentReadsAndWritesNoRace(t *testing.T) {
	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	setup := &conn{srv: srv, tenant: "default"}
	ctx := context.Background()
	if _, err := setup.execSQL(ctx, "CREATE TABLE stress_items (id INT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	srv.persistDelayForTest = func() { time.Sleep(2 * time.Millisecond) }

	const writers = 8
	const readers = 8
	const opsPerGoroutine = 40
	var nextID atomic.Int64

	var wg sync.WaitGroup
	errs := make(chan error, (writers+readers)*opsPerGoroutine)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &conn{srv: srv, tenant: "default"}
			for i := 0; i < opsPerGoroutine; i++ {
				id := nextID.Add(1)
				query := fmt.Sprintf("INSERT INTO stress_items (id, name) VALUES (%d, 'item-%d')", id, id)
				if _, err := c.execSQL(ctx, query); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &conn{srv: srv, tenant: "default"}
			for i := 0; i < opsPerGoroutine; i++ {
				if _, err := c.execSQL(ctx, "SELECT COUNT(*) FROM stress_items"); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
