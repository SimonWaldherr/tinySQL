package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCommitHonorsContextWhileWaitingForWriter(t *testing.T) {
	for _, entry := range []string{"exec", "query", "driver transaction"} {
		t.Run(entry, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { db.Close() })
			stmt, err := engine.NewParser("CREATE TABLE items (id INT)").ParseStatement()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Execute(t.Context(), db, "default", stmt); err != nil {
				t.Fatal(err)
			}
			c := &conn{srv: newServer(db, cfg{maxWriters: 1}), tenant: "default"}
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			tx, err := c.BeginTx(ctx, driver.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.ExecContext(t.Context(), "INSERT INTO items VALUES (1)", nil); err != nil {
				t.Fatal(err)
			}
			if err := c.srv.acquireWriter(t.Context()); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				switch entry {
				case "exec":
					_, err := c.ExecContext(ctx, "COMMIT", nil)
					result <- err
				case "query":
					rows, err := c.QueryContext(ctx, "COMMIT", nil)
					if rows != nil {
						rows.Close()
					}
					result <- err
				default:
					result <- tx.Commit()
				}
			}()
			select {
			case err := <-result:
				c.srv.releaseWriter()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("commit error = %v", err)
				}
			case <-time.After(time.Second):
				// Unblock the old behavior before failing so no goroutine leaks.
				c.srv.releaseWriter()
				<-result
				t.Fatal("commit ignored context deadline")
			}
			if !c.inTx || len(c.srv.writerPool) != 0 {
				t.Fatal("canceled commit changed transaction state or leaked writer slot")
			}
			items, err := db.Get("default", "items")
			if err != nil || len(items.Rows) != 0 {
				t.Fatalf("canceled commit published rows: %v, %v", items, err)
			}
			// SQL-level COMMIT may be retried with a fresh request context.
			if _, err := c.ExecContext(t.Context(), "COMMIT", nil); err != nil {
				t.Fatalf("commit retry: %v", err)
			}
			if c.inTx {
				t.Fatal("successful retry retained transaction state")
			}
		})
	}
}

func TestCommitCanceledWhileWaitingForContentLockDoesNotPublish(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { db.Close() })
	stmt, err := engine.NewParser("CREATE TABLE items (id INT)").ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(t.Context(), db, "default", stmt); err != nil {
		t.Fatal(err)
	}
	c := &conn{srv: newServer(db, cfg{maxWriters: 1}), tenant: "default"}
	if _, err := c.BeginTx(t.Context(), driver.TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(t.Context(), "INSERT INTO items VALUES (1)", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	db.LockContentForWrite()
	result := make(chan error, 1)
	go func() {
		_, err := c.ExecContext(ctx, "COMMIT", nil)
		result <- err
	}()
	<-ctx.Done()
	db.UnlockContentForWrite()
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commit published after its context expired: %v", err)
	}
	items, err := db.Get("default", "items")
	if err != nil || len(items.Rows) != 0 || !c.inTx || len(c.srv.writerPool) != 0 {
		t.Fatalf("canceled commit changed database/state: table=%v, err=%v, inTx=%v, writers=%d", items, err, c.inTx, len(c.srv.writerPool))
	}
	if err := c.rollbackTx(); err != nil {
		t.Fatal(err)
	}
}
