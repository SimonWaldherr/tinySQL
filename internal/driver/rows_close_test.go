package driver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRowsCloseReleasesCallback(t *testing.T) {
	var calls int
	r := &rows{}
	r.onClose = func() {
		calls++
		if r.onClose != nil {
			t.Error("Close retained its callback during cleanup")
		}
	}
	for range 2 {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 || r.onClose != nil || !r.closed.Load() {
		t.Fatalf("cleanup calls = %d, retained callback = %v, closed = %v", calls, r.onClose != nil, r.closed.Load())
	}
}

func TestRowsCloseReleasesCallbackAfterPanic(t *testing.T) {
	var calls int
	r := &rows{}
	r.onClose = func() {
		calls++
		panic("cleanup failed")
	}
	func() {
		defer func() {
			if got := recover(); got != "cleanup failed" {
				t.Fatalf("cleanup panic = %v, want cleanup failed", got)
			}
		}()
		_ = r.Close()
		t.Fatal("Close swallowed the cleanup panic")
	}()
	if r.onClose != nil {
		t.Fatal("Close retained its callback after cleanup panicked")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
}

func TestRowsCloseJoinsStreamBeforeCleanup(t *testing.T) {
	stream := rowsCloseTestStream(t, false, 0)
	select {
	case <-stream.Done():
		t.Fatal("unconsumed stream finished without a consumer")
	default:
	}
	r := &rows{stream: stream, onClose: func() {
		select {
		case <-stream.Done():
		default:
			t.Error("cleanup ran before the stream producer stopped")
		}
	}}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.onClose != nil {
		t.Fatal("Close retained its callback")
	}
}

func TestRowsConcurrentClosePreservesStreamError(t *testing.T) {
	// This stream finishes with a projection error before Close starts. All
	// callers must see that same error after the single cleanup has run.
	stream := rowsCloseTestStream(t, true, engine.DefaultResultStreamBuffer)
	<-stream.Done()
	wantErr := stream.Err()
	if wantErr == nil {
		t.Fatal("malformed source row did not produce an error")
	}
	var calls atomic.Int32
	r := &rows{stream: stream, onClose: func() { calls.Add(1) }}
	const closers = 32
	start := make(chan struct{})
	results := make(chan error, closers)
	var group sync.WaitGroup
	for range closers {
		group.Go(func() {
			<-start
			results <- r.Close()
		})
	}
	close(start)
	group.Wait()
	close(results)
	for err := range results {
		if err != wantErr {
			t.Fatalf("Close error = %v, want producer error %v", err, wantErr)
		}
	}
	if got := calls.Load(); got != 1 || r.onClose != nil {
		t.Fatalf("cleanup calls = %d, retained callback = %v", got, r.onClose != nil)
	}
	if err := r.Close(); err != wantErr {
		t.Fatalf("repeated Close error = %v, want %v", err, wantErr)
	}
}

func rowsCloseTestStream(t *testing.T, malformed bool, buffer int) *engine.ResultStream {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("close_rows", []storage.Column{{Name: "id", Type: storage.IntType}}, false)
	table.Rows = [][]any{{1}}
	if malformed {
		table.Rows = append(table.Rows, []any{})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	stmt, err := engine.NewParser(`SELECT id FROM close_rows`).ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	stream, err := engine.ExecuteStreamWithOptions(context.Background(), db, "default", stmt, engine.StreamOptions{Buffer: buffer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}
