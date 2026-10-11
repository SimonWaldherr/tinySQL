package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompletedStreamReleasesDerivedContext(t *testing.T) {
	for _, tc := range []struct {
		name   string
		query  string
		buffer int
	}{
		{"producer", "SELECT id FROM stream_rows", 0},
		{"tail", "SELECT id FROM stream_rows", DefaultResultStreamBuffer},
		{"materialized", "SELECT id FROM stream_rows ORDER BY id", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := streamTestDB(t, 100)
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream, err := ExecuteStreamWithOptions(parent, db, "default", mustParse(tc.query), StreamOptions{Buffer: tc.buffer})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stream.Close() })
			count := 0
			for stream.Next() {
				count++
			}
			if err := stream.Err(); err != nil || count != 100 {
				t.Fatalf("stream result: rows=%d, err=%v", count, err)
			}
			select {
			case <-stream.ctx.Done():
			default:
				t.Fatal("finished stream retained its parent context registration")
			}
			if err := parent.Err(); err != nil {
				t.Fatalf("stream completion canceled parent: %v", err)
			}
		})
	}
}

func TestStreamCompletedBeforeHeaderIsConsumed(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { db.Close() })
	// Materialize into a buffer large enough to finish before the caller reads
	// the header. Releasing the child context must not look like cancellation.
	stmt := mustParse("SELECT 1 AS id ORDER BY id")
	for i := 0; i < 100; i++ {
		stream, err := ExecuteStreamWithOptions(t.Context(), db, "default", stmt, StreamOptions{Buffer: 1})
		if err != nil {
			t.Fatal(err)
		}
		if !stream.Next() || stream.Row()["id"] != 1 || stream.Next() || stream.Err() != nil {
			stream.Close()
			t.Fatalf("completed stream lost its result: %v", stream.Err())
		}
		stream.Close()
	}
}

func TestFailedStreamReleasesDerivedContext(t *testing.T) {
	db := streamTestDB(t, 2)
	table, err := db.Get("default", "stream_rows")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows[1] = []any{} // invalid projection after the first valid row
	stream, err := ExecuteStreamWithOptions(t.Context(), db, "default", mustParse("SELECT id FROM stream_rows"), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	if !stream.Next() || stream.Next() || stream.Err() == nil {
		t.Fatalf("expected a projection failure after the first row: %v", stream.Err())
	}
	select {
	case <-stream.ctx.Done():
	default:
		t.Fatal("failed stream retained its derived context")
	}
}

func TestStreamPreservesParentCancellationCause(t *testing.T) {
	db := streamTestDB(t, 1000)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	stream, err := ExecuteStreamWithOptions(ctx, db, "default", mustParse("SELECT id FROM stream_rows"), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	cause := errors.New("request ended")
	cancel(cause)
	for stream.Next() {
	}
	if err := stream.Err(); !errors.Is(err, cause) {
		t.Fatalf("stream lost cancellation cause: %v", err)
	}
}
