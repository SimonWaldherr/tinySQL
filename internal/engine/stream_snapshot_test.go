package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestExecuteStreamSlowConsumerDoesNotBlockUnrelatedWrite exercises the
// important package/API property of a stream: a caller that leaves a zero
// buffer stream blocked on its first row must not keep contentMu's global
// read side and stall writes to another table.
func TestExecuteStreamSlowConsumerDoesNotBlockUnrelatedWrite(t *testing.T) {
	db := streamTestDB(t, 1_000)
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE stream_other (id INT)`)); err != nil {
		t.Fatal(err)
	}

	stream, err := ExecuteStreamWithOptions(context.Background(), db, "default", mustParse(`SELECT id FROM stream_rows`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	writeDone := make(chan error, 1)
	go func() {
		_, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO stream_other VALUES (1)`))
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("unrelated write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow stream held the global content read lock during an unrelated write")
	}
}

// TestExecuteStreamKeepsPinnedTableSnapshot verifies copy-on-write for the
// only table a live direct stream reads. The insert must be visible to a new
// query but not mutate the stream's old table slice while it is being scanned.
func TestExecuteStreamKeepsPinnedTableSnapshot(t *testing.T) {
	db := streamTestDB(t, 32)
	stream, err := ExecuteStreamWithOptions(context.Background(), db, "default", mustParse(`SELECT id FROM stream_rows`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO stream_rows VALUES (999, 'new')`)); err != nil {
		t.Fatalf("write while stream is live: %v", err)
	}

	var ids []int
	for stream.Next() {
		id, ok := stream.Row()["id"].(int)
		if !ok {
			t.Fatalf("id has type %T, want int", stream.Row()["id"])
		}
		ids = append(ids, id)
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 32 {
		t.Fatalf("streamed %d rows, want original snapshot of 32: %v", len(ids), ids)
	}
	for i, id := range ids {
		if id != i {
			t.Fatalf("stream row %d = %d, want %d", i, id, i)
		}
	}

	rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT id FROM stream_rows WHERE id = 999`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 999 {
		t.Fatalf("post-write query = %#v, want inserted row", rs.Rows)
	}
}

// TestExecuteStreamCancellationReleasesTablePin proves cancellation unpins
// the old table before Done closes. This prevents a cancelled client from
// retaining a historical table instance or forcing future writes to copy it.
func TestExecuteStreamCancellationReleasesTablePin(t *testing.T) {
	db := streamTestDB(t, 1_000)
	table, err := db.Get("default", "stream_rows")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := ExecuteStreamWithOptions(ctx, db, "default", mustParse(`SELECT id FROM stream_rows`), StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("cancelled stream did not stop promptly")
	}
	if !errors.Is(stream.Err(), context.Canceled) {
		t.Fatalf("stream error = %v, want context.Canceled", stream.Err())
	}
	if db.IsTablePinnedForStream(table) {
		t.Fatal("cancelled stream retained its table pin after Done")
	}
}
