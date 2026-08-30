package storage

import (
	"bytes"
	"testing"
	"time"
)

type blockingCheckpointWriter struct {
	started chan struct{}
	release chan struct{}
	once    chan struct{}
	buf     bytes.Buffer
}

func newBlockingCheckpointWriter() *blockingCheckpointWriter {
	return &blockingCheckpointWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
		once:    make(chan struct{}, 1),
	}
}

func (w *blockingCheckpointWriter) Write(p []byte) (int, error) {
	select {
	case w.once <- struct{}{}:
		close(w.started)
	default:
	}
	<-w.release
	return w.buf.Write(p)
}

func TestSaveToWriterDoesNotHoldDBLockDuringOutput(t *testing.T) {
	db := NewDB()
	rows := make([][]any, 2000)
	for i := range rows {
		rows[i] = []any{i, "enough data to fill the checkpoint buffer"}
	}
	if err := db.Put("default", &Table{
		Name: "source",
		Cols: []Column{{Name: "id", Type: IntType}, {Name: "value", Type: TextType}},
		Rows: rows,
	}); err != nil {
		t.Fatalf("Put source: %v", err)
	}

	w := newBlockingCheckpointWriter()
	saveDone := make(chan error, 1)
	go func() { saveDone <- SaveToWriter(db, w) }()
	select {
	case <-w.started:
	case <-time.After(2 * time.Second):
		close(w.release)
		t.Fatal("checkpoint writer was not reached")
	}

	putDone := make(chan error, 1)
	go func() {
		putDone <- db.Put("default", &Table{Name: "concurrent", Cols: []Column{{Name: "id", Type: IntType}}})
	}()
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("concurrent Put: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(w.release)
		t.Fatal("concurrent Put blocked on checkpoint output")
	}

	close(w.release)
	if err := <-saveDone; err != nil {
		t.Fatalf("SaveToWriter: %v", err)
	}
	restored, err := LoadFromBytes(w.buf.Bytes())
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	if _, err := restored.Get("default", "source"); err != nil {
		t.Fatalf("snapshot missing source table: %v", err)
	}
	if _, err := restored.Get("default", "concurrent"); err == nil {
		t.Fatal("snapshot unexpectedly included table written after capture")
	}
}
