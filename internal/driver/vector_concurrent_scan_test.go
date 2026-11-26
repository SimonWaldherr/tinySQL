package driver

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type pausedVectorDriverRead struct {
	context.Context
	entered, resume chan struct{}
	once            sync.Once
}

func (ctx *pausedVectorDriverRead) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.entered)
		<-ctx.resume
	})
	return ctx.Context.Done()
}

func TestVectorQueryReleasesDriverReadGuardDuringRanking(t *testing.T) {
	const rows, dims, tenant = 4096, 256, "default"
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("vector_guard", []storage.Column{
		{Name: "id", Type: storage.IntType}, {Name: "v", Type: storage.VectorType},
	}, false)
	oldVector := make([]float64, dims)
	oldVector[0] = 1
	for i := range rows {
		table.Rows = append(table.Rows, []any{i, oldVector})
	}
	if err := db.Put(tenant, table); err != nil {
		t.Fatal(err)
	}
	srv := newServer(db, cfg{})
	reader, writer := &conn{srv: srv, tenant: tenant}, &conn{srv: srv, tenant: tenant}
	vector := "[" + strings.Repeat("0,", dims-1) + "0]"
	query := `SELECT id FROM VEC_SEARCH('vector_guard', 'v', '` + vector + `', 1, 'l2')`
	warm, err := reader.QueryContext(t.Context(), query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	// The next search must release both locks after following this update.
	updated := strings.Replace(vector, "0", "2", 1)
	if _, err := writer.ExecContext(t.Context(), `UPDATE vector_guard SET v = '`+updated+`' WHERE id = 0`, nil); err != nil {
		t.Fatal(err)
	}
	ctx := &pausedVectorDriverRead{Context: t.Context(), entered: make(chan struct{}), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(ctx.resume) })
	defer resume()
	read := make(chan struct {
		rows driver.Rows
		err  error
	}, 1)
	go func() {
		rows, err := reader.QueryContext(ctx, query, nil)
		read <- struct {
			rows driver.Rows
			err  error
		}{rows, err}
	}()
	select {
	case <-ctx.entered:
	case result := <-read:
		if result.rows != nil {
			_ = result.rows.Close()
		}
		t.Fatalf("query finished before ranking: %v", result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("query never entered ranking")
	}
	write := make(chan error, 1)
	go func() {
		_, err := writer.ExecContext(t.Context(), `INSERT INTO vector_guard VALUES (5000, '`+vector+`')`, nil)
		write <- err
	}()
	select {
	case err := <-write:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("driver append waited for vector ranking")
	}
	resume()
	result := <-read
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.rows.Close()
	dest := make([]driver.Value, 1)
	if err := result.rows.Next(dest); err != nil || dest[0] != int64(5000) {
		t.Fatalf("nearest row after concurrent append = %v, %v", dest, err)
	}
}
