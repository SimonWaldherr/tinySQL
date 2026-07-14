package driver

import (
	"context"
	"database/sql"
	stdDriver "database/sql/driver"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// newConcurrentPreparedStmt creates one driver statement deliberately shared by
// many goroutines. database/sql may create one statement per physical connection,
// but driver statements must also be safe when a caller keeps a single connection.
func newConcurrentPreparedStmt(tb testing.TB) *stmt {
	tb.Helper()

	srv := newServer(storage.NewDB(), cfg{tenant: "default"})
	c := &conn{srv: srv, tenant: "default"}
	ctx := context.Background()
	for _, query := range []string{
		"CREATE TABLE concurrent_items (id INTEGER, name TEXT)",
		"CREATE UNIQUE INDEX concurrent_items_id ON concurrent_items(id)",
	} {
		if _, err := c.execSQL(ctx, query); err != nil {
			tb.Fatalf("setup %q: %v", query, err)
		}
	}
	for id := 0; id < 64; id++ {
		query := fmt.Sprintf("INSERT INTO concurrent_items (id, name) VALUES (%d, 'item-%d')", id, id)
		if _, err := c.execSQL(ctx, query); err != nil {
			tb.Fatalf("setup item %d: %v", id, err)
		}
	}

	raw, err := c.Prepare("SELECT name FROM concurrent_items WHERE id = ?")
	if err != nil {
		tb.Fatalf("prepare: %v", err)
	}
	prepared, ok := raw.(*stmt)
	if !ok {
		tb.Fatalf("prepared statement type %T", raw)
	}
	tb.Cleanup(func() { _ = prepared.Close() })
	return prepared
}

func queryPreparedName(ctx context.Context, s *stmt, id int) (string, error) {
	raw, err := s.QueryContext(ctx, []stdDriver.NamedValue{{Ordinal: 1, Value: int64(id)}})
	if err != nil {
		return "", err
	}
	defer raw.Close()

	dest := make([]stdDriver.Value, 1)
	if err := raw.Next(dest); err != nil {
		return "", err
	}
	if err := raw.Next(make([]stdDriver.Value, 1)); err != io.EOF {
		return "", fmt.Errorf("expected exactly one row, got %v", err)
	}
	name, ok := dest[0].(string)
	if !ok {
		return "", fmt.Errorf("name type %T", dest[0])
	}
	return name, nil
}

func TestPreparedStmtConcurrentBindings(t *testing.T) {
	statement := newConcurrentPreparedStmt(t)
	const workers = 32
	const queriesPerWorker = 64

	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < queriesPerWorker; n++ {
				id := (worker + n) % 64
				got, err := queryPreparedName(context.Background(), statement, id)
				if err != nil {
					errs <- err
					return
				}
				if want := fmt.Sprintf("item-%d", id); got != want {
					errs <- fmt.Errorf("id %d: got %q, want %q", id, got, want)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestSQLPreparedStmtConcurrentConnections(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=prepared_concurrent&pool_readers=8")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	if _, err := db.Exec("CREATE TABLE sql_concurrent_items (id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX sql_concurrent_items_id ON sql_concurrent_items(id)"); err != nil {
		t.Fatal(err)
	}
	for id := 0; id < 64; id++ {
		if _, err := db.Exec("INSERT INTO sql_concurrent_items (id, name) VALUES (?, ?)", id, fmt.Sprintf("item-%d", id)); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	statement, err := db.Prepare("SELECT name FROM sql_concurrent_items WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()

	const workers = 32
	const queriesPerWorker = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < queriesPerWorker; n++ {
				id := (worker + n) % 64
				var got string
				if err := statement.QueryRowContext(context.Background(), id).Scan(&got); err != nil {
					errs <- err
					return
				}
				if want := fmt.Sprintf("item-%d", id); got != want {
					errs <- fmt.Errorf("id %d: got %q, want %q", id, got, want)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestParseSQLCachedConcurrentColdMissSharesReadAST(t *testing.T) {
	// Isolate the package cache so this regression test proves a cold-start
	// burst. A returned SELECT AST is immutable during execution and can safely
	// be shared by all waiters.
	parsedStmtMu.Lock()
	oldCache, oldInFlight := parsedStmtCache, parsedStmtInFlight
	parsedStmtCache = make(map[string]engine.Statement)
	parsedStmtInFlight = make(map[string]*parsedStmtCall)
	parsedStmtMu.Unlock()
	t.Cleanup(func() {
		parsedStmtMu.Lock()
		parsedStmtCache, parsedStmtInFlight = oldCache, oldInFlight
		parsedStmtMu.Unlock()
	})

	const workers = 48
	start := make(chan struct{})
	results := make(chan engine.Statement, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			statement, err := parseSQLCached("SELECT name FROM concurrent_items WHERE id = 42")
			if err != nil {
				errs <- err
				return
			}
			results <- statement
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	var first engine.Statement
	for statement := range results {
		if first == nil {
			first = statement
			continue
		}
		if statement != first {
			t.Fatal("cold parse cache returned distinct SELECT ASTs")
		}
	}
	if first == nil {
		t.Fatal("no parsed statement returned")
	}
	parsedStmtMu.RLock()
	entries, inFlight := len(parsedStmtCache), len(parsedStmtInFlight)
	parsedStmtMu.RUnlock()
	if entries != 1 || inFlight != 0 {
		t.Fatalf("cache state after cold burst: entries=%d in_flight=%d", entries, inFlight)
	}
}

func BenchmarkPreparedStmtParallel(b *testing.B) {
	statement := newConcurrentPreparedStmt(b)
	ctx := context.Background()
	var next atomic.Uint64
	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(next.Add(1) % 64)
			if _, err := queryPreparedName(ctx, statement, id); err != nil {
				b.Fatal(err)
			}
		}
	})
}
