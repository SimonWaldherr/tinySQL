package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func TestCanceledDriverEntryPoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A disconnected connection proves no binding, preparation, or database
	// access happens before the cancellation check.
	c := new(conn)
	prepared, err := buildPreparedQuery("SELECT ?")
	if err != nil {
		t.Fatal(err)
	}
	args := []driver.NamedValue{{Ordinal: 1, Value: make([]byte, 4096)}}
	for name, run := range map[string]func() error{
		"exec":            func() error { _, e := c.ExecContext(ctx, "SELECT ?", args); return e },
		"query":           func() error { _, e := c.QueryContext(ctx, "SELECT ?", args); return e },
		"prepare":         func() error { _, e := c.PrepareContext(ctx, "SELECT ?"); return e },
		"begin":           func() error { _, e := c.BeginTx(ctx, driver.TxOptions{}); return e },
		"statement exec":  func() error { _, e := (&stmt{c: c, prepared: prepared}).ExecContext(ctx, args); return e },
		"statement query": func() error { _, e := (&stmt{c: c, prepared: prepared}).QueryContext(ctx, args); return e },
		"fallback exec":   func() error { _, e := (&stmt{c: c, sql: "SELECT $1"}).ExecContext(ctx, args); return e },
		"fallback query":  func() error { _, e := (&stmt{c: c, sql: "SELECT $1"}).QueryContext(ctx, args); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestParseCacheWaiterDeadline(t *testing.T) {
	const query = "SELECT 987654321 AS canceled_cache_waiter"
	// Represent a leader which deliberately remains unfinished until cleanup.
	// A waiter must return on its own deadline and leave that leader intact.
	call := &parsedStmtCall{done: make(chan struct{})}
	parsedStmtMu.Lock()
	parsedStmtInFlight[query] = call
	parsedStmtMu.Unlock()
	t.Cleanup(func() {
		parsedStmtMu.Lock()
		delete(parsedStmtInFlight, query)
		close(call.done)
		parsedStmtMu.Unlock()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := parseSQLCachedContext(ctx, query); result <- err }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cache waiter ignored deadline")
	}
	parsedStmtMu.RLock()
	remaining := parsedStmtInFlight[query]
	parsedStmtMu.RUnlock()
	if remaining != call {
		t.Fatal("canceled waiter removed shared leader")
	}
	select {
	case <-call.done:
		t.Fatal("canceled waiter closed leader")
	default:
	}
}

func TestRowsEncodingError(t *testing.T) {
	r := &rows{rs: &engine.ResultSet{Cols: []string{"payload"}, Rows: []engine.Row{{"payload": make(chan int)}}}}
	defer r.Close()
	err := r.Next(make([]driver.Value, 1))
	var unsupported *json.UnsupportedTypeError
	if !errors.As(err, &unsupported) || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("missing column/encoding error: %v", err)
	}
}

func TestSQLColumnScanType(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=scan_type_contract")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rs, err := db.QueryContext(context.Background(), "SELECT 1 AS id")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	cols, err := rs.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 1 || cols[0].ScanType() != reflect.TypeFor[any]() {
		t.Fatalf("unexpected column types: %v", cols)
	}
}

func BenchmarkCanceledDriverBinding(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := sql.Open("tinysql", "mem://?tenant=canceled_binding_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	native, err := db.Conn(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer native.Close()
	err = native.Raw(func(raw any) error {
		c := raw.(*conn)
		args := []driver.NamedValue{{Ordinal: 1, Value: make([]byte, 4096)}}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := c.QueryContext(ctx, "SELECT ?", args); !errors.Is(err, context.Canceled) {
				b.Fatalf("got %v", err)
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}
