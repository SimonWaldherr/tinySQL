package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestParseDSNMemory(t *testing.T) {
	c, err := parseDSN("mem://?tenant=tenant1&pool_readers=2&pool_writers=3&autosave=yes&busy_timeout=750ms")
	if err != nil {
		t.Fatalf("parseDSN returned error: %v", err)
	}
	if c.tenant != "tenant1" {
		t.Fatalf("expected tenant1, got %q", c.tenant)
	}
	if c.filePath != "" {
		t.Fatalf("expected empty filePath for mem, got %q", c.filePath)
	}
	if !c.autosave {
		t.Fatalf("expected autosave to be true")
	}
	if c.maxReaders != 2 {
		t.Fatalf("expected maxReaders=2, got %d", c.maxReaders)
	}
	if c.maxWriters != 3 {
		t.Fatalf("expected maxWriters=3, got %d", c.maxWriters)
	}
	if c.busyTimeout != 750*time.Millisecond {
		t.Fatalf("expected busyTimeout=750ms, got %s", c.busyTimeout)
	}
}

func TestParseDSNFile(t *testing.T) {
	c, err := parseDSN("file:./test.db?tenant=tenant2&autosave=true")
	if err != nil {
		t.Fatalf("parseDSN returned error: %v", err)
	}
	if c.tenant != "tenant2" {
		t.Fatalf("expected tenant2, got %q", c.tenant)
	}
	wantPath := filepath.Clean("./test.db")
	if c.filePath != wantPath {
		t.Fatalf("expected filePath %q, got %q", wantPath, c.filePath)
	}
	if !c.autosave {
		t.Fatalf("expected autosave enabled")
	}
}

func TestParseDSNErrors(t *testing.T) {
	if _, err := parseDSN("file:"); err == nil {
		t.Fatalf("expected error for missing file path")
	}
	if _, err := parseDSN("custom://path"); err == nil {
		t.Fatalf("expected error for unsupported scheme")
	}
}

func TestApplyDSNOptionErrors(t *testing.T) {
	var c cfg
	if err := applyDSNOption(&c, "pool_readers", "abc"); err == nil {
		t.Fatalf("expected error for invalid reader pool size")
	}
	if err := applyDSNOption(&c, "pool_writers", "-1"); err == nil {
		t.Fatalf("expected error for negative writer pool size")
	}
	if err := applyDSNOption(&c, "busy_timeout", "nope"); err == nil {
		t.Fatalf("expected error for invalid busy_timeout")
	}
}

func TestParsePoolSize(t *testing.T) {
	if n, err := parsePoolSize("5", "pool_readers"); err != nil || n != 5 {
		t.Fatalf("expected 5, got %d (err=%v)", n, err)
	}
	if _, err := parsePoolSize("abc", "pool_readers"); err == nil {
		t.Fatalf("expected error for invalid number")
	}
	if _, err := parsePoolSize("-2", "pool_readers"); err == nil {
		t.Fatalf("expected error for negative value")
	}
}

func TestParseBusyTimeout(t *testing.T) {
	if dur, err := parseBusyTimeout("1500"); err != nil || dur != 1500*time.Millisecond {
		t.Fatalf("expected 1500ms, got %s (err=%v)", dur, err)
	}
	if dur, err := parseBusyTimeout("2s"); err != nil || dur != 2*time.Second {
		t.Fatalf("expected 2s, got %s (err=%v)", dur, err)
	}
	if _, err := parseBusyTimeout("-1"); err == nil {
		t.Fatalf("expected error for negative duration")
	}
	if _, err := parseBusyTimeout("later"); err == nil {
		t.Fatalf("expected error for invalid duration string")
	}
}

func TestDSNAliases(t *testing.T) {
	c, err := parseDSN("mem://?read_pool=2&write_pool=1&busytimeout=100")
	if err != nil {
		t.Fatalf("parseDSN returned error: %v", err)
	}
	if c.maxReaders != 2 || c.maxWriters != 1 || c.busyTimeout != 100*time.Millisecond {
		t.Fatalf("alias parsing failed: %#v", c)
	}
}

func TestBindPlaceholders(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: "O'Reilly"},
		{Ordinal: 3, Value: map[string]any{"foo": "bar"}},
	}
	query := "SELECT ?, '?', 'It''s ?', ?, ?"
	got, err := bindPlaceholders(query, args)
	if err != nil {
		t.Fatalf("bindPlaceholders returned error: %v", err)
	}
	want := "SELECT 42, '?', 'It''s ?', 'O''Reilly', '{\"foo\":\"bar\"}'"
	if got != want {
		t.Fatalf("unexpected expansion:\nwant: %s\n got: %s", want, got)
	}
}

func TestBindPlaceholdersErrors(t *testing.T) {
	if _, err := bindPlaceholders("SELECT ?", nil); err == nil {
		t.Fatalf("expected error for missing args")
	}
	args := []driver.NamedValue{{Ordinal: 1, Value: 1}, {Ordinal: 2, Value: 2}}
	if _, err := bindPlaceholders("SELECT ?", args); err == nil {
		t.Fatalf("expected error for too many args")
	}
}

func TestSqlLiteral(t *testing.T) {
	if lit := sqlLiteral(nil); lit != "NULL" {
		t.Fatalf("expected NULL, got %s", lit)
	}
	if lit := sqlLiteral(int64(5)); lit != "5" {
		t.Fatalf("expected 5, got %s", lit)
	}
	if lit := sqlLiteral(3.14); lit != "3.14" {
		t.Fatalf("expected 3.14, got %s", lit)
	}
	if lit := sqlLiteral(true); lit != "TRUE" {
		t.Fatalf("expected TRUE, got %s", lit)
	}
	if lit := sqlLiteral("O'Reilly"); lit != "'O''Reilly'" {
		t.Fatalf("expected quoted string, got %s", lit)
	}
	if lit := sqlLiteral(map[string]any{"foo": "bar"}); lit != "'{\"foo\":\"bar\"}'" {
		t.Fatalf("expected JSON literal, got %s", lit)
	}
}

func TestRowsNext(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"ID", "DATA"},
		Rows: []engine.Row{
			{"id": int64(7), "data": map[string]any{"k": "v"}},
		},
	}
	r := &rows{rs: rs}
	dest := make([]driver.Value, 2)
	if err := r.Next(dest); err != nil {
		t.Fatalf("Next returned error: %v", err)
	}
	if dest[0] != int64(7) {
		t.Fatalf("expected int64 7, got %v", dest[0])
	}
	if dest[1] != "{\"k\":\"v\"}" {
		t.Fatalf("expected marshalled JSON, got %v", dest[1])
	}
	if err := r.Next(dest); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if cols := r.Columns(); len(cols) != 2 || cols[0] != "ID" {
		t.Fatalf("unexpected columns: %v", cols)
	}
	if nullable, ok := r.ColumnTypeNullable(0); !nullable || !ok {
		t.Fatalf("expected nullable=true")
	}
	if scan := r.ColumnTypeScanType(0); scan != "interface{}" {
		t.Fatalf("unexpected scan type: %v", scan)
	}
}

func TestEmptyRows(t *testing.T) {
	var r emptyRows
	if cols := r.Columns(); len(cols) != 0 {
		t.Fatalf("expected no columns, got %v", cols)
	}
	if err := r.Next(make([]driver.Value, 0)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestServerAcquireNilPool(t *testing.T) {
	s := &server{}
	if err := s.acquireReader(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil pool, got %v", err)
	}
}

func TestServerAcquireContextCancelled(t *testing.T) {
	s := &server{readerPool: make(chan struct{}, 1)}
	s.readerPool <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.acquireReader(ctx); err != context.Canceled {
		t.Fatalf("expected context canceled, got %v", err)
	}
	<-s.readerPool
}

func TestServerAcquireBusyTimeout(t *testing.T) {
	s := &server{writerPool: make(chan struct{}, 1), busyTimeout: 5 * time.Millisecond}
	s.writerPool <- struct{}{}
	start := time.Now()
	err := s.acquireWriter(context.Background())
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if !strings.Contains(err.Error(), "busy timeout") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("expected to wait at least timeout, waited %s", elapsed)
	}
	<-s.writerPool
}

func TestServerAcquireRespectsContextDeadline(t *testing.T) {
	s := &server{readerPool: make(chan struct{}, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	s.readerPool <- struct{}{}
	err := s.acquireReader(ctx)
	if err == nil {
		t.Fatalf("expected context deadline exceeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	<-s.readerPool
}

func TestServerSaveIfNeeded(t *testing.T) {
	db := storage.NewDB()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "db.gob")
	s := &server{db: db, filePath: path, autosave: true}
	s.saveIfNeeded()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected autosave to create file: %v", err)
	}
}

func TestDrvOpenAndConnExecQuery(t *testing.T) {
	// Open with in-memory DSN
	d := &drv{}
	rawConn, err := d.Open("mem://?tenant=test")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)

	// Create table and insert
	if _, err := c.ExecContext(context.Background(), "CREATE TABLE t (id INT, name TEXT)", nil); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO t VALUES (?, ?)", []driver.NamedValue{{Ordinal: 1, Value: 1}, {Ordinal: 2, Value: "a"}}); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Query select
	rows, err := c.QueryContext(context.Background(), "SELECT id, name FROM t", nil)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("next failed: %v", err)
	}

	// Query with non-select should return emptyRows
	r, err := c.QueryContext(context.Background(), "UPDATE t SET name='b'", nil)
	if err != nil {
		t.Fatalf("non-select query failed: %v", err)
	}
	if _, ok := r.(emptyRows); !ok {
		t.Fatalf("expected emptyRows for non-select query path")
	}
}

func TestStmtExecQueryPaths(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	if _, err := c.ExecContext(context.Background(), "CREATE TABLE s (id INT)", nil); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	st, err := c.Prepare("INSERT INTO s VALUES (?)")
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if _, err := st.Exec([]driver.Value{int64(7)}); err != nil {
		t.Fatalf("stmt exec failed: %v", err)
	}
	q, err := c.Prepare("SELECT id FROM s")
	if err != nil {
		t.Fatalf("prepare query failed: %v", err)
	}
	rs, err := q.Query(nil)
	if err != nil {
		t.Fatalf("stmt query failed: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rs.Next(dest); err != nil {
		t.Fatalf("stmt rows next failed: %v", err)
	}
}

func TestTransactionsSnapshotAndReadonly(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	if _, err := c.ExecContext(context.Background(), "CREATE TABLE tx (id INT)", nil); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	// Begin read-only tx and attempt write
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO tx VALUES (1)", nil); err == nil {
		_ = tx.Rollback()
		t.Fatalf("expected error writing in read-only tx")
	}
	_ = tx.Rollback()

	// Begin default tx and commit
	tx2, err := c.Begin()
	if err != nil {
		t.Fatalf("begin default tx failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO tx VALUES (2)", nil); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("write in tx failed: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	// Read after commit
	rows, err := c.QueryContext(context.Background(), "SELECT id FROM tx", nil)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("expected at least one row: %v", err)
	}
}

func TestCheckNamedValue(t *testing.T) {
	c := &conn{}
	// time.Time
	t0 := time.Unix(0, rand.Int63())
	nv := &driver.NamedValue{Ordinal: 1, Value: t0}
	if err := c.CheckNamedValue(nv); err != nil {
		t.Fatalf("CheckNamedValue time failed: %v", err)
	}
	if _, ok := nv.Value.(string); !ok {
		t.Fatalf("expected time to be converted to RFC3339 string")
	}
	// []byte
	nv = &driver.NamedValue{Ordinal: 1, Value: []byte{1, 2, 3}}
	_ = c.CheckNamedValue(nv)
	if _, ok := nv.Value.(string); !ok {
		t.Fatalf("expected []byte to base64 string")
	}
	// int
	nv = &driver.NamedValue{Ordinal: 1, Value: int(5)}
	_ = c.CheckNamedValue(nv)
	if v, ok := nv.Value.(int64); !ok || v != 5 {
		t.Fatalf("expected int -> int64 5, got %#v", nv.Value)
	}
}

func TestUnsupportedIsolationLevel(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	if _, err := c.BeginTx(context.Background(), driver.TxOptions{Isolation: driver.IsolationLevel(42)}); err == nil {
		t.Fatalf("expected unsupported isolation level error")
	}
}

func TestStmtNumInputAndClose(t *testing.T) {
	d := &drv{}
	rawConn, _ := d.Open("mem://")
	c := rawConn.(*conn)
	s, _ := c.Prepare("SELECT 1")
	if s.NumInput() != -1 {
		t.Fatalf("expected -1 NumInput")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close should be no-op: %v", err)
	}
}

func TestConnNonContextFallbacks(t *testing.T) {
	d := &drv{}
	rawConn, _ := d.Open("mem://")
	c := rawConn.(*conn)
	c.Exec("CREATE TABLE a (x INT)", nil)
	if _, err := c.Exec("INSERT INTO a VALUES (?)", []driver.Value{int64(1)}); err != nil {
		t.Fatalf("non-context exec failed: %v", err)
	}
	if _, err := c.Query("SELECT x FROM a", nil); err != nil {
		t.Fatalf("non-context query failed: %v", err)
	}
}

func TestConnClosePersistsWhenAutosave(t *testing.T) {
	// Use a temp file with autosave; mock via drv.srv
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "db.gob")
	s := newServer(storage.NewDB(), cfg{filePath: path, autosave: true})
	d := &drv{srv: s}
	rawConn, err := d.Open("")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	// Create a table and close; autosave should persist
	if _, err := c.ExecContext(context.Background(), "CREATE TABLE p (id INT)", nil); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected autosave file: %v", err)
	}
}

func TestBindPlaceholdersNestedQuotes(t *testing.T) {
	// Ensure double single-quote inside string stays intact
	q := "SELECT 'It''s fine', ?"
	got, err := bindPlaceholders(q, []driver.NamedValue{{Ordinal: 1, Value: "ok"}})
	if err != nil {
		t.Fatalf("bind failed: %v", err)
	}
	want := "SELECT 'It''s fine', 'ok'"
	if got != want {
		t.Fatalf("want %q got %q", want, got)
	}
}

func TestSqlLiteralEscaping(t *testing.T) {
	got := sqlLiteral("a'b\nc")
	if !strings.Contains(got, "O''") && !strings.Contains(got, "'a''b") {
		// At minimum ensure single quotes are doubled; newline is fine as-is
		t.Fatalf("expected doubled quotes in %q", got)
	}
}

// tiny helper to quiet unused imports during incremental edits
var _ = fmt.Sprintf
