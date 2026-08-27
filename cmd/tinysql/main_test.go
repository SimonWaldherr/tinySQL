package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildTinysql(t *testing.T) {
	// Generous on purpose. This shells out to the real toolchain, the link step
	// is not cached, and "go test ./..." runs several of these concurrently, so a
	// tight budget fails on a busy machine while the build itself is fine. The
	// bound is here to catch a hung toolchain, not to police build speed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_tinysql_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	for _, arg := range []string{"-version", "--version", "version"} {
		version, err := exec.Command(out, arg).Output()
		if err != nil {
			_ = os.Remove(out)
			t.Fatalf("tinysql %s failed: %v", arg, err)
		}
		if got, want := string(version), "tinySQL "+versionString()+"\n"; got != want {
			_ = os.Remove(out)
			t.Fatalf("tinysql %s = %q, want %q", arg, got, want)
		}
	}
	_ = os.Remove(out)
}

func TestShellAndReplSubcommandsRouteToMainCLI(t *testing.T) {
	for _, name := range []string{"shell", "repl"} {
		handled, err := tryUtilityCommand(name, []string{"-batch"})
		if !handled {
			t.Fatalf("%s was not handled as an integrated shell command", name)
		}
		if err == nil || !strings.Contains(err.Error(), "batch mode requested") {
			t.Fatalf("%s returned %v, want batch-mode error from runCLI", name, err)
		}
	}
}

func setupTestDB(t *testing.T) *tsql.DB {
	t.Helper()
	db := tsql.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT, email TEXT)",
		"INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')",
		"INSERT INTO users (id, name, email) VALUES (2, 'Bob', NULL)",
		"CREATE TABLE orders (id INT, user_id INT, amount FLOAT)",
		"INSERT INTO orders (id, user_id, amount) VALUES (101, 1, 99.5)",
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	return db
}

func TestCountTables(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := countTables(&buf, db, "default", nil); err != nil {
		t.Fatalf("countTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected 'users' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "orders") {
		t.Errorf("expected 'orders' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "TOTAL") {
		t.Errorf("expected 'TOTAL' in output, got:\n%s", out)
	}
}

func TestCountTables_Filtered(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := countTables(&buf, db, "default", []string{"users"}); err != nil {
		t.Fatalf("countTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected 'users' in output, got:\n%s", out)
	}
	if strings.Contains(out, "orders") {
		t.Errorf("did not expect 'orders' in filtered output, got:\n%s", out)
	}
}

func TestShowStats(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := showStats(&buf, db, "default"); err != nil {
		t.Fatalf("showStats: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Tables:") {
		t.Errorf("expected 'Tables:' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "Total rows:") {
		t.Errorf("expected 'Total rows:' in output, got:\n%s", out)
	}
}

func TestDumpTables(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := dumpTables(&buf, db, "default", nil); err != nil {
		t.Fatalf("dumpTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CREATE TABLE") {
		t.Errorf("expected 'CREATE TABLE' in dump, got:\n%s", out)
	}
	if !strings.Contains(out, "INSERT INTO") {
		t.Errorf("expected 'INSERT INTO' in dump, got:\n%s", out)
	}
}

func TestDumpTables_Filtered(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := dumpTables(&buf, db, "default", []string{"orders"}); err != nil {
		t.Fatalf("dumpTables: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "orders") {
		t.Errorf("expected 'orders' in dump, got:\n%s", out)
	}
	// Users should NOT be dumped
	if strings.Contains(out, "CREATE TABLE users") {
		t.Errorf("did not expect users in filtered dump, got:\n%s", out)
	}
}

func TestImportFileCmd_CSV(t *testing.T) {
	db := tsql.NewDB()
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "test.csv")
	os.WriteFile(csvFile, []byte("name,age\nAlice,30\nBob,25\n"), 0644)

	var buf bytes.Buffer
	if err := importFileCmd(db, "default", []string{csvFile, "people"}, &buf); err != nil {
		t.Fatalf("importFileCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Imported") {
		t.Errorf("expected import confirmation, got:\n%s", out)
	}

	// Verify data was imported
	ctx := context.Background()
	stmt, _ := tsql.ParseSQL("SELECT COUNT(*) AS cnt FROM people")
	rs, err := tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if rs == nil || len(rs.Rows) == 0 {
		t.Fatal("expected count result")
	}
}

func TestImportFileCmd_JSON(t *testing.T) {
	db := tsql.NewDB()
	dir := t.TempDir()
	jsonFile := filepath.Join(dir, "items.json")
	os.WriteFile(jsonFile, []byte(`[{"x":"hello"},{"x":"world"}]`), 0644)

	var buf bytes.Buffer
	if err := importFileCmd(db, "default", []string{jsonFile, "items"}, &buf); err != nil {
		t.Fatalf("importFileCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Imported") {
		t.Errorf("expected import confirmation, got:\n%s", out)
	}
}

func TestImportFileCmd_AutoTableName(t *testing.T) {
	db := tsql.NewDB()
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "products.csv")
	os.WriteFile(csvFile, []byte("name,price\nWidget,9.99\n"), 0644)

	var buf bytes.Buffer
	// Don't pass table name — should default to "products" from filename
	if err := importFileCmd(db, "default", []string{csvFile}, &buf); err != nil {
		t.Fatalf("importFileCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "products") {
		t.Errorf("expected auto-detected table name 'products', got:\n%s", out)
	}
}

func TestExecute_Select(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true}
	var buf bytes.Buffer
	_, err := execute(context.Background(), db, cfg, "SELECT name FROM users ORDER BY name", &buf)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Alice") {
		t.Errorf("expected 'Alice' in output, got:\n%s", out)
	}
}

func TestExecute_Timer(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true, Timer: true}
	var buf bytes.Buffer
	_, err := execute(context.Background(), db, cfg, "SELECT 1 AS x", &buf)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Run Time:") {
		t.Errorf("expected 'Run Time:' in timer output, got:\n%s", out)
	}
}

func TestExecute_MultiStatement(t *testing.T) {
	db := tsql.NewDB()
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true}
	var buf bytes.Buffer
	_, err := execute(context.Background(), db, cfg,
		"CREATE TABLE ms (x INT); INSERT INTO ms (x) VALUES (42)", &buf)
	if err != nil {
		t.Fatalf("execute multi: %v", err)
	}
	// Verify the table exists
	stmt, _ := tsql.ParseSQL("SELECT x FROM ms")
	rs, err := tsql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if rs == nil || len(rs.Rows) == 0 {
		t.Fatal("expected a row")
	}
}

func TestExecuteStreamsCSVAndJSONLines(t *testing.T) {
	db := setupTestDB(t)

	t.Run("csv", func(t *testing.T) {
		cfg := &Config{Tenant: "default", Mode: ModeCSV, Header: true}
		var out bytes.Buffer
		if _, err := execute(context.Background(), db, cfg, "SELECT id, name FROM users", &out); err != nil {
			t.Fatalf("execute CSV: %v", err)
		}
		if got, want := out.String(), "id,name\n1,Alice\n2,Bob\n"; got != want {
			t.Fatalf("CSV output = %q, want %q", got, want)
		}
	})

	for _, mode := range []OutputMode{ModeJSONL, ModeNDJSON} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := &Config{Tenant: "default", Mode: mode}
			var out bytes.Buffer
			if _, err := execute(context.Background(), db, cfg, "SELECT id, name FROM users", &out); err != nil {
				t.Fatalf("execute %s: %v", mode, err)
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != 2 {
				t.Fatalf("%s output has %d lines, want 2: %q", mode, len(lines), out.String())
			}
			var first map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
				t.Fatalf("first JSON line: %v", err)
			}
			if first["name"] != "Alice" {
				t.Fatalf("first JSON line = %#v, want Alice", first)
			}
		})
	}

	t.Run("json-array", func(t *testing.T) {
		cfg := &Config{Tenant: "default", Mode: ModeJSON}
		var out bytes.Buffer
		if _, err := execute(context.Background(), db, cfg, "SELECT id, name FROM users", &out); err != nil {
			t.Fatalf("execute JSON: %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
			t.Fatalf("streamed JSON is invalid: %v\n%s", err, out.String())
		}
		if len(rows) != 2 || rows[1]["name"] != "Bob" {
			t.Fatalf("JSON rows = %#v", rows)
		}
	})
}

// blockingFirstWrite proves that CSV output reaches its first record while the
// direct ResultStream is still live. Its source table is pinned by identity,
// so a concurrent writer may finish through copy-on-write without changing the
// rows the slow CSV writer will subsequently receive.
type blockingFirstWrite struct {
	bytes.Buffer
	first       chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func newBlockingFirstWrite() *blockingFirstWrite {
	return &blockingFirstWrite{first: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingFirstWrite) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.first)
		<-w.release
	})
	return w.Buffer.Write(p)
}

func (w *blockingFirstWrite) unblock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func TestExecuteCSVStreamsFirstRowBeforeQueryCompletes(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE stream_probe (id INT)",
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 256; i++ {
		stmt, err := tsql.ParseSQL(fmt.Sprintf("INSERT INTO stream_probe VALUES (%d)", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}

	w := newBlockingFirstWrite()
	defer w.unblock()
	execDone := make(chan error, 1)
	go func() {
		_, err := execute(context.Background(), db, &Config{Tenant: "default", Mode: ModeCSV, Header: false}, "SELECT id FROM stream_probe", w)
		execDone <- err
	}()
	select {
	case <-w.first:
	case <-time.After(2 * time.Second):
		t.Fatal("first CSV result was not written")
	}

	mutationDone := make(chan error, 1)
	go func() {
		stmt, err := tsql.ParseSQL("INSERT INTO stream_probe VALUES (999)")
		if err == nil {
			_, err = tsql.Execute(context.Background(), db, "default", stmt)
		}
		mutationDone <- err
	}()
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatalf("mutation while streamed writer is blocked: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mutation remained blocked behind a slow streamed CSV writer")
	}

	w.unblock()
	if err := <-execDone; err != nil {
		t.Fatalf("streamed execute: %v", err)
	}
	rows := strings.Split(strings.TrimSpace(w.String()), "\n")
	if len(rows) != 256 {
		t.Fatalf("CSV rows = %d, want original snapshot of 256", len(rows))
	}
	for _, row := range rows {
		if row == "999" {
			t.Fatalf("CSV stream included concurrent row: %q", w.String())
		}
	}
}

type cancelAfterFirstWrite struct {
	bytes.Buffer
	cancel context.CancelFunc
	once   sync.Once
}

func (w *cancelAfterFirstWrite) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.once.Do(w.cancel)
	return n, err
}

var errBenchmarkFirstWrite = errors.New("stop after first emitted row")

type stopAfterFirstWrite struct{}

func (stopAfterFirstWrite) Write([]byte) (int, error) { return 0, errBenchmarkFirstWrite }

// BenchmarkCLIStreamFirstRow measures parse, planning, stream startup and
// the first list-mode write through the CLI's actual execution path. List mode
// has no header, so the failed write occurs only when the first result row is
// ready; execute closes the producer before returning the sentinel error.
func BenchmarkCLIStreamFirstRow(b *testing.B) {
	db := tsql.NewDB()
	ctx := context.Background()
	if _, err := tsql.Execute(ctx, db, "default", tsql.MustParseSQL("CREATE TABLE cli_bench_rows (id INT)")); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "cli_bench_rows")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, 20000)
	for i := range table.Rows {
		table.Rows[i] = []any{i}
	}
	table.Version++
	cfg := &Config{Tenant: "default", Mode: ModeList}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := execute(ctx, db, cfg, "SELECT id FROM cli_bench_rows", stopAfterFirstWrite{})
		if !errors.Is(err, errBenchmarkFirstWrite) {
			b.Fatalf("execute = %v, want first-write sentinel", err)
		}
	}
}

func TestExecuteStreamingHonorsContextCancellation(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()
	for _, sql := range []string{"CREATE TABLE cancel_probe (id INT)"} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 512; i++ {
		stmt, err := tsql.ParseSQL(fmt.Sprintf("INSERT INTO cancel_probe VALUES (%d)", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}

	queryCtx, cancel := context.WithCancel(context.Background())
	w := &cancelAfterFirstWrite{cancel: cancel}
	_, err := execute(queryCtx, db, &Config{Tenant: "default", Mode: ModeJSONL}, "SELECT id FROM cancel_probe", w)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("streamed execute error = %v, want context.Canceled", err)
	}

	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	defer writeCancel()
	stmt, err := tsql.ParseSQL("INSERT INTO cancel_probe VALUES (999)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tsql.Execute(writeCtx, db, "default", stmt); err != nil {
		t.Fatalf("stream cancellation did not release database lock: %v", err)
	}
}

func TestStorageFlagsAndOpenDatabase(t *testing.T) {
	fs := flag.NewFlagSet("storage", flag.ContinueOnError)
	flags := addStorageFlags(fs)
	if err := fs.Parse([]string{"-storage", "hybrid", "-memory-limit", "2MiB", "-read-only", "-wal-sync", "normal", "-sync-on-mutate", "-compress"}); err != nil {
		t.Fatal(err)
	}
	opts, err := flags.options()
	if err != nil {
		t.Fatal(err)
	}
	if opts.Mode != "hybrid" || opts.MemoryLimit != 2<<20 || !opts.ReadOnly || opts.WALSync != tsql.WALSyncNormal || !opts.SyncOnMutate || !opts.CompressFiles {
		t.Fatalf("storage options = %#v", opts)
	}

	dir := filepath.Join(t.TempDir(), "disk-db")
	db, savePath, err := openDatabaseWithOptions(dir, cliStorageOptions{Mode: "disk", WALSync: tsql.WALSyncFull})
	if err != nil {
		t.Fatalf("open disk backend: %v", err)
	}
	if savePath != "" {
		t.Fatalf("disk backend save path = %q, want managed storage", savePath)
	}
	if got := db.StorageMode(); got != tsql.ModeDisk {
		t.Fatalf("storage mode = %s, want disk", got)
	}
	stmt, err := tsql.ParseSQL("CREATE TABLE persisted (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tsql.Execute(context.Background(), db, "default", stmt); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close disk backend: %v", err)
	}

	reopened, _, err := openDatabaseWithOptions(dir, cliStorageOptions{Mode: "disk", WALSync: tsql.WALSyncFull, ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen disk backend read-only: %v", err)
	}
	defer reopened.Close()
	if !reopened.TableExists("default", "persisted") {
		t.Fatal("persisted table missing after reopening disk backend")
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"SELECT 1; SELECT 2", 2},
		{"SELECT 'a;b' FROM t", 1},
		{"CREATE TABLE t (x INT)", 1},
		{"", 0},
	}
	for _, tc := range tests {
		got := splitStatements(tc.input)
		if len(got) != tc.want {
			t.Errorf("splitStatements(%q): got %d stmts, want %d", tc.input, len(got), tc.want)
		}
	}
}

func TestFmtScalar(t *testing.T) {
	tests := []struct {
		v       any
		nullVal string
		want    string
	}{
		{nil, "", ""},
		{nil, "NULL", "NULL"},
		{"hello", "", "hello"},
		{float64(42), "", "42"},
		{float64(3.14), "", "3.14"},
	}
	for _, tc := range tests {
		got := fmtScalar(tc.v, tc.nullVal)
		if got != tc.want {
			t.Errorf("fmtScalar(%v, %q) = %q, want %q", tc.v, tc.nullVal, got, tc.want)
		}
	}
}

func TestOpenDatabase_Memory(t *testing.T) {
	db, savePath, err := openDatabase(":memory:")
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
	if savePath != "" {
		t.Errorf("expected empty savePath for :memory:, got %q", savePath)
	}
}

func TestOpenDatabase_NewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, savePath, err := openDatabase(path)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
	if savePath != path {
		t.Errorf("expected savePath=%q, got %q", path, savePath)
	}
}

func TestPrintTables(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	printTables(&buf, db, "default")
	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected 'users' in tables list, got:\n%s", out)
	}
}

func TestPrintSchema(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	if err := printSchema(&buf, db, "default", "users"); err != nil {
		t.Fatalf("printSchema: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CREATE TABLE users") {
		t.Errorf("expected 'CREATE TABLE users', got:\n%s", out)
	}
}

// Ensure the Repl handleMeta dispatches new commands without error.
func TestReplHandleMeta_NewCommands(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn, Header: true}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	commands := []string{".help", ".tables", ".schema", ".count", ".stats"}
	for _, cmd := range commands {
		buf.Reset()
		if err := r.handleMeta(cmd); err != nil {
			t.Errorf("handleMeta(%q) error: %v", cmd, err)
		}
		if buf.Len() == 0 {
			t.Errorf("handleMeta(%q) produced no output", cmd)
		}
	}
}

func TestReplHandleMeta_Mode(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	// Show current mode
	if err := r.handleMeta(".mode"); err != nil {
		t.Fatalf("handleMeta(.mode): %v", err)
	}
	if !strings.Contains(buf.String(), "column") {
		t.Errorf("expected current mode in output, got:\n%s", buf.String())
	}

	// Change mode
	if err := r.handleMeta(".mode json"); err != nil {
		t.Fatalf("handleMeta(.mode json): %v", err)
	}
	if cfg.Mode != ModeJSON {
		t.Errorf("expected mode=json, got %s", cfg.Mode)
	}
}

func TestReplHandleMeta_Timer(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default"}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	if err := r.handleMeta(".timer on"); err != nil {
		t.Fatalf("handleMeta(.timer on): %v", err)
	}
	if !cfg.Timer {
		t.Error("expected timer to be on")
	}

	buf.Reset()
	if err := r.handleMeta(".timer"); err != nil {
		t.Fatalf("handleMeta(.timer): %v", err)
	}
	if !strings.Contains(buf.String(), "on") {
		t.Errorf("expected timer status, got:\n%s", buf.String())
	}
}

func TestReplHandleMeta_Dump(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default", Mode: ModeColumn}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	if err := r.handleMeta(".dump users"); err != nil {
		t.Fatalf("handleMeta(.dump users): %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CREATE TABLE users") {
		t.Errorf("expected CREATE TABLE in dump, got:\n%s", out)
	}
	if !strings.Contains(out, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in dump, got:\n%s", out)
	}
}

func TestReplHandleMeta_Unknown(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Tenant: "default"}
	var buf bytes.Buffer
	r := NewRepl(db, cfg, "", &buf)

	err := r.handleMeta(".nonexistent")
	if err == nil {
		t.Error("expected error for unknown meta command")
	}
	if !strings.Contains(fmt.Sprint(err), "unknown") {
		t.Errorf("expected 'unknown' in error, got: %v", err)
	}
}
