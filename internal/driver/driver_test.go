package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
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

func TestParseDSNMode(t *testing.T) {
	c, err := parseDSN("file:./test.db?tenant=t&mode=json")
	if err != nil {
		t.Fatalf("parseDSN returned error: %v", err)
	}
	if !c.modeSet || c.mode != storage.ModeJSON {
		t.Fatalf("expected mode=json, got modeSet=%v mode=%v", c.modeSet, c.mode)
	}

	if _, err := parseDSN("file:./test.db?mode=bogus"); err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

// TestDriverModeJSONPersistsAndReopens exercises mode=json end-to-end
// through database/sql: data written through one connection must survive a
// full close/reopen cycle as human-readable per-table JSON files on disk —
// previously the driver could only produce ModeMemory-style GOB snapshots
// via LoadFromFile/SaveToFile, with no way to reach ModeDisk/ModeJSON/
// ModeHybrid/ModeWAL from a database/sql DSN at all.
func TestDriverModeJSONPersistsAndReopens(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "db") + "?tenant=default&mode=json"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE notes (id INT, body TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO notes VALUES (1, 'hello json mode')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	tablePath := filepath.Join(dir, "db", "default", "notes.json")
	data, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatalf("expected %s on disk: %v", tablePath, err)
	}
	if !strings.Contains(string(data), "hello json mode") {
		t.Fatalf("expected readable row content in JSON file, got: %s", data)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	row := reopened.QueryRow(`SELECT body FROM notes WHERE id = 1`)
	var body string
	if err := row.Scan(&body); err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	if body != "hello json mode" {
		t.Fatalf("got %q, want %q", body, "hello json mode")
	}
}

// TestDriverModeRequiresFilePath guards the mode= validation added
// alongside JSON-mode DSN support: a non-memory mode with no file path is a
// clear configuration error, not a silent fallback to in-memory.
func TestDriverModeRequiresFilePath(t *testing.T) {
	if _, err := sql.Open("tinysql", "mem://?mode=disk"); err != nil {
		// sql.Open itself is lazy and may not error until first use.
		return
	}
	db, _ := sql.Open("tinysql", "mem://?mode=disk")
	defer db.Close()
	if _, err := db.Exec(`SELECT 1`); err == nil {
		t.Fatal("expected error opening mode=disk without a file path")
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

func TestParseDSNStorageOptionsAreStrictAndComplete(t *testing.T) {
	c, err := parseDSN("file:/tmp/tiny?tenant=tiles&mode=index&autosave=1&pool_readers=4&pool_writers=2&busy_timeout=125ms&max_memory_bytes=64MiB&read_only=0&sync_on_mutate=true&compress_files=false&checkpoint_every=9&checkpoint_interval=2s&checkpoint_max_bytes=512MiB")
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	if c.maxMemoryBytes != 64<<20 || c.checkpointMaxBytes != 512<<20 {
		t.Fatalf("byte options = memory %d checkpoint %d", c.maxMemoryBytes, c.checkpointMaxBytes)
	}
	if c.checkpointEvery != 9 || c.checkpointInterval != 2*time.Second || !c.syncOnMutate || c.compressFiles || c.readOnly {
		t.Fatalf("storage options not preserved: %#v", c)
	}
	for _, dsn := range []string{
		"mem://?max_memory_bytes=not-a-size",
		"mem://?read_only=maybe",
		"mem://?checkpoint_interval=-1s",
		"mem://?unknown_option=1",
		"mem://?tenant=a&tenant=b",
	} {
		if _, err := parseDSN(dsn); err == nil {
			t.Fatalf("expected strict parse error for %q", dsn)
		}
	}
}

func TestConnectorSharesOneStorageDBPerSQLDB(t *testing.T) {
	var (
		mu     sync.Mutex
		opened = make(map[*storage.DB]int)
	)
	serverOpenHook.Lock()
	if serverOpenHook.fn != nil {
		serverOpenHook.Unlock()
		t.Fatal("test hook already installed")
	}
	serverOpenHook.fn = func(db *storage.DB, _ cfg) {
		mu.Lock()
		opened[db]++
		mu.Unlock()
	}
	serverOpenHook.Unlock()
	t.Cleanup(func() {
		serverOpenHook.Lock()
		serverOpenHook.fn = nil
		serverOpenHook.Unlock()
	})

	// A named mem DSN is deliberately used here: it must not inherit a
	// SetDefaultDB installed by another embedding test.
	db, err := sql.Open("tinysql", "mem://?tenant=connector_shared&pool_readers=8")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(`CREATE TABLE probe (id INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO probe VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	ready := make(chan error, 8)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := db.Begin()
			if err == nil {
				var n int
				err = tx.QueryRow(`SELECT id FROM probe WHERE id = 1`).Scan(&n)
				if err == nil && n != 1 {
					err = fmt.Errorf("query returned %d, want 1", n)
				}
			}
			ready <- err
			<-release // Keep the physical connection checked out.
			if tx != nil {
				_ = tx.Rollback()
			}
		}()
	}
	for range 8 {
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	wg.Wait()

	mu.Lock()
	if len(opened) != 1 {
		mu.Unlock()
		t.Fatalf("physical connections opened %d storage DBs, want one", len(opened))
	}
	mu.Unlock()

	// A second sql.Open gets a second Connector and therefore an isolated
	// in-memory database even when its DSN text is identical.
	separate, err := sql.Open("tinysql", "mem://?tenant=connector_shared")
	if err != nil {
		t.Fatal(err)
	}
	defer separate.Close()
	if err := separate.QueryRow(`SELECT id FROM probe`).Scan(new(int)); err == nil {
		t.Fatal("separate sql.Open unexpectedly observed the first mem:// database")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(opened) != 2 {
		t.Fatalf("separate sql.Open constructed %d storage DBs, want two", len(opened))
	}
}

func TestConnectorForwardsStorageConfig(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "artifact")
	dsn := "file:" + dir + "?mode=index&max_memory_bytes=64MiB&sync_on_mutate=1&compress_files=1&checkpoint_every=7&checkpoint_interval=3s&checkpoint_max_bytes=512MiB"
	co, err := (&drv{}).OpenConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := co.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*conn)
	defer c.srv.db.Close()
	got := c.srv.db.Config()
	if got == nil || got.Mode != storage.ModeIndex || got.MaxMemoryBytes != 64<<20 || !got.SyncOnMutate || !got.CompressFiles || got.CheckpointEvery != 7 || got.CheckpointInterval != 3*time.Second || got.CheckpointMaxBytes != 512<<20 {
		t.Fatalf("storage config was not forwarded: %#v", got)
	}
	if stats := c.srv.db.BackendStats(); stats.MemoryLimitBytes != 64<<20 {
		t.Fatalf("buffer-pool limit = %d, want %d", stats.MemoryLimitBytes, 64<<20)
	}
}

func TestReadOnlyIndexDSNPreventsWritesAndArtifacts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "artifact")
	writeDSN := "file:" + dir + "?mode=index"
	writer, err := sql.Open("tinysql", writeDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`CREATE TABLE images (tile_id TEXT, tile_data BLOB)`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`CREATE TABLE map (zoom_level INT, tile_column INT, tile_row INT, tile_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	want := []byte{0x1f, 0x8b, 0x08, 0x00, 0xff}
	if _, err := writer.Exec(`INSERT INTO images VALUES (?, ?)`, "z/x/y", want); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`INSERT INTO map VALUES (12, 2174, 1423, 'z/x/y')`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`CREATE UNIQUE INDEX idx_map_zxy ON map(zoom_level, tile_column, tile_row)`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`CREATE UNIQUE INDEX idx_images_tile_id ON images(tile_id)`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	manifestBefore, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	tablePath := filepath.Join(dir, "default", "images.tbl")
	tableBefore, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := sql.Open("tinysql", writeDSN+"&read_only=1&max_memory_bytes=1MiB")
	if err != nil {
		t.Fatal(err)
	}
	var tileID string
	if err := reader.QueryRow(`SELECT tile_id FROM map WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?`, 12, 2174, 1423).Scan(&tileID); err != nil {
		t.Fatal(err)
	}
	if tileID != "z/x/y" {
		t.Fatalf("map point lookup = %q", tileID)
	}
	var got []byte
	if err := reader.QueryRow(`SELECT tile_data FROM images WHERE tile_id = ?`, tileID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BLOB mismatch: got %x want %x", got, want)
	}
	got[0] ^= 0xff // A caller must not mutate a cache/driver-owned BLOB.
	var again []byte
	if err := reader.QueryRow(`SELECT tile_data FROM images WHERE tile_id = ?`, "z/x/y").Scan(&again); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("BLOB scan aliased storage: got %x want %x", again, want)
	}
	if err := reader.QueryRow(`SELECT tile_data FROM images WHERE tile_id = ?`, "missing").Scan(new([]byte)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing tile error = %v, want sql.ErrNoRows", err)
	}
	explain, err := reader.Query(`EXPLAIN SELECT tile_id FROM map WHERE zoom_level = 12 AND tile_column = 2174 AND tile_row = 1423`)
	if err != nil {
		t.Fatal(err)
	}
	foundSeek := false
	for explain.Next() {
		var step int
		var operation, detail string
		if err := explain.Scan(&step, &operation, &detail); err != nil {
			explain.Close()
			t.Fatal(err)
		}
		if operation == "INDEX POINT SEEK" && strings.Contains(detail, "index=idx_map_zxy") {
			foundSeek = true
		}
	}
	if err := explain.Close(); err != nil {
		t.Fatal(err)
	}
	if !foundSeek {
		t.Fatal("EXPLAIN did not expose the composite index point seek")
	}
	if _, err := reader.Exec(`INSERT INTO images VALUES ('new', X'00')`); err == nil {
		t.Fatal("read-only INSERT unexpectedly succeeded")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	manifestAfter, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	tableAfter, _ := os.ReadFile(tablePath)
	if !reflect.DeepEqual(manifestBefore, manifestAfter) || !reflect.DeepEqual(tableBefore, tableAfter) {
		t.Fatal("read-only open changed persistent artifact")
	}

	missing := filepath.Join(t.TempDir(), "missing")
	missingDB, err := sql.Open("tinysql", "file:"+missing+"?mode=index&read_only=1")
	if err != nil {
		t.Fatal(err)
	}
	defer missingDB.Close()
	if err := missingDB.Ping(); err == nil {
		t.Fatal("read-only open of missing artifact unexpectedly succeeded")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("read-only open created %q: %v", missing, err)
	}
}

// TestPagedIndexReadOnlyConnectionsUseOnlyLocatedPages is the database/sql
// regression test for the page-oriented MBTiles path. Eight checked-out
// connections share one storage.DB, while each z/x/y -> BLOB read uses its
// persistent B+Tree index and never invokes the compatibility LoadTable path.
func TestPagedIndexReadOnlyConnectionsUseOnlyLocatedPages(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "paged-mbtiles")
	buildMBTilesLikeIndexArtifact(t, dir, 256, 4096, storage.ModePagedIndex)

	var (
		mu     sync.Mutex
		opened []*storage.DB
	)
	serverOpenHook.Lock()
	if serverOpenHook.fn != nil {
		serverOpenHook.Unlock()
		t.Fatal("test hook already installed")
	}
	serverOpenHook.fn = func(db *storage.DB, _ cfg) {
		mu.Lock()
		opened = append(opened, db)
		mu.Unlock()
	}
	serverOpenHook.Unlock()
	t.Cleanup(func() {
		serverOpenHook.Lock()
		serverOpenHook.fn = nil
		serverOpenHook.Unlock()
	})

	db, err := sql.Open("tinysql", "file:"+dir+"?mode=paged_index&read_only=1&max_memory_bytes=16KiB&pool_readers=8")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)

	ctx := context.Background()
	ready := make(chan error, 8)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			if err == nil {
				defer conn.Close()
				for n := worker; n < 256 && err == nil; n += 8 {
					var id string
					err = conn.QueryRowContext(ctx, `SELECT tile_id FROM map WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?`, 12, n, 0).Scan(&id)
					if err != nil {
						break
					}
					var payload []byte
					err = conn.QueryRowContext(ctx, `SELECT tile_data FROM images WHERE tile_id = ?`, id).Scan(&payload)
					if err == nil && len(payload) != 4096 {
						err = fmt.Errorf("payload length %d, want 4096", len(payload))
					}
				}
			}
			ready <- err
			<-release // keep all physical driver connections checked out
		}(worker)
	}
	for range 8 {
		if err := <-ready; err != nil {
			close(release)
			wg.Wait()
			t.Fatal(err)
		}
	}
	close(release)
	wg.Wait()

	mu.Lock()
	if len(opened) != 1 {
		mu.Unlock()
		t.Fatalf("opened %d storage DBs, want exactly one", len(opened))
	}
	shared := opened[0]
	mu.Unlock()
	stats := shared.BackendStats()
	if stats.LoadCount != 0 {
		t.Fatalf("point lookups called LoadTable %d times", stats.LoadCount)
	}
	if stats.PageReads == 0 || stats.CacheMisses == 0 {
		t.Fatalf("paged reads not observed: %#v", stats)
	}
	if stats.CachedPages > stats.MaxCachePages {
		t.Fatalf("read-only cache exceeded limit: %#v", stats)
	}
	if stats.TransientFrames != 0 {
		t.Fatalf("query-local pages leaked after concurrent reads: %#v", stats)
	}
	if stats.MemoryLimitBytes != 16<<10 {
		t.Fatalf("memory limit = %d, want 16384", stats.MemoryLimitBytes)
	}

	var blob []byte
	if err := db.QueryRowContext(ctx, `SELECT tile_data FROM images WHERE tile_id = ?`, "12/5/0").Scan(&blob); err != nil {
		t.Fatal(err)
	}
	blob[0] ^= 0xff
	var fresh []byte
	if err := db.QueryRowContext(ctx, `SELECT tile_data FROM images WHERE tile_id = ?`, "12/5/0").Scan(&fresh); err != nil {
		t.Fatal(err)
	}
	if fresh[0] == blob[0] {
		t.Fatal("database/sql BLOB scan aliases page-cache or driver memory")
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
	if _, err := st.(*stmt).ExecContext(context.Background(), []driver.NamedValue{{Ordinal: 1, Value: int64(7)}}); err != nil {
		t.Fatalf("stmt exec failed: %v", err)
	}
	q, err := c.Prepare("SELECT id FROM s")
	if err != nil {
		t.Fatalf("prepare query failed: %v", err)
	}
	rs, err := q.(*stmt).QueryContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("stmt query failed: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rs.Next(dest); err != nil {
		t.Fatalf("stmt rows next failed: %v", err)
	}
}

func TestPreparedSelectBindsNewValuesWithoutReparse(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatal(err)
	}
	c := rawConn.(*conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, "CREATE TABLE prepared (id INT, name TEXT)", nil); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id   int64
		name string
	}{{1, "one"}, {2, "two"}} {
		if _, err := c.ExecContext(ctx, "INSERT INTO prepared VALUES (?, ?)", []driver.NamedValue{{Ordinal: 1, Value: row.id}, {Ordinal: 2, Value: row.name}}); err != nil {
			t.Fatal(err)
		}
	}
	preparedStmt, err := c.Prepare("SELECT name FROM prepared WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	fast, ok := preparedStmt.(*stmt)
	if !ok || fast.prepared == nil {
		t.Fatal("SELECT with positional parameter did not build prepared AST")
	}
	for _, want := range []string{"one", "two"} {
		id := int64(1)
		if want == "two" {
			id = 2
		}
		rows, err := fast.QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: id}})
		if err != nil {
			t.Fatal(err)
		}
		values := make([]driver.Value, 1)
		if err := rows.Next(values); err != nil {
			t.Fatal(err)
		}
		if got, _ := values[0].(string); got != want {
			t.Fatalf("id %d: got %q, want %q", id, got, want)
		}
		_ = rows.Close()
	}

	// Range predicates also use a cached plan shape, but must read the current
	// bound value rather than the value from the first execution.
	rangeStmt, err := c.Prepare("SELECT name FROM prepared WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	rangeFast := rangeStmt.(*stmt)
	rows, err := rangeFast.QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil || values[0] != "two" {
		t.Fatalf("range id > 1: values=%v err=%v", values, err)
	}
	_ = rows.Close()
	rows, err = rangeFast.QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Next(values); err != io.EOF {
		t.Fatalf("range id > 2 returned row or error: %v (%v)", values, err)
	}
	_ = rows.Close()

	// Access-path state is not cached: adding an index after the first reads
	// must remain correct, as must a schema change that changes column count.
	if _, err := c.ExecContext(ctx, "CREATE INDEX prepared_id ON prepared(id)", nil); err != nil {
		t.Fatal(err)
	}
	table, err := c.srv.db.Get("default", "prepared")
	if err != nil {
		t.Fatal(err)
	}
	table.Cols = append(table.Cols, storage.Column{Name: "extra", Type: storage.TextType})
	for i := range table.Rows {
		table.Rows[i] = append(table.Rows[i], nil)
	}
	rows, err = fast.QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	values = make([]driver.Value, 1)
	if err := rows.Next(values); err != nil || values[0] != "two" {
		t.Fatalf("prepared query after index/schema change: values=%v err=%v", values, err)
	}
	_ = rows.Close()

	// DROP/CREATE replaces the table pointer. A cached template must not retain
	// the old table or its rows.
	if _, err := c.ExecContext(ctx, "DROP TABLE prepared", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "CREATE TABLE prepared (id INT, name TEXT)", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO prepared VALUES (3, 'three')", nil); err != nil {
		t.Fatal(err)
	}
	rows, err = fast.QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: int64(3)}})
	if err != nil {
		t.Fatal(err)
	}
	values = make([]driver.Value, 1)
	if err := rows.Next(values); err != nil || values[0] != "three" {
		t.Fatalf("prepared query after table replacement: values=%v err=%v", values, err)
	}
	_ = rows.Close()
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

func TestSQLTransactionControlCommands(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	if _, err := c.ExecContext(context.Background(), "CREATE TABLE tx_sql (id INT)", nil); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if _, err := c.ExecContext(context.Background(), "begin transaction;", nil); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO tx_sql VALUES (1)", nil); err != nil {
		t.Fatalf("insert in tx failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
	rows, err := c.QueryContext(context.Background(), "SELECT id FROM tx_sql", nil)
	if err != nil {
		t.Fatalf("select after rollback failed: %v", err)
	}
	if err := rows.Next(make([]driver.Value, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("expected no rows after rollback, got err=%v", err)
	}

	if _, err := c.ExecContext(context.Background(), "START TRANSACTION", nil); err != nil {
		t.Fatalf("START TRANSACTION failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "INSERT INTO tx_sql VALUES (2)", nil); err != nil {
		t.Fatalf("insert in second tx failed: %v", err)
	}
	if _, err := c.ExecContext(context.Background(), "COMMIT TRANSACTION", nil); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
	rows, err = c.QueryContext(context.Background(), "SELECT id FROM tx_sql", nil)
	if err != nil {
		t.Fatalf("select after commit failed: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("expected committed row: %v", err)
	}
	if dest[0] != int64(2) && dest[0] != 2 {
		t.Fatalf("committed row = %#v, want 2", dest[0])
	}
}

func TestTransactionCommitMergesUnrelatedConcurrentChanges(t *testing.T) {
	s := newServer(storage.NewDB(), cfg{})
	d := &drv{srv: s}
	raw1, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("open conn1: %v", err)
	}
	raw2, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("open conn2: %v", err)
	}
	c1 := raw1.(*conn)
	c2 := raw2.(*conn)
	ctx := context.Background()

	for _, sql := range []string{
		"CREATE TABLE tx_a (id INT)",
		"CREATE TABLE tx_b (id INT)",
	} {
		if _, err := c1.ExecContext(ctx, sql, nil); err != nil {
			t.Fatalf("setup %q: %v", sql, err)
		}
	}
	if _, err := c1.ExecContext(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("begin c1: %v", err)
	}
	if _, err := c2.ExecContext(ctx, "INSERT INTO tx_b VALUES (20)", nil); err != nil {
		t.Fatalf("concurrent insert c2: %v", err)
	}
	if _, err := c1.ExecContext(ctx, "INSERT INTO tx_a VALUES (10)", nil); err != nil {
		t.Fatalf("insert c1: %v", err)
	}
	if _, err := c1.ExecContext(ctx, "COMMIT", nil); err != nil {
		t.Fatalf("commit c1: %v", err)
	}

	for table, want := range map[string]int{"tx_a": 10, "tx_b": 20} {
		rows, err := c1.QueryContext(ctx, "SELECT id FROM "+table, nil)
		if err != nil {
			t.Fatalf("select %s: %v", table, err)
		}
		dest := make([]driver.Value, 1)
		if err := rows.Next(dest); err != nil {
			t.Fatalf("expected row in %s: %v", table, err)
		}
		if dest[0] != int64(want) && dest[0] != want {
			t.Fatalf("%s row = %#v, want %d", table, dest[0], want)
		}
	}
}

func TestTransactionCommitRejectsSameTableConflict(t *testing.T) {
	s := newServer(storage.NewDB(), cfg{})
	d := &drv{srv: s}
	raw1, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("open conn1: %v", err)
	}
	raw2, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("open conn2: %v", err)
	}
	c1 := raw1.(*conn)
	c2 := raw2.(*conn)
	ctx := context.Background()

	if _, err := c1.ExecContext(ctx, "CREATE TABLE tx_conflict (id INT)", nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c1.ExecContext(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("begin c1: %v", err)
	}
	if _, err := c2.ExecContext(ctx, "INSERT INTO tx_conflict VALUES (2)", nil); err != nil {
		t.Fatalf("insert c2: %v", err)
	}
	if _, err := c1.ExecContext(ctx, "INSERT INTO tx_conflict VALUES (1)", nil); err != nil {
		t.Fatalf("insert c1: %v", err)
	}
	if _, err := c1.ExecContext(ctx, "COMMIT", nil); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("commit error = %v, want ErrTransactionConflict", err)
	}
	if c1.inTx {
		t.Fatal("connection stayed in transaction after commit conflict")
	}

	rows, err := c2.QueryContext(ctx, "SELECT id FROM tx_conflict", nil)
	if err != nil {
		t.Fatalf("select after conflict: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("expected concurrent row to remain: %v", err)
	}
	if dest[0] != int64(2) && dest[0] != 2 {
		t.Fatalf("remaining row = %#v, want 2", dest[0])
	}
	if err := rows.Next(dest); !errors.Is(err, io.EOF) {
		t.Fatalf("expected only the concurrent row, got next err=%v row=%#v", err, dest)
	}
}

func TestQueryExplainReturnsRows(t *testing.T) {
	d := &drv{}
	rawConn, err := d.Open("mem://")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c := rawConn.(*conn)
	rows, err := c.QueryContext(context.Background(), "EXPLAIN SELECT * FROM explain_table WHERE id = 1", nil)
	if err != nil {
		t.Fatalf("EXPLAIN query failed: %v", err)
	}
	if cols := rows.Columns(); len(cols) != 3 || cols[0] != "step" || cols[1] != "operation" || cols[2] != "detail" {
		t.Fatalf("unexpected EXPLAIN columns: %#v", cols)
	}
	dest := make([]driver.Value, 3)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("expected EXPLAIN row: %v", err)
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
	input := []byte{1, 2, 3}
	nv = &driver.NamedValue{Ordinal: 1, Value: input}
	_ = c.CheckNamedValue(nv)
	b, ok := nv.Value.([]byte)
	if !ok || string(b) != string(input) {
		t.Fatalf("expected []byte to remain binary, got %#v", nv.Value)
	}
	input[0] = 9
	if b[0] != 1 {
		t.Fatalf("expected CheckNamedValue to take ownership of BLOB bytes")
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
