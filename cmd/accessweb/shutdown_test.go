package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

// The database is held in memory and only written back on the way out, so a
// regression here loses every edit made during a run. main used to save from a
// defer beneath log.Fatal(ListenAndServe(...)), which exits through os.Exit and
// runs no defers at all.
//
// OpenWithDB is the same sharing path main takes: a named DSN would give the
// handlers their own storage, so the rows written here would not be in the
// database that gets saved.
func TestServeUntilSignalPersistsOnShutdown(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "persist.db")

	nativeDB := tinysql.NewDB()
	sqlDB, err := tsqldriver.OpenWithDB(nativeDB)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(0)

	if _, err := sqlDB.Exec(`CREATE TABLE survivors (id INT, note TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO survivors (id, note) VALUES (1, 'written before shutdown')`); err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveUntilSignal(ctx, srv, sqlDB, nativeDB, dbFile) }()

	// Let the goroutine reach ListenAndServe before asking it to stop.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveUntilSignal returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serveUntilSignal did not return after cancellation")
	}

	if _, err := os.Stat(dbFile); err != nil {
		t.Fatalf("database was not written on shutdown: %v", err)
	}

	reloaded, err := tinysql.LoadFromFile(dbFile)
	if err != nil {
		t.Fatalf("reload saved database: %v", err)
	}
	// LoadFromFile opens a WAL alongside the snapshot; leaving it open blocks
	// the TempDir cleanup on Windows.
	t.Cleanup(func() { reloaded.Close() })
	table, err := reloaded.Get(defaultTenant, "survivors")
	if err != nil {
		t.Fatalf("saved database has no survivors table: %v", err)
	}
	if len(table.Rows) != 1 {
		t.Fatalf("saved database has %d rows, want 1", len(table.Rows))
	}
}

// The handles must address one database, not two. A named DSN silently gives
// the sql.DB its own storage, which left the UI writing to a database that was
// never saved and never showed the rows loaded from the file.
func TestOpenWithDBSharesStorage(t *testing.T) {
	nativeDB := tinysql.NewDB()
	sqlDB, err := tsqldriver.OpenWithDB(nativeDB)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(0)

	if _, err := sqlDB.Exec(`CREATE TABLE shared (id INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDB.Get(defaultTenant, "shared"); err != nil {
		t.Fatalf("table created through sql.DB is not visible in the native DB: %v", err)
	}
}

// An in-memory run must not create a file named ":memory:".
func TestServeUntilSignalSkipsSaveForMemoryDB(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(cwd) })

	nativeDB := tinysql.NewDB()
	sqlDB, err := tsqldriver.OpenWithDB(nativeDB)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(0)

	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveUntilSignal(ctx, srv, sqlDB, nativeDB, ":memory:") }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("in-memory run wrote %d file(s): %v", len(entries), entries)
	}
}
