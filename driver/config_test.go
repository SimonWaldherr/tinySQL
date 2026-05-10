package driver

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultOpenConfigDSN(t *testing.T) {
	cfg := DefaultOpenConfig()
	dsn, err := cfg.DSN()
	if err != nil {
		t.Fatalf("DSN() returned error: %v", err)
	}
	if !strings.HasPrefix(dsn, "mem://?") {
		t.Fatalf("expected mem:// DSN, got %q", dsn)
	}
	qraw := strings.TrimPrefix(dsn, "mem://?")
	q, err := url.ParseQuery(qraw)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}
	if got := q.Get("tenant"); got != "default" {
		t.Fatalf("expected tenant=default, got %q", got)
	}
	if got := q.Get("pool_readers"); got != "4" {
		t.Fatalf("expected pool_readers=4, got %q", got)
	}
	if got := q.Get("pool_writers"); got != "1" {
		t.Fatalf("expected pool_writers=1, got %q", got)
	}
	if got := q.Get("busy_timeout"); got != "250ms" {
		t.Fatalf("expected busy_timeout=250ms, got %q", got)
	}
}

func TestOpenConfigFileDSN(t *testing.T) {
	cfg := OpenConfig{
		Mode:        "file",
		FilePath:    "./data/test.db",
		Tenant:      "acme",
		Autosave:    true,
		PoolReaders: 2,
		PoolWriters: 1,
	}
	dsn, err := cfg.DSN()
	if err != nil {
		t.Fatalf("DSN() returned error: %v", err)
	}
	expectedPrefix := "file:" + filepath.Clean(cfg.FilePath) + "?"
	if !strings.HasPrefix(dsn, expectedPrefix) {
		t.Fatalf("unexpected file DSN prefix: %q", dsn)
	}
	qraw := strings.TrimPrefix(dsn, expectedPrefix)
	q, err := url.ParseQuery(qraw)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}
	if got := q.Get("tenant"); got != "acme" {
		t.Fatalf("expected tenant=acme, got %q", got)
	}
	if got := q.Get("autosave"); got != "1" {
		t.Fatalf("expected autosave=1, got %q", got)
	}
}

func TestOpenConfigValidation(t *testing.T) {
	tests := []struct {
		cfg OpenConfig
		msg string
	}{
		{cfg: OpenConfig{Mode: "file"}, msg: "requires FilePath"},
		{cfg: OpenConfig{Mode: "mem", PoolReaders: -1}, msg: "PoolReaders"},
		{cfg: OpenConfig{Mode: "mem", PoolWriters: -1}, msg: "PoolWriters"},
		{cfg: OpenConfig{Mode: "mem", BusyTimeout: -1 * time.Millisecond}, msg: "BusyTimeout"},
		{cfg: OpenConfig{Mode: "mem", MaxOpenConns: -1}, msg: "MaxOpenConns"},
		{cfg: OpenConfig{Mode: "mem", MaxIdleConns: -1}, msg: "MaxIdleConns"},
		{cfg: OpenConfig{Mode: "mem", ConnMaxLifetime: -1 * time.Second}, msg: "ConnMaxLifetime"},
		{cfg: OpenConfig{Mode: "mem", ConnMaxIdleTime: -1 * time.Second}, msg: "ConnMaxIdleTime"},
		{cfg: OpenConfig{Mode: "mem", PingTimeout: -1 * time.Second}, msg: "PingTimeout"},
		{cfg: OpenConfig{Mode: "other"}, msg: "unsupported mode"},
	}

	for _, tc := range tests {
		_, err := tc.cfg.DSN()
		if err == nil {
			t.Fatalf("expected validation error for %+v", tc.cfg)
		}
		if !strings.Contains(err.Error(), tc.msg) {
			t.Fatalf("expected error containing %q, got %q", tc.msg, err.Error())
		}
	}
}

func TestOpenWithConfig(t *testing.T) {
	cfg := DefaultOpenConfig()
	cfg.Tenant = "cfg_test"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := OpenWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenWithConfig failed: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "CREATE TABLE t (id INT, name TEXT)"); err != nil {
		t.Fatalf("CREATE failed: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (?, ?)", 1, "Alice"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	var got string
	if err := db.QueryRowContext(ctx, "SELECT name FROM t WHERE id = ?", 1).Scan(&got); err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if got != "Alice" {
		t.Fatalf("expected Alice, got %q", got)
	}
}
