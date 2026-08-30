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

func TestWorkloadProfilesExposeStorageAndPoolTuning(t *testing.T) {
	tests := []struct {
		name string
		cfg  OpenConfig
		want map[string]string
	}{
		{
			name: "offline_navigation", cfg: OfflineNavigationOpenConfig("./nav-artifact"),
			want: map[string]string{"mode": "index", "max_memory_bytes": "268435456", "read_only": "1", "pool_readers": "4"},
		},
		{
			name: "rag", cfg: RAGOpenConfig("./rag-artifact"),
			want: map[string]string{"mode": "hybrid", "max_memory_bytes": "536870912", "sync_on_mutate": "1", "compress_files": "1", "pool_readers": "8"},
		},
		{
			name: "embedded_tool", cfg: EmbeddedToolOpenConfig("./tool.db"),
			want: map[string]string{"mode": "advanced_wal", "wal_sync": "normal", "checkpoint_every": "1000", "checkpoint_interval": "5m0s", "checkpoint_max_bytes": "67108864"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := test.cfg.DSN()
			if err != nil {
				t.Fatal(err)
			}
			_, rawQuery := splitPublicTestDSN(dsn)
			query, err := url.ParseQuery(rawQuery)
			if err != nil {
				t.Fatal(err)
			}
			for key, want := range test.want {
				if got := query.Get(key); got != want {
					t.Errorf("%s = %q, want %q in %s", key, got, want, dsn)
				}
			}
		})
	}
}

func TestOpenConfigAdvancedOptionsValidation(t *testing.T) {
	tests := []OpenConfig{
		{Mode: "mem", StorageMode: "hybrid"},
		{Mode: "file", FilePath: "x", StorageMode: "unknown"},
		{Mode: "file", FilePath: "x", StorageMode: "wal", ReadOnly: true},
		{Mode: "file", FilePath: "x", MaxMemoryBytes: -1},
		{Mode: "file", FilePath: "x", CheckpointMaxBytes: -2},
		{Mode: "file", FilePath: "x", WALSync: "unsafe"},
		{Mode: "file", FilePath: "x", PersistDebounce: time.Microsecond},
	}
	for _, cfg := range tests {
		if _, err := cfg.DSN(); err == nil {
			t.Errorf("DSN(%+v) succeeded, want validation error", cfg)
		}
	}
}

func TestEmbeddedToolProfilePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	cfg := EmbeddedToolOpenConfig(filepath.Join(t.TempDir(), "tool.db"))
	db, err := OpenWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO settings VALUES (?, ?)`, "theme", "offline"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err := db.QueryRowContext(ctx, `SELECT value FROM settings WHERE name = ?`, "theme").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "offline" {
		t.Fatalf("persisted value = %q", value)
	}
}

func splitPublicTestDSN(dsn string) (string, string) {
	if index := strings.IndexByte(dsn, '?'); index >= 0 {
		return dsn[:index], dsn[index+1:]
	}
	return dsn, ""
}
