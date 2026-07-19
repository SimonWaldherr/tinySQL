package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestBuildServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_server_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

func TestEqualStringSlices(t *testing.T) {
	tests := []struct {
		a, b []string
		want bool
	}{
		{[]string{"a", "b"}, []string{"a", "b"}, true},
		{[]string{"a", "b"}, []string{"b", "a"}, false}, // Order matters now
		{[]string{"a"}, []string{"a", "b"}, false},
		{[]string{"a", "b"}, []string{"a"}, false},
		{[]string{}, []string{}, true},
	}
	for _, tt := range tests {
		if got := equalStringSlices(tt.a, tt.b); got != tt.want {
			t.Errorf("equalStringSlices(%v, %v) = %v; want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestParseBoolValue(t *testing.T) {
	trueVals := []string{"1", "true", "TRUE", "yes", "on"}
	for _, v := range trueVals {
		got, err := parseBoolValue(v)
		if err != nil || !got {
			t.Fatalf("parseBoolValue(%q) = %v, %v; want true, nil", v, got, err)
		}
	}

	falseVals := []string{"0", "false", "FALSE", "no", "off"}
	for _, v := range falseVals {
		got, err := parseBoolValue(v)
		if err != nil || got {
			t.Fatalf("parseBoolValue(%q) = %v, %v; want false, nil", v, got, err)
		}
	}

	if _, err := parseBoolValue("maybe"); err == nil {
		t.Fatal("expected parseBoolValue to reject invalid value")
	}
}

func TestOpenDBFromDSN_Memory(t *testing.T) {
	db, tenant, err := openDBFromDSN("mem://?tenant=acme")
	if err != nil {
		t.Fatalf("openDBFromDSN mem failed: %v", err)
	}
	defer db.Close()

	if tenant != "acme" {
		t.Fatalf("tenant = %q, want acme", tenant)
	}
	if db.StorageMode() != storage.ModeMemory {
		t.Fatalf("storage mode = %v, want %v", db.StorageMode(), storage.ModeMemory)
	}
}

func TestOpenDBFromDSN_FileAutosaveUsesWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "srv.db")
	db, tenant, err := openDBFromDSN("file:" + path + "?tenant=default&autosave=1")
	if err != nil {
		t.Fatalf("openDBFromDSN file autosave failed: %v", err)
	}
	defer db.Close()

	if tenant != "default" {
		t.Fatalf("tenant = %q, want default", tenant)
	}
	if db.StorageMode() != storage.ModeWAL {
		t.Fatalf("storage mode = %v, want %v", db.StorageMode(), storage.ModeWAL)
	}
}

func TestOpenDBFromDSN_FileModeOverride(t *testing.T) {
	dir := t.TempDir()
	db, tenant, err := openDBFromDSN("file:" + dir + "?tenant=t1&mode=disk")
	if err != nil {
		t.Fatalf("openDBFromDSN file mode override failed: %v", err)
	}
	defer db.Close()

	if tenant != "t1" {
		t.Fatalf("tenant = %q, want t1", tenant)
	}
	if db.StorageMode() != storage.ModeDisk {
		t.Fatalf("storage mode = %v, want %v", db.StorageMode(), storage.ModeDisk)
	}
}

func TestServerNormalizeSQL(t *testing.T) {
	s := &server{maxSQLBytes: 5}
	if _, err := s.normalizeSQL("   "); err == nil {
		t.Fatal("expected empty SQL to fail")
	}
	if _, err := s.normalizeSQL("SELECT 1"); err == nil {
		t.Fatal("expected oversized SQL to fail")
	}
	sqlText, err := s.normalizeSQL("  abc  ")
	if err != nil {
		t.Fatalf("normalizeSQL failed: %v", err)
	}
	if sqlText != "abc" {
		t.Fatalf("normalizeSQL returned %q, want abc", sqlText)
	}
}

func TestOpenDBFromDSN_Invalid(t *testing.T) {
	if _, _, err := openDBFromDSN("invalid://dsn"); err == nil {
		t.Fatal("expected unsupported DSN error")
	}
	if _, _, err := openDBFromDSN("file:?autosave=1"); err == nil {
		t.Fatal("expected file DSN with missing path to fail")
	}
	if _, _, err := openDBFromDSN("mem://?mode=disk"); err == nil {
		t.Fatal("expected mem DSN with disk mode and missing path to fail")
	}
}

func TestParseTrustedProxyCIDRs(t *testing.T) {
	nets, err := parseTrustedProxyCIDRs("127.0.0.1,10.0.0.0/8")
	if err != nil {
		t.Fatalf("parseTrustedProxyCIDRs failed: %v", err)
	}
	if len(nets) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(nets))
	}
	if !nets[0].Contains(net.ParseIP("127.0.0.1")) {
		t.Fatal("trusted proxy set should contain 127.0.0.1")
	}
	if !nets[1].Contains(net.ParseIP("10.1.2.3")) {
		t.Fatal("trusted proxy CIDR should contain 10.1.2.3")
	}
	if _, err := parseTrustedProxyCIDRs("bad-ip"); err == nil {
		t.Fatal("expected invalid trusted proxy to fail")
	}
}

func TestParseTLSMinVersion(t *testing.T) {
	if got, err := parseTLSMinVersion("1.2"); err != nil || got != tls.VersionTLS12 {
		t.Fatalf("expected tls 1.2 to pass: %v", err)
	}
	if got, err := parseTLSMinVersion("1.3"); err != nil || got != tls.VersionTLS13 {
		t.Fatalf("expected tls 1.3 to pass: %v", err)
	}
	if got, err := parseTLSMinVersion(""); err != nil || got != tls.VersionTLS12 {
		t.Fatalf("expected default tls 1.2, got %x, %v", got, err)
	}
	if _, err := parseTLSMinVersion("1.1"); err == nil {
		t.Fatal("expected tls 1.1 to fail")
	}
}

func TestServerDSNHelpers(t *testing.T) {
	if got := normalizeDSN("  "); got != "mem://?tenant=default" {
		t.Fatalf("empty DSN normalized to %q", got)
	}
	if got := normalizeDSN(" file:/tmp/db "); got != "file:/tmp/db" {
		t.Fatalf("trimmed DSN = %q", got)
	}
	if got := dsnTenant(url.Values{}); got != "default" {
		t.Fatalf("empty tenant = %q", got)
	}
	if got := dsnTenant(url.Values{"tenant": []string{" acme "}}); got != "acme" {
		t.Fatalf("tenant = %q", got)
	}
	if _, err := parseDSNQuery("%zz", "mem"); err == nil {
		t.Fatal("expected invalid query to fail")
	}

	mode, err := parseStorageModeOrDefault(url.Values{})
	if err != nil || mode != storage.ModeMemory {
		t.Fatalf("default mode = %v, %v", mode, err)
	}
	mode, err = parseStorageModeOrDefault(url.Values{"mode": []string{"wal"}})
	if err != nil || mode != storage.ModeWAL {
		t.Fatalf("wal mode = %v, %v", mode, err)
	}
	if _, err := parseStorageModeOrDefault(url.Values{"mode": []string{"bad"}}); err == nil {
		t.Fatal("expected bad mode to fail")
	}

	path, raw, err := parseFileDSNParts("file:/tmp/example.db?tenant=t1")
	if err != nil || path != "/tmp/example.db" || raw != "tenant=t1" {
		t.Fatalf("parseFileDSNParts = %q, %q, %v", path, raw, err)
	}
	if _, _, err := parseFileDSNParts("file:   ?tenant=t1"); err == nil {
		t.Fatal("expected blank file path to fail")
	}

	mode, err = parseFileDSNMode(url.Values{"autosave": []string{"1"}})
	if err != nil || mode != storage.ModeWAL {
		t.Fatalf("autosave mode = %v, %v", mode, err)
	}
	mode, err = parseFileDSNMode(url.Values{"autosave": []string{"0"}})
	if err != nil || mode != storage.ModeMemory {
		t.Fatalf("autosave false mode = %v, %v", mode, err)
	}
	if _, err := parseFileDSNMode(url.Values{"autosave": []string{"maybe"}}); err == nil {
		t.Fatal("expected bad autosave to fail")
	}
}

func TestServerStorageOptionHelpers(t *testing.T) {
	values := url.Values{
		"compress":            []string{"true"},
		"sync_on_mutate":      []string{"1"},
		"max_memory_mb":       []string{"2"},
		"checkpoint_every":    []string{"7"},
		"checkpoint_interval": []string{"2s"},
	}
	cfg := storage.StorageConfig{}
	if err := applyStorageOptions(values, &cfg); err != nil {
		t.Fatalf("applyStorageOptions failed: %v", err)
	}
	if !cfg.CompressFiles || !cfg.SyncOnMutate || cfg.MaxMemoryBytes != 2*1024*1024 || cfg.CheckpointEvery != 7 || cfg.CheckpointInterval != 2*time.Second {
		t.Fatalf("unexpected storage config: %#v", cfg)
	}

	var b bool
	if err := parseOptionalBool(url.Values{}, "missing", &b); err != nil {
		t.Fatalf("missing bool failed: %v", err)
	}
	if err := parseOptionalBool(url.Values{"x": []string{"maybe"}}, "x", &b); err == nil {
		t.Fatal("expected bad bool to fail")
	}

	var i int64
	if err := parseOptionalInt64(url.Values{"n": []string{"-1"}}, "n", &i); err == nil {
		t.Fatal("expected negative int64 to fail")
	}
	if err := parseOptionalInt64(url.Values{"n": []string{"abc"}}, "n", &i); err == nil {
		t.Fatal("expected invalid int64 to fail")
	}

	var u uint64
	if err := parseOptionalUint64(url.Values{"n": []string{"abc"}}, "n", &u); err == nil {
		t.Fatal("expected invalid uint64 to fail")
	}

	var d time.Duration
	if err := parseOptionalDuration(url.Values{"d": []string{"-1s"}}, "d", &d); err == nil {
		t.Fatal("expected negative duration to fail")
	}
	if err := parseOptionalDuration(url.Values{"d": []string{"bad"}}, "d", &d); err == nil {
		t.Fatal("expected invalid duration to fail")
	}
	if err := applyStorageOptions(url.Values{"max_memory_mb": []string{"-1"}}, &storage.StorageConfig{}); err == nil {
		t.Fatal("expected negative max_memory_mb to fail")
	}
}

func TestClientIPFromRequest_TrustedProxy(t *testing.T) {
	nets, err := parseTrustedProxyCIDRs("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{trustedProxies: nets}
	r := httptest.NewRequest("GET", "http://example.com", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("X-Forwarded-For", "203.0.113.10, 127.0.0.1")

	if got := s.clientIPFromRequest(r); got != "203.0.113.10" {
		t.Fatalf("clientIPFromRequest = %q, want 203.0.113.10", got)
	}
}

func TestClientIPFromRequest_UntrustedProxy(t *testing.T) {
	s := &server{}
	r := httptest.NewRequest("GET", "http://example.com", nil)
	r.RemoteAddr = "198.51.100.5:8080"
	r.Header.Set("X-Forwarded-For", "203.0.113.10")

	if got := s.clientIPFromRequest(r); got != "198.51.100.5" {
		t.Fatalf("clientIPFromRequest = %q, want 198.51.100.5", got)
	}
}

func TestMetricsRegistryPrometheusText(t *testing.T) {
	m := newMetricsRegistry()
	m.Observe("http", "/api/query", "POST", 200, 50*time.Millisecond)
	m.Observe("grpc", "/tinysql.TinySQL/Query", "UNARY", 0, 20*time.Millisecond)

	text := m.PrometheusText()
	if !strings.Contains(text, "tinysql_requests_total") {
		t.Fatal("expected requests metric in prometheus output")
	}
	if !strings.Contains(text, "tinysql_request_duration_seconds_bucket") {
		t.Fatal("expected duration bucket metric in prometheus output")
	}
}

func TestWithRequestTimeoutOverride(t *testing.T) {
	s := &server{requestTimeout: 50 * time.Millisecond}

	if _, _, err := s.withRequestTimeoutOverride(context.Background(), -1); err == nil {
		t.Fatal("expected negative timeout to fail")
	}

	ctx, cancel, err := s.withRequestTimeoutOverride(context.Background(), 200)
	if err != nil {
		t.Fatalf("withRequestTimeoutOverride failed: %v", err)
	}
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected deadline to be set")
	}
	if rem := time.Until(deadline); rem <= 0 || rem > 65*time.Millisecond {
		t.Fatalf("expected capped timeout around 50ms, got %s", rem)
	}
}

func TestParsePeerListDedup(t *testing.T) {
	got := parsePeerList("node1:9090,node2:9090,node1:9090, ,node2:9090,node3:9090")
	want := []string{"node1:9090", "node2:9090", "node3:9090"}
	if len(got) != len(want) {
		t.Fatalf("parsePeerList len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parsePeerList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestHandleClusterStatusNoPeers(t *testing.T) {
	s := &server{peers: nil}
	req := httptest.NewRequest(http.MethodGet, "/api/cluster/status", nil)
	rec := httptest.NewRecorder()

	s.handleClusterStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body struct {
		OK           bool `json:"ok"`
		Cluster      bool `json:"cluster"`
		PeerCount    int  `json:"peer_count"`
		HealthyPeers int  `json:"healthy_peers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.OK {
		t.Fatal("expected ok=true")
	}
	if body.Cluster {
		t.Fatal("expected cluster=false for empty peer list")
	}
	if body.PeerCount != 0 || body.HealthyPeers != 0 {
		t.Fatalf("unexpected peer counters: %+v", body)
	}
}

func TestTruncateRows(t *testing.T) {
	rows := []map[string]any{{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}}

	if out, truncated := truncateRows(rows, 0, 0); truncated || len(out) != 4 {
		t.Fatalf("no caps: got len=%d truncated=%v, want len=4 truncated=false", len(out), truncated)
	}

	out, truncated := truncateRows(rows, 2, 0)
	if !truncated || len(out) != 2 {
		t.Fatalf("max-rows cap: got len=%d truncated=%v, want len=2 truncated=true", len(out), truncated)
	}

	// Each encoded row ({"a":N}) is 7 bytes; a tiny byte cap must truncate
	// before exhausting the row count.
	out, truncated = truncateRows(rows, 0, 5)
	if !truncated || len(out) >= len(rows) {
		t.Fatalf("max-bytes cap: got len=%d truncated=%v, want len<%d truncated=true", len(out), truncated, len(rows))
	}
}

func TestQueryTruncatesRows(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()

	s := &server{
		db:              db,
		cache:           engine.NewQueryCache(10),
		defaultT:        "default",
		maxResponseRows: 2,
	}

	ctx := context.Background()
	if _, err := s.Exec(ctx, &execRequest{Tenant: "default", SQL: "CREATE TABLE t (id INT)"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 5; i++ {
		sql := fmt.Sprintf("INSERT INTO t VALUES (%d)", i)
		if _, err := s.Exec(ctx, &execRequest{Tenant: "default", SQL: sql}); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	resp, err := s.Query(ctx, &queryRequest{Tenant: "default", SQL: "SELECT id FROM t"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("query error: %s", resp.Error)
	}
	if !resp.Truncated {
		t.Fatal("expected response to be marked truncated")
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("len(Rows) = %d, want 2 (capped by maxResponseRows)", len(resp.Rows))
	}
	if resp.Count != 2 {
		t.Fatalf("Count = %d, want 2", resp.Count)
	}

	// Raising the cap above the row count must not mark the response truncated.
	s.maxResponseRows = 100
	resp, err = s.Query(ctx, &queryRequest{Tenant: "default", SQL: "SELECT id FROM t"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if resp.Truncated {
		t.Fatal("expected response not to be truncated when under the cap")
	}
	if len(resp.Rows) != 5 {
		t.Fatalf("len(Rows) = %d, want 5", len(resp.Rows))
	}
}

// TestInstrumentHTTPAlwaysLogsFailures verifies that a non-2xx HTTP response
// is logged even with verbose logging (-v) disabled, while a successful
// response stays silent -- matching the "silent by default" fix that made
// failures always observable without leaking request content into the log.
func TestInstrumentHTTPAlwaysLogsFailures(t *testing.T) {
	s := &server{verbose: false, metrics: newMetricsRegistry()}

	var buf bytes.Buffer
	origOutput := log.Writer()
	origFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	}()

	failing := s.instrumentHTTP("/api/test", func(w http.ResponseWriter, r *http.Request) {
		writeErrorJSON(w, http.StatusBadRequest, "boom")
	})
	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	rec := httptest.NewRecorder()
	failing(rec, req)

	logged := buf.String()
	if !strings.Contains(logged, "FAILED") {
		t.Fatalf("expected a FAILED log line for a non-2xx response with verbose=false, got: %q", logged)
	}
	if !strings.Contains(logged, "route=/api/test") || !strings.Contains(logged, "status=400") {
		t.Fatalf("expected FAILED log to include route and status, got: %q", logged)
	}

	buf.Reset()
	ok := s.instrumentHTTP("/api/ok", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	req2 := httptest.NewRequest(http.MethodGet, "/api/ok", nil)
	rec2 := httptest.NewRecorder()
	ok(rec2, req2)

	if strings.Contains(buf.String(), "FAILED") {
		t.Fatalf("expected no failure log for a 200 response, got: %q", buf.String())
	}
}
