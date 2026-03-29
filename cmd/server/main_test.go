package main

import (
	"context"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if _, err := parseTLSMinVersion("1.2"); err != nil {
		t.Fatalf("expected tls 1.2 to pass: %v", err)
	}
	if _, err := parseTLSMinVersion("1.3"); err != nil {
		t.Fatalf("expected tls 1.3 to pass: %v", err)
	}
	if _, err := parseTLSMinVersion("1.1"); err == nil {
		t.Fatal("expected tls 1.1 to fail")
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
