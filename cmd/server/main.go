package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultRequestTimeout          = 30 * time.Second
	defaultPeerTimeout             = 10 * time.Second
	defaultShutdownTimeout         = 15 * time.Second
	defaultReadTimeout             = 15 * time.Second
	defaultReadHeaderTimeout       = 5 * time.Second
	defaultWriteTimeout            = 30 * time.Second
	defaultIdleTimeout             = 120 * time.Second
	defaultMaxBodyBytes      int64 = 1 << 20   // 1 MiB
	defaultMaxSQLBytes       int   = 256 << 10 // 256 KiB
	defaultMaxHeaderBytes          = 1 << 20   // 1 MiB
	defaultMaxGRPCMsgBytes         = 4 << 20   // 4 MiB
)

// Flags
var (
	flagDSN            = flag.String("dsn", "mem://?tenant=default", "Storage DSN (mem:// or file:/path.db?tenant=...&autosave=1)")
	flagHTTP           = flag.String("http", ":8080", "HTTP listen address (empty to disable)")
	flagAuth           = flag.String("auth", "", "Authorization token for HTTP and gRPC (optional)")
	flagGRPC           = flag.String("grpc", ":9090", "gRPC listen address (empty to disable)")
	flagPeers          = flag.String("peers", "", "Comma-separated list of gRPC peer addresses for federation")
	flagTenant         = flag.String("tenant", "default", "Default tenant if none provided in request")
	flagTrustedProxies = flag.String("trusted-proxies", "", "Comma-separated trusted proxy CIDRs/IPs for X-Forwarded-For handling")

	flagRequestTimeout  = flag.Duration("request-timeout", defaultRequestTimeout, "Maximum time per SQL request")
	flagPeerTimeout     = flag.Duration("peer-timeout", defaultPeerTimeout, "Maximum time per federated peer call")
	flagShutdownTimeout = flag.Duration("shutdown-timeout", defaultShutdownTimeout, "Graceful shutdown timeout")

	flagReadTimeout       = flag.Duration("http-read-timeout", defaultReadTimeout, "HTTP read timeout")
	flagReadHeaderTimeout = flag.Duration("http-read-header-timeout", defaultReadHeaderTimeout, "HTTP read header timeout")
	flagWriteTimeout      = flag.Duration("http-write-timeout", defaultWriteTimeout, "HTTP write timeout")
	flagIdleTimeout       = flag.Duration("http-idle-timeout", defaultIdleTimeout, "HTTP idle timeout")
	flagMaxHeaderBytes    = flag.Int("http-max-header-bytes", defaultMaxHeaderBytes, "HTTP max header bytes")

	flagMaxBodyBytes = flag.Int64("max-body-bytes", defaultMaxBodyBytes, "Maximum HTTP request body size in bytes")
	flagMaxSQLBytes  = flag.Int("max-sql-bytes", defaultMaxSQLBytes, "Maximum SQL query length in bytes")

	flagGRPCMaxRecv = flag.Int("grpc-max-recv-bytes", defaultMaxGRPCMsgBytes, "Maximum gRPC request size in bytes")
	flagGRPCMaxSend = flag.Int("grpc-max-send-bytes", defaultMaxGRPCMsgBytes, "Maximum gRPC response size in bytes")

	flagTLSMinVersion     = flag.String("tls-min-version", "1.2", "TLS minimum version: 1.2 or 1.3")
	flagHTTPTLSCert       = flag.String("http-tls-cert", "", "Path to HTTP TLS certificate (enables HTTPS when set with key)")
	flagHTTPTLSKey        = flag.String("http-tls-key", "", "Path to HTTP TLS private key")
	flagGRPCTLSCert       = flag.String("grpc-tls-cert", "", "Path to gRPC TLS certificate")
	flagGRPCTLSKey        = flag.String("grpc-tls-key", "", "Path to gRPC TLS private key")
	flagPeerTLS           = flag.Bool("peer-tls", false, "Use TLS when calling federation peers")
	flagPeerTLSCA         = flag.String("peer-tls-ca", "", "Optional CA bundle for federation peer TLS verification")
	flagPeerTLSServerName = flag.String("peer-tls-server-name", "", "Optional server name override for federation peer TLS")
	flagPeerTLSSkipVerify = flag.Bool("peer-tls-skip-verify", false, "Skip TLS certificate verification for federation peers (unsafe)")

	flagVerbose = flag.Bool("v", false, "Verbose logging")
)

// HTTP types
type execRequest struct {
	Tenant    string `json:"tenant"`
	SQL       string `json:"sql"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

type execResponse struct {
	Success      bool   `json:"success"`
	Error        string `json:"error,omitempty"`
	RowsAffected int64  `json:"rows_affected,omitempty"`
	LastInsertID int64  `json:"last_insert_id,omitempty"`
	Duration     string `json:"duration"`
}

type queryRequest struct {
	Tenant        string `json:"tenant"`
	SQL           string `json:"sql"`
	TimeoutMS     int64  `json:"timeout_ms,omitempty"`
	PeerTimeoutMS int64  `json:"peer_timeout_ms,omitempty"`
}

type queryResponse struct {
	SQL      string           `json:"sql"`
	Columns  []string         `json:"columns"`
	Rows     []map[string]any `json:"rows"`
	Error    string           `json:"error,omitempty"`
	Duration string           `json:"duration"`
	Count    int              `json:"count"`
}

// gRPC JSON codec
type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return storage.JSONMarshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// gRPC service interface and descriptors (manual, no protobuf)
type TinySQLServer interface {
	Exec(context.Context, *execRequest) (*execResponse, error)
	Query(context.Context, *queryRequest) (*queryResponse, error)
}

func registerTinySQLServer(s *grpc.Server, srv TinySQLServer) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "tinysql.TinySQL",
		HandlerType: (*TinySQLServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Exec", Handler: _TinySQL_Exec_Handler},
			{MethodName: "Query", Handler: _TinySQL_Query_Handler},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "tinysql", // informational
	}, srv)
}

func _TinySQL_Exec_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(execRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TinySQLServer).Exec(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/tinysql.TinySQL/Exec"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(TinySQLServer).Exec(ctx, req.(*execRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TinySQL_Query_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(queryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TinySQLServer).Query(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/tinysql.TinySQL/Query"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(TinySQLServer).Query(ctx, req.(*queryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// server state
type server struct {
	db             *storage.DB
	cache          *engine.QueryCache
	peers          []string
	defaultT       string
	authToken      string
	trustedProxies []*net.IPNet
	peerDialCreds  credentials.TransportCredentials
	requestTimeout time.Duration
	peerTimeout    time.Duration
	maxBodyBytes   int64
	maxSQLBytes    int
	verbose        bool
	startedAt      time.Time
	ready          atomic.Bool
	metrics        *metricsRegistry
}

func newServer(db *storage.DB, defaultTenant, authToken string, peers []string, trustedProxies []*net.IPNet, peerDialCreds credentials.TransportCredentials) *server {
	s := &server{
		db:             db,
		cache:          engine.NewQueryCache(200),
		peers:          peers,
		defaultT:       defaultTenant,
		authToken:      strings.TrimSpace(authToken),
		trustedProxies: trustedProxies,
		peerDialCreds:  peerDialCreds,
		requestTimeout: *flagRequestTimeout,
		peerTimeout:    *flagPeerTimeout,
		maxBodyBytes:   *flagMaxBodyBytes,
		maxSQLBytes:    *flagMaxSQLBytes,
		verbose:        *flagVerbose,
		startedAt:      time.Now(),
		metrics:        newMetricsRegistry(),
	}
	s.ready.Store(true)
	return s
}

func (s *server) tenantOrDefault(t string) string {
	if strings.TrimSpace(t) == "" {
		return s.defaultT
	}
	return t
}

func (s *server) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.requestTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.requestTimeout)
}

func (s *server) withRequestTimeoutOverride(ctx context.Context, timeoutMS int64) (context.Context, context.CancelFunc, error) {
	if timeoutMS < 0 {
		return nil, nil, fmt.Errorf("timeout_ms must be >= 0")
	}
	if timeoutMS == 0 {
		reqCtx, cancel := s.withRequestTimeout(ctx)
		return reqCtx, cancel, nil
	}
	d := time.Duration(timeoutMS) * time.Millisecond
	if d <= 0 {
		return nil, nil, fmt.Errorf("timeout_ms is out of range")
	}
	if s.requestTimeout > 0 && d > s.requestTimeout {
		d = s.requestTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, d)
	return reqCtx, cancel, nil
}

func (s *server) peerTimeoutOverride(timeoutMS int64) (time.Duration, error) {
	if timeoutMS < 0 {
		return 0, fmt.Errorf("peer_timeout_ms must be >= 0")
	}
	if timeoutMS == 0 {
		return s.peerTimeout, nil
	}
	d := time.Duration(timeoutMS) * time.Millisecond
	if d <= 0 {
		return 0, fmt.Errorf("peer_timeout_ms is out of range")
	}
	return d, nil
}

func (s *server) normalizeSQL(sqlText string) (string, error) {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return "", fmt.Errorf("sql must not be empty")
	}
	if s.maxSQLBytes > 0 && len(sqlText) > s.maxSQLBytes {
		return "", fmt.Errorf("sql exceeds max length (%d bytes)", s.maxSQLBytes)
	}
	return sqlText, nil
}

func (s *server) isAuthorized(token string) bool {
	if s.authToken == "" {
		return true
	}
	token = strings.TrimSpace(token)
	if len(token) != len(s.authToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.authToken)) == 1
}

func bearerToken(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		value = strings.TrimSpace(value[7:])
	}
	return value
}

func (s *server) withAuth(h http.HandlerFunc) http.HandlerFunc {
	if s.authToken == "" {
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r.Header.Get("Authorization"))
		if !s.isAuthorized(token) {
			writeErrorJSON(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h(w, r)
	}
}

func (s *server) grpcUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		statusCode := codes.OK
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic in gRPC %s: %v", info.FullMethod, rec)
				err = status.Error(codes.Internal, "internal server error")
			}
			statusCode = status.Code(err)
			s.metrics.Observe("grpc", info.FullMethod, "UNARY", int(statusCode), time.Since(start))
			if s.verbose {
				log.Printf("grpc method=%s status=%s duration=%s", info.FullMethod, statusCode.String(), time.Since(start))
			}
		}()

		if s.authToken != "" {
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return nil, status.Error(codes.Unauthenticated, "missing authorization metadata")
			}
			vals := md.Get("authorization")
			token := ""
			if len(vals) > 0 {
				token = bearerToken(vals[0])
			}
			if !s.isAuthorized(token) {
				return nil, status.Error(codes.Unauthenticated, "unauthorized")
			}
		}

		reqCtx, cancel := s.withRequestTimeout(ctx)
		defer cancel()
		return handler(reqCtx, req)
	}
}

func (s *server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic in HTTP %s %s: %v", r.Method, r.URL.Path, rec)
				writeErrorJSON(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func parseIP(raw string) net.IP {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(raw, "[") && strings.Contains(raw, "]") {
		host, _, err := net.SplitHostPort(raw)
		if err == nil {
			raw = strings.Trim(host, "[]")
		}
	}
	if h, _, err := net.SplitHostPort(raw); err == nil {
		raw = h
	}
	return net.ParseIP(strings.Trim(raw, "[]"))
}

func (s *server) isTrustedProxy(ip net.IP) bool {
	if ip == nil || len(s.trustedProxies) == 0 {
		return false
	}
	for _, n := range s.trustedProxies {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *server) clientIPFromRequest(r *http.Request) string {
	remote := parseIP(r.RemoteAddr)
	if remote == nil {
		return ""
	}
	// Only trust forwarded headers from explicitly trusted proxies.
	if !s.isTrustedProxy(remote) {
		return remote.String()
	}

	var chain []net.IP
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, p := range strings.Split(xff, ",") {
			if ip := parseIP(p); ip != nil {
				chain = append(chain, ip)
			}
		}
	}
	chain = append(chain, remote)

	// Walk right-to-left and return the first untrusted address.
	for i := len(chain) - 1; i >= 0; i-- {
		if !s.isTrustedProxy(chain[i]) {
			return chain[i].String()
		}
	}

	if xr := parseIP(r.Header.Get("X-Real-IP")); xr != nil {
		return xr.String()
	}
	return remote.String()
}

func (s *server) instrumentHTTP(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		dur := time.Since(start)

		s.metrics.Observe("http", route, r.Method, rec.status, dur)
		if s.verbose {
			log.Printf("http route=%s method=%s status=%d duration=%s client_ip=%s", route, r.Method, rec.status, dur, s.clientIPFromRequest(r))
		}
	}
}

// TinySQLServer implementation
func (s *server) Exec(ctx context.Context, req *execRequest) (*execResponse, error) {
	start := time.Now()
	tenant := s.tenantOrDefault(req.Tenant)
	sqlText, err := s.normalizeSQL(req.SQL)
	if err != nil {
		return &execResponse{Success: false, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}

	ctx, cancel, err := s.withRequestTimeoutOverride(ctx, req.TimeoutMS)
	if err != nil {
		return &execResponse{Success: false, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}
	defer cancel()

	parser := engine.NewParser(sqlText)
	stmt, err := parser.ParseStatement()
	if err != nil {
		return &execResponse{Success: false, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}
	_, err = engine.Execute(ctx, s.db, tenant, stmt)
	if err != nil {
		return &execResponse{Success: false, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}
	return &execResponse{Success: true, Duration: time.Since(start).String()}, nil
}

func (s *server) Query(ctx context.Context, req *queryRequest) (*queryResponse, error) {
	start := time.Now()
	tenant := s.tenantOrDefault(req.Tenant)
	sqlText, err := s.normalizeSQL(req.SQL)
	if err != nil {
		return &queryResponse{SQL: req.SQL, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}

	ctx, cancel, err := s.withRequestTimeoutOverride(ctx, req.TimeoutMS)
	if err != nil {
		return &queryResponse{SQL: req.SQL, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}
	defer cancel()

	compiled, err := s.cache.Compile(sqlText)
	if err != nil {
		return &queryResponse{SQL: sqlText, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}
	rs, err := compiled.Execute(ctx, s.db, tenant)
	if err != nil {
		return &queryResponse{SQL: sqlText, Error: err.Error(), Duration: time.Since(start).String()}, nil
	}

	var cols []string
	var rows []map[string]any
	if rs != nil && len(rs.Rows) > 0 {
		for c := range rs.Rows[0] {
			cols = append(cols, c)
		}
		sort.Strings(cols)

		rows = make([]map[string]any, 0, len(rs.Rows))
		for _, r := range rs.Rows {
			m := make(map[string]any, len(r))
			for k, v := range r {
				m[k] = v
			}
			rows = append(rows, m)
		}
	}
	return &queryResponse{
		SQL:      sqlText,
		Columns:  cols,
		Rows:     rows,
		Duration: time.Since(start).String(),
		Count:    len(rows),
	}, nil
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, limit int64, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain exactly one JSON object")
	}
	return nil
}

// HTTP handlers
func (s *server) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req execRequest
	if err := decodeJSONBody(w, r, s.maxBodyBytes, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	resp, _ := s.Exec(r.Context(), &req)
	if !resp.Success {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req queryRequest
	if err := decodeJSONBody(w, r, s.maxBodyBytes, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	resp, _ := s.Query(r.Context(), &req)
	if resp.Error != "" {
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeErrorJSON(w, http.StatusServiceUnavailable, "server not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true})
}

func (s *server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, s.metrics.PrometheusText())
}

func (s *server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	stats := s.db.BackendStats()
	reqTotals := s.metrics.TotalRequestsByProtocol()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"ready":           s.ready.Load(),
		"time":            time.Now().Format(time.RFC3339),
		"uptime":          time.Since(s.startedAt).String(),
		"tenant":          s.defaultT,
		"tables":          len(s.db.ListTables(s.defaultT)),
		"peers":           s.peers,
		"trusted_proxies": len(s.trustedProxies),
		"storage_mode":    s.db.StorageMode().String(),
		"request_totals": map[string]any{
			"http": reqTotals["http"],
			"grpc": reqTotals["grpc"],
		},
		"backend_stats": map[string]any{
			"tables_in_memory":   stats.TablesInMemory,
			"tables_on_disk":     stats.TablesOnDisk,
			"memory_used_bytes":  stats.MemoryUsedBytes,
			"memory_limit_bytes": stats.MemoryLimitBytes,
			"disk_used_bytes":    stats.DiskUsedBytes,
			"cache_hit_rate":     stats.CacheHitRate,
			"sync_count":         stats.SyncCount,
			"load_count":         stats.LoadCount,
			"eviction_count":     stats.EvictionCount,
		},
	})
}

func (s *server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	type peerStatus struct {
		Address  string `json:"address"`
		Healthy  bool   `json:"healthy"`
		Duration string `json:"duration"`
		Error    string `json:"error,omitempty"`
	}

	type peerRes struct {
		status peerStatus
	}

	ctx, cancel := s.withRequestTimeout(r.Context())
	defer cancel()

	ch := make(chan peerRes, len(s.peers))
	var wg sync.WaitGroup
	for _, raw := range s.peers {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			start := time.Now()
			_, err := grpcQuery(ctx, addr, &queryRequest{Tenant: s.defaultT, SQL: "SELECT 1"}, s.authToken, s.peerTimeout, *flagGRPCMaxRecv, s.peerDialCreds)
			status := peerStatus{
				Address:  addr,
				Healthy:  err == nil,
				Duration: time.Since(start).String(),
			}
			if err != nil {
				status.Error = err.Error()
			}
			ch <- peerRes{status: status}
		}(addr)
	}

	wg.Wait()
	close(ch)

	peers := make([]peerStatus, 0, len(s.peers))
	healthy := 0
	for res := range ch {
		peers = append(peers, res.status)
		if res.status.Healthy {
			healthy++
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Address < peers[j].Address })

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"cluster":       len(peers) > 0,
		"peer_count":    len(peers),
		"healthy_peers": healthy,
		"peers":         peers,
	})
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	return equalStringSlices(aa, bb)
}

// Federated query: query all peers via gRPC JSON codec and merge rows (concat)
func (s *server) handleFederatedQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if len(s.peers) == 0 {
		writeErrorJSON(w, http.StatusBadRequest, "no peers configured")
		return
	}

	var req queryRequest
	if err := decodeJSONBody(w, r, s.maxBodyBytes, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	peerTimeout, err := s.peerTimeoutOverride(req.PeerTimeoutMS)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	type peerRes struct {
		rows []map[string]any
		cols []string
		err  error
	}

	localCh := make(chan *queryResponse, 1)
	go func() {
		local, _ := s.Query(r.Context(), &req)
		localCh <- local
	}()

	ch := make(chan peerRes, len(s.peers))
	var wg sync.WaitGroup
	for _, raw := range s.peers {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			out, err := grpcQuery(r.Context(), addr, &queryRequest{Tenant: req.Tenant, SQL: req.SQL, TimeoutMS: req.TimeoutMS}, s.authToken, peerTimeout, *flagGRPCMaxRecv, s.peerDialCreds)
			if err != nil {
				ch <- peerRes{err: err}
				return
			}
			ch <- peerRes{rows: out.Rows, cols: out.Columns}
		}(addr)
	}

	local := <-localCh
	if local.Error != "" {
		writeJSON(w, http.StatusBadRequest, local)
		return
	}

	cols := append([]string{}, local.Columns...)
	rows := append([]map[string]any{}, local.Rows...)

	wg.Wait()
	close(ch)

	for res := range ch {
		if res.err != nil {
			if s.verbose {
				log.Printf("federation peer error: %v", res.err)
			}
			continue
		}
		if len(cols) == 0 && len(res.cols) > 0 {
			cols = append([]string(nil), res.cols...)
		}
		if len(cols) > 0 && !sameColumns(cols, res.cols) {
			if s.verbose {
				log.Printf("federation peer columns mismatch: local=%v peer=%v", cols, res.cols)
			}
			continue
		}
		rows = append(rows, res.rows...)
	}

	writeJSON(w, http.StatusOK, &queryResponse{
		SQL:      req.SQL,
		Columns:  cols,
		Rows:     rows,
		Duration: "n/a",
		Count:    len(rows),
	})
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode response failed: %v", err)
	}
}

func writeErrorJSON(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]any{"error": message})
}

// gRPC JSON client helper
func grpcQuery(ctx context.Context, addr string, req *queryRequest, authToken string, timeout time.Duration, maxRecvMsg int, transportCreds credentials.TransportCredentials) (*queryResponse, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("empty peer address")
	}

	if transportCreds == nil {
		transportCreds = insecure.NewCredentials()
	}

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(
			grpc.ForceCodec(jsonCodec{}),
			grpc.MaxCallRecvMsgSize(maxRecvMsg),
		),
	}
	conn, err := grpc.NewClient(addr, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("connect to peer %s: %w", addr, err)
	}
	defer conn.Close()

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if authToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+authToken)
	}

	var resp queryResponse
	if err := conn.Invoke(ctx, "/tinysql.TinySQL/Query", req, &resp); err != nil {
		return nil, fmt.Errorf("invoke peer %s: %w", addr, err)
	}
	if resp.Error != "" {
		return &resp, fmt.Errorf("peer %s: %s", addr, resp.Error)
	}
	return &resp, nil
}

func parsePeerList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	peers := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			peers = append(peers, p)
		}
	}
	return peers
}

func parseTLSMinVersion(raw string) (uint16, error) {
	switch strings.TrimSpace(raw) {
	case "", "1.2", "tls1.2", "TLS1.2":
		return tls.VersionTLS12, nil
	case "1.3", "tls1.3", "TLS1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unsupported TLS minimum version %q (valid: 1.2, 1.3)", raw)
	}
}

func parseTrustedProxyCIDRs(raw string) ([]*net.IPNet, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]*net.IPNet, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.Contains(p, "/") {
			_, n, err := net.ParseCIDR(p)
			if err != nil {
				return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", p, err)
			}
			out = append(out, n)
			continue
		}
		ip := net.ParseIP(p)
		if ip == nil {
			return nil, fmt.Errorf("invalid trusted proxy IP %q", p)
		}
		bits := 128
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
			bits = 32
		}
		out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return out, nil
}

func validateTLSPair(certPath, keyPath, name string) error {
	hasCert := strings.TrimSpace(certPath) != ""
	hasKey := strings.TrimSpace(keyPath) != ""
	if hasCert != hasKey {
		return fmt.Errorf("%s TLS requires both cert and key", name)
	}
	return nil
}

func loadServerTLSConfig(certPath, keyPath string, minVersion uint16) (*tls.Config, error) {
	certPath = strings.TrimSpace(certPath)
	keyPath = strings.TrimSpace(keyPath)
	if certPath == "" || keyPath == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert/key: %w", err)
	}
	return &tls.Config{
		MinVersion:   minVersion,
		Certificates: []tls.Certificate{cert},
	}, nil
}

func loadPeerTLSCredentials(caPath, serverName string, minVersion uint16, skipVerify bool) (credentials.TransportCredentials, error) {
	tlsCfg := &tls.Config{
		MinVersion:         minVersion,
		InsecureSkipVerify: skipVerify, //nolint:gosec // Explicit flag for controlled environments.
	}
	if strings.TrimSpace(serverName) != "" {
		tlsCfg.ServerName = strings.TrimSpace(serverName)
	}
	if caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read peer TLS CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("failed to parse peer TLS CA bundle")
		}
		tlsCfg.RootCAs = pool
	}
	return credentials.NewTLS(tlsCfg), nil
}

func parseBoolValue(raw string) (bool, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool value %q", raw)
	}
}

func parseOptionalBool(values url.Values, key string, dst *bool) error {
	raw, ok := values[key]
	if !ok || len(raw) == 0 {
		return nil
	}
	v, err := parseBoolValue(raw[0])
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = v
	return nil
}

func parseOptionalInt64(values url.Values, key string, dst *int64) error {
	raw := strings.TrimSpace(values.Get(key))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if v < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	*dst = v
	return nil
}

func parseOptionalUint64(values url.Values, key string, dst *uint64) error {
	raw := strings.TrimSpace(values.Get(key))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = v
	return nil
}

func parseOptionalDuration(values url.Values, key string, dst *time.Duration) error {
	raw := strings.TrimSpace(values.Get(key))
	if raw == "" {
		return nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if v < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	*dst = v
	return nil
}

func applyStorageOptions(values url.Values, cfg *storage.StorageConfig) error {
	if err := parseOptionalBool(values, "compress", &cfg.CompressFiles); err != nil {
		return err
	}
	if err := parseOptionalBool(values, "compress_files", &cfg.CompressFiles); err != nil {
		return err
	}
	if err := parseOptionalBool(values, "sync_on_mutate", &cfg.SyncOnMutate); err != nil {
		return err
	}
	if err := parseOptionalInt64(values, "max_memory_bytes", &cfg.MaxMemoryBytes); err != nil {
		return err
	}
	if strings.TrimSpace(values.Get("max_memory_mb")) != "" {
		mb, err := strconv.ParseInt(strings.TrimSpace(values.Get("max_memory_mb")), 10, 64)
		if err != nil {
			return fmt.Errorf("max_memory_mb: %w", err)
		}
		if mb < 0 {
			return fmt.Errorf("max_memory_mb must be >= 0")
		}
		cfg.MaxMemoryBytes = mb * 1024 * 1024
	}
	if err := parseOptionalUint64(values, "checkpoint_every", &cfg.CheckpointEvery); err != nil {
		return err
	}
	if err := parseOptionalDuration(values, "checkpoint_interval", &cfg.CheckpointInterval); err != nil {
		return err
	}
	return nil
}

func openDBFromDSN(dsn string) (*storage.DB, string, error) {
	dsn = normalizeDSN(dsn)
	lower := strings.ToLower(dsn)
	switch {
	case strings.HasPrefix(lower, "mem://"):
		return openMemDBFromDSN(dsn)
	case strings.HasPrefix(lower, "file:"):
		return openFileDBFromDSN(dsn)
	default:
		return nil, "", fmt.Errorf("unsupported DSN %q", dsn)
	}
}

func normalizeDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "mem://?tenant=default"
	}
	return dsn
}

func dsnTenant(values url.Values) string {
	tenant := strings.TrimSpace(values.Get("tenant"))
	if tenant == "" {
		return "default"
	}
	return tenant
}

func parseDSNQuery(rawQuery, prefix string) (url.Values, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, fmt.Errorf("parse %s DSN query: %w", prefix, err)
	}
	return values, nil
}

func parseStorageModeOrDefault(values url.Values) (storage.StorageMode, error) {
	mode := storage.ModeMemory
	if ms := strings.TrimSpace(values.Get("mode")); ms != "" {
		parsed, err := storage.ParseStorageMode(ms)
		if err != nil {
			return 0, fmt.Errorf("parse mode: %w", err)
		}
		mode = parsed
	}
	return mode, nil
}

func openMemDBFromDSN(dsn string) (*storage.DB, string, error) {
	rawQuery := ""
	if i := strings.Index(dsn, "?"); i >= 0 {
		rawQuery = dsn[i+1:]
	}
	values, err := parseDSNQuery(rawQuery, "mem")
	if err != nil {
		return nil, "", err
	}

	mode, err := parseStorageModeOrDefault(values)
	if err != nil {
		return nil, "", err
	}

	cfg := storage.DefaultStorageConfig(mode)
	cfg.Mode = mode
	if mode != storage.ModeMemory {
		path := strings.TrimSpace(values.Get("path"))
		if path == "" {
			return nil, "", fmt.Errorf("mem DSN with mode=%s requires path query parameter", mode)
		}
		cfg.Path = filepath.Clean(path)
	}
	if err := applyStorageOptions(values, &cfg); err != nil {
		return nil, "", err
	}

	db, err := storage.OpenDB(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("open database: %w", err)
	}
	return db, dsnTenant(values), nil
}

func parseFileDSNParts(dsn string) (string, string, error) {
	rest := dsn[len("file:"):]
	if rest == "" {
		return "", "", fmt.Errorf("file DSN path required")
	}

	path := rest
	rawQuery := ""
	if i := strings.Index(rest, "?"); i >= 0 {
		path = rest[:i]
		rawQuery = rest[i+1:]
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", fmt.Errorf("file DSN path required")
	}
	return path, rawQuery, nil
}

func parseFileDSNMode(values url.Values) (storage.StorageMode, error) {
	mode, err := parseStorageModeOrDefault(values)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(values.Get("mode")) != "" {
		return mode, nil
	}
	if raw := strings.TrimSpace(values.Get("autosave")); raw != "" {
		autosave, err := parseBoolValue(raw)
		if err != nil {
			return 0, fmt.Errorf("autosave: %w", err)
		}
		if autosave {
			return storage.ModeWAL, nil
		}
	}
	return mode, nil
}

func openFileDBFromDSN(dsn string) (*storage.DB, string, error) {
	path, rawQuery, err := parseFileDSNParts(dsn)
	if err != nil {
		return nil, "", err
	}
	values, err := parseDSNQuery(rawQuery, "file")
	if err != nil {
		return nil, "", err
	}

	mode, err := parseFileDSNMode(values)
	if err != nil {
		return nil, "", err
	}

	cfg := storage.DefaultStorageConfig(mode)
	cfg.Mode = mode
	cfg.Path = filepath.Clean(path)
	if err := applyStorageOptions(values, &cfg); err != nil {
		return nil, "", err
	}

	db, err := storage.OpenDB(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("open database: %w", err)
	}
	return db, dsnTenant(values), nil
}

func run() error {
	flag.Parse()

	httpAddr, grpcAddr, minTLSVersion, trustedProxies, err := parseRunConfig()
	if err != nil {
		return err
	}

	db, dsnTenant, err := openDBFromDSN(*flagDSN)
	if err != nil {
		return err
	}

	tenant := resolveRunTenant(dsnTenant)

	peerDialCreds, err := buildRunPeerDialCreds(minTLSVersion)
	if err != nil {
		_ = db.Close()
		return err
	}

	srv := newServer(db, tenant, *flagAuth, parsePeerList(*flagPeers), trustedProxies, peerDialCreds)
	encoding.RegisterCodec(jsonCodec{})

	errChan := make(chan error, 2)

	httpSrv, err := startHTTPServer(srv, db, httpAddr, minTLSVersion, errChan)
	if err != nil {
		return err
	}

	grpcSrv, err := startGRPCServer(srv, db, grpcAddr, minTLSVersion, errChan)
	if err != nil {
		return err
	}

	runErr := waitForServerStop(errChan)

	srv.ready.Store(false)

	shutdownErr := shutdownRunServers(httpSrv, grpcSrv, db)

	if runErr != nil {
		return runErr
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	return nil
}

func parseRunConfig() (string, string, uint16, []*net.IPNet, error) {
	httpAddr := strings.TrimSpace(*flagHTTP)
	grpcAddr := strings.TrimSpace(*flagGRPC)
	if httpAddr == "" && grpcAddr == "" {
		return "", "", 0, nil, fmt.Errorf("no server (HTTP or gRPC) enabled")
	}
	if err := validateTLSPair(*flagHTTPTLSCert, *flagHTTPTLSKey, "http"); err != nil {
		return "", "", 0, nil, err
	}
	if err := validateTLSPair(*flagGRPCTLSCert, *flagGRPCTLSKey, "grpc"); err != nil {
		return "", "", 0, nil, err
	}
	if err := validateRunPeerTLSFlags(); err != nil {
		return "", "", 0, nil, err
	}
	minTLSVersion, err := parseTLSMinVersion(*flagTLSMinVersion)
	if err != nil {
		return "", "", 0, nil, err
	}
	trustedProxies, err := parseTrustedProxyCIDRs(*flagTrustedProxies)
	if err != nil {
		return "", "", 0, nil, err
	}
	return httpAddr, grpcAddr, minTLSVersion, trustedProxies, nil
}

func validateRunPeerTLSFlags() error {
	if *flagPeerTLS {
		return nil
	}
	if strings.TrimSpace(*flagPeerTLSCA) != "" || strings.TrimSpace(*flagPeerTLSServerName) != "" || *flagPeerTLSSkipVerify {
		return fmt.Errorf("peer TLS options require -peer-tls=true")
	}
	return nil
}

func resolveRunTenant(dsnTenant string) string {
	tenant := strings.TrimSpace(*flagTenant)
	tenantFlagSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "tenant" {
			tenantFlagSet = true
		}
	})
	if !tenantFlagSet && dsnTenant != "" {
		tenant = dsnTenant
	}
	if tenant == "" {
		return "default"
	}
	return tenant
}

func buildRunPeerDialCreds(minTLSVersion uint16) (credentials.TransportCredentials, error) {
	if !*flagPeerTLS {
		return nil, nil
	}
	return loadPeerTLSCredentials(
		strings.TrimSpace(*flagPeerTLSCA),
		strings.TrimSpace(*flagPeerTLSServerName),
		minTLSVersion,
		*flagPeerTLSSkipVerify,
	)
}

func startHTTPServer(srv *server, db *storage.DB, httpAddr string, minTLSVersion uint16, errChan chan<- error) (*http.Server, error) {
	if httpAddr == "" {
		return nil, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/exec", srv.instrumentHTTP("/api/exec", srv.withAuth(srv.handleExec)))
	mux.HandleFunc("/api/query", srv.instrumentHTTP("/api/query", srv.withAuth(srv.handleQuery)))
	mux.HandleFunc("/api/status", srv.instrumentHTTP("/api/status", srv.withAuth(srv.handleStatus)))
	mux.HandleFunc("/api/cluster/status", srv.instrumentHTTP("/api/cluster/status", srv.withAuth(srv.handleClusterStatus)))
	mux.HandleFunc("/api/federated/query", srv.instrumentHTTP("/api/federated/query", srv.withAuth(srv.handleFederatedQuery)))
	mux.HandleFunc("/metrics", srv.instrumentHTTP("/metrics", srv.withAuth(srv.handleMetrics)))
	mux.HandleFunc("/healthz", srv.instrumentHTTP("/healthz", srv.handleHealth))
	mux.HandleFunc("/readyz", srv.instrumentHTTP("/readyz", srv.handleReady))

	httpTLSCfg, err := loadServerTLSConfig(*flagHTTPTLSCert, *flagHTTPTLSKey, minTLSVersion)
	if err != nil {
		srv.ready.Store(false)
		_ = db.Close()
		return nil, err
	}

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           srv.recoverMiddleware(mux),
		ReadTimeout:       *flagReadTimeout,
		ReadHeaderTimeout: *flagReadHeaderTimeout,
		WriteTimeout:      *flagWriteTimeout,
		IdleTimeout:       *flagIdleTimeout,
		MaxHeaderBytes:    *flagMaxHeaderBytes,
		TLSConfig:         httpTLSCfg,
	}

	go func() {
		proto := "http"
		var serveErr error
		if httpTLSCfg != nil {
			proto = "https"
		}
		log.Printf("HTTP listening on %s (%s)", httpAddr, proto)
		if httpTLSCfg != nil {
			serveErr = httpSrv.ListenAndServeTLS("", "")
		} else {
			serveErr = httpSrv.ListenAndServe()
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errChan <- fmt.Errorf("http serve: %w", serveErr)
		}
	}()
	return httpSrv, nil
}

func startGRPCServer(srv *server, db *storage.DB, grpcAddr string, minTLSVersion uint16, errChan chan<- error) (*grpc.Server, error) {
	if grpcAddr == "" {
		return nil, nil
	}
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		srv.ready.Store(false)
		_ = db.Close()
		return nil, fmt.Errorf("grpc listen: %w", err)
	}

	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(*flagGRPCMaxRecv),
		grpc.MaxSendMsgSize(*flagGRPCMaxSend),
		grpc.UnaryInterceptor(srv.grpcUnaryInterceptor()),
	}
	grpcTLSCfg, err := loadServerTLSConfig(*flagGRPCTLSCert, *flagGRPCTLSKey, minTLSVersion)
	if err != nil {
		srv.ready.Store(false)
		_ = lis.Close()
		_ = db.Close()
		return nil, err
	}
	if grpcTLSCfg != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(grpcTLSCfg)))
	}

	grpcSrv := grpc.NewServer(grpcOpts...)
	registerTinySQLServer(grpcSrv, srv)

	go func() {
		proto := "plaintext"
		if *flagGRPCTLSCert != "" && *flagGRPCTLSKey != "" {
			proto = "tls"
		}
		log.Printf("gRPC listening on %s (%s)", grpcAddr, proto)
		if serveErr := grpcSrv.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			errChan <- fmt.Errorf("grpc serve: %w", serveErr)
		}
	}()
	return grpcSrv, nil
}

func waitForServerStop(errChan <-chan error) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-errChan:
		if err != nil {
			log.Printf("server error: %v", err)
		}
		return err
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
		return nil
	}
}

func shutdownRunServers(httpSrv *http.Server, grpcSrv *grpc.Server, db *storage.DB) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *flagShutdownTimeout)
	defer cancel()

	var shutdownErr error
	if httpSrv != nil {
		if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("http shutdown: %w", err))
		}
	}
	if grpcSrv != nil {
		done := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			grpcSrv.Stop()
		}
	}
	if err := db.Close(); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close db: %w", err))
	}
	return shutdownErr
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
