package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "strings"
    "sync"
    "time"

    _ "github.com/SimonWaldherr/tinySQL/internal/driver"
    "github.com/SimonWaldherr/tinySQL/internal/engine"
    "github.com/SimonWaldherr/tinySQL/internal/storage"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/encoding"
)

// Flags
var (
    flagDSN     = flag.String("dsn", "mem://?tenant=default", "DSN for database/sql (mem:// or file:/path.db?tenant=...&autosave=1)")
    flagHTTP    = flag.String("http", ":8080", "HTTP listen address (empty to disable)")
    flagGRPC    = flag.String("grpc", ":9090", "gRPC listen address (empty to disable)")
    flagPeers   = flag.String("peers", "", "Comma-separated list of gRPC peer addresses for federation (optional)")
    flagTenant  = flag.String("tenant", "default", "Default tenant if none provided in request")
    flagVerbose = flag.Bool("v", false, "Verbose logging")
)

// HTTP types
type execRequest struct {
    Tenant string `json:"tenant"`
    SQL    string `json:"sql"`
}
type execResponse struct {
    Success      bool   `json:"success"`
    Error        string `json:"error,omitempty"`
    RowsAffected int64  `json:"rows_affected,omitempty"`
    LastInsertID int64  `json:"last_insert_id,omitempty"`
    Duration     string `json:"duration"`
}

type queryRequest struct {
    Tenant string `json:"tenant"`
    SQL    string `json:"sql"`
}
type queryResponse struct {
    SQL      string                   `json:"sql"`
    Columns  []string                 `json:"columns"`
    Rows     []map[string]any         `json:"rows"`
    Error    string                   `json:"error,omitempty"`
    Duration string                   `json:"duration"`
    Count    int                      `json:"count"`
}

// gRPC JSON codec
type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }
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
    handler := func(ctx context.Context, req any) (any, error) { return srv.(TinySQLServer).Exec(ctx, req.(*execRequest)) }
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
    handler := func(ctx context.Context, req any) (any, error) { return srv.(TinySQLServer).Query(ctx, req.(*queryRequest)) }
    return interceptor(ctx, in, info, handler)
}

// server state
type server struct {
    db       *storage.DB
    cache    *engine.QueryCache
    peers    []string
    defaultT string
}

func newServer() *server {
    return &server{
        db:       storage.NewDB(),
        cache:    engine.NewQueryCache(200),
        defaultT: *flagTenant,
    }
}

func (s *server) tenantOrDefault(t string) string {
    if strings.TrimSpace(t) == "" {
        return s.defaultT
    }
    return t
}

// TinySQLServer implementation
func (s *server) Exec(ctx context.Context, req *execRequest) (*execResponse, error) {
    start := time.Now()
    tenant := s.tenantOrDefault(req.Tenant)
    parser := engine.NewParser(req.SQL)
    stmt, err := parser.ParseStatement()
    if err != nil {
        return &execResponse{Success: false, Error: err.Error(), Duration: time.Since(start).String()}, nil
    }
    _, err = engine.Execute(ctx, s.db, tenant, stmt)
    if err != nil {
        return &execResponse{Success: false, Error: err.Error(), Duration: time.Since(start).String()}, nil
    }
    // We don't track rowsAffected/lastInsertID in engine; return zeroes.
    return &execResponse{Success: true, Duration: time.Since(start).String()}, nil
}

func (s *server) Query(ctx context.Context, req *queryRequest) (*queryResponse, error) {
    start := time.Now()
    tenant := s.tenantOrDefault(req.Tenant)
    compiled, err := s.cache.Compile(req.SQL)
    if err != nil {
        return &queryResponse{SQL: req.SQL, Error: err.Error(), Duration: time.Since(start).String()}, nil
    }
    rs, err := compiled.Execute(ctx, s.db, tenant)
    if err != nil {
        return &queryResponse{SQL: req.SQL, Error: err.Error(), Duration: time.Since(start).String()}, nil
    }
    var cols []string
    var rows []map[string]any
    if rs != nil && len(rs.Rows) > 0 {
        for c := range rs.Rows[0] {
            cols = append(cols, c)
        }
        for _, r := range rs.Rows {
            m := make(map[string]any, len(r))
            for k, v := range r {
                m[k] = v
            }
            rows = append(rows, m)
        }
    }
    return &queryResponse{
        SQL:      req.SQL,
        Columns:  cols,
        Rows:     rows,
        Duration: time.Since(start).String(),
        Count:    len(rows),
    }, nil
}

// HTTP handlers
func (s *server) handleExec(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    var req execRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
        return
    }
    resp, _ := s.Exec(r.Context(), &req)
    writeJSON(w, resp)
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    var req queryRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
        return
    }
    resp, _ := s.Query(r.Context(), &req)
    writeJSON(w, resp)
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, map[string]any{
        "ok":        true,
        "time":      time.Now().Format(time.RFC3339),
        "tenant":    s.defaultT,
        "tables":    len(s.db.ListTables(s.defaultT)),
        "peers":     s.peers,
        "build":     "dev",
    })
}

// Federated query: query all peers via gRPC JSON codec and merge rows (concat)
func (s *server) handleFederatedQuery(w http.ResponseWriter, r *http.Request) {
    if len(s.peers) == 0 {
        http.Error(w, "No peers configured", http.StatusBadRequest)
        return
    }
    var req queryRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
        return
    }
    // Local first
    local, _ := s.Query(r.Context(), &req)
    cols := append([]string{}, local.Columns...)
    rows := append([]map[string]any{}, local.Rows...)

    // Query peers concurrently
    type peerRes struct{ rows []map[string]any; err error }
    ch := make(chan peerRes, len(s.peers))
    var wg sync.WaitGroup
    for _, addr := range s.peers {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()
            out, err := grpcQuery(addr, &queryRequest{Tenant: req.Tenant, SQL: req.SQL})
            if err != nil {
                ch <- peerRes{nil, err}
                return
            }
            // Simple column check: ensure columns match; else skip
            if !equalStringSlices(cols, out.Columns) {
                ch <- peerRes{nil, fmt.Errorf("peer %s columns mismatch", addr)}
                return
            }
            ch <- peerRes{out.Rows, nil}
        }(strings.TrimSpace(addr))
    }
    wg.Wait()
    close(ch)
    for res := range ch {
        if res.err != nil {
            if *flagVerbose {
                log.Printf("federation peer error: %v", res.err)
            }
            continue
        }
        rows = append(rows, res.rows...)
    }
    writeJSON(w, &queryResponse{
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
    mm := make(map[string]struct{}, len(a))
    for _, s := range a { mm[s] = struct{}{} }
    for _, s := range b { if _, ok := mm[s]; !ok { return false } }
    return true
}

func writeJSON(w http.ResponseWriter, v any) {
    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(v)
}

// gRPC JSON client helper
func grpcQuery(addr string, req *queryRequest) (*queryResponse, error) {
    conn, err := grpc.Dial(addr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
    )
    if err != nil {
        return nil, err
    }
    defer conn.Close()
    var resp queryResponse
    if err := conn.Invoke(context.Background(), "/tinysql.TinySQL/Query", req, &resp); err != nil {
        return nil, err
    }
    if resp.Error != "" {
        return &resp, fmt.Errorf(resp.Error)
    }
    return &resp, nil
}

func main() {
    flag.Parse()

    // Open a database/sql handle mainly to ensure driver is registered; we operate on internal DB
    dbh, err := sql.Open("tinysql", *flagDSN)
    if err != nil {
        log.Fatalf("open error: %v", err)
    }
    defer dbh.Close()

    srv := newServer()
    if p := strings.TrimSpace(*flagPeers); p != "" {
        srv.peers = strings.Split(p, ",")
    }

    // Register JSON codec for gRPC
    encoding.RegisterCodec(jsonCodec{})

    // Start gRPC server
    var grpcErr error
    if *flagGRPC != "" {
        go func() {
            lis, err := net.Listen("tcp", *flagGRPC)
            if err != nil {
                log.Printf("gRPC listen error: %v", err)
                grpcErr = err
                return
            }
            gs := grpc.NewServer()
            registerTinySQLServer(gs, srv)
            log.Printf("gRPC listening on %s", *flagGRPC)
            if err := gs.Serve(lis); err != nil {
                log.Printf("gRPC serve error: %v", err)
                grpcErr = err
            }
        }()
    }

    // Start HTTP server
    if *flagHTTP != "" {
        mux := http.NewServeMux()
        mux.HandleFunc("/api/exec", srv.handleExec)
        mux.HandleFunc("/api/query", srv.handleQuery)
        mux.HandleFunc("/api/status", srv.handleStatus)
        mux.HandleFunc("/api/federated/query", srv.handleFederatedQuery)
        log.Printf("HTTP listening on %s", *flagHTTP)
        if err := http.ListenAndServe(*flagHTTP, mux); err != nil {
            log.Printf("HTTP serve error: %v", err)
            if grpcErr != nil {
                os.Exit(1)
            }
        }
    } else {
        // If HTTP disabled, block on gRPC only
        select {}
    }
}
