package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

func awaitCluster(t *testing.T, description string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out: " + description)
}

func newServingReplica(t *testing.T) *server {
	t.Helper()
	db := storage.NewDB()
	db.SetReadOnly(true)
	s := newServer(db, "default", "cluster-secret", nil, nil, nil)
	s.replica = &replicaHealth{timeout: 1500 * time.Millisecond}
	t.Cleanup(func() { _ = s.db.Close() })
	return s
}

func clusterRequest(t *testing.T, handler http.Handler, path, sql string) *httptest.ResponseRecorder {
	t.Helper()
	method := http.MethodGet
	var body io.Reader
	if sql != "" {
		method = http.MethodPost
		data, err := json.Marshal(map[string]string{"sql": sql})
		if err != nil {
			t.Fatal(err)
		}
		body = strings.NewReader(string(data))
	}
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Authorization", "Bearer cluster-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

// Exercise both transports through the same serving lifecycle: cold start,
// authenticated reads, read-only enforcement, DDL re-bootstrap, and reconnect.
func TestServingReplicaCluster(t *testing.T) {
	for _, transport := range replicaTransports {
		t.Run(transport.name, func(t *testing.T) {
			primary, primaryDB := newAdvancedWALTestServer(t)
			primary.authToken = "cluster-secret"
			encoding.RegisterCodec(jsonCodec{})
			encoding.RegisterCodec(protobufCodec{})
			grpcSrv, addr, err := startGRPCServer(primary, primaryDB, "127.0.0.1:0", tls.VersionTLS12, make(chan error, 2))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { grpcSrv.Stop() })
			s := newServingReplica(t)
			handler := s.httpHandler()
			if got := clusterRequest(t, handler, "/readyz", "").Code; got != 503 {
				t.Fatalf("cold replica ready = %d", got)
			}
			if got := clusterRequest(t, handler, "/api/query", "SELECT 1").Code; got != 503 {
				t.Fatalf("cold replica query = %d", got)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			opts := replicaOptions{AuthToken: s.authToken, CallTimeout: 300 * time.Millisecond, MaxRecvMsgSize: defaultMaxGRPCMsgBytes, Observe: s.replica.observe}
			go func() {
				defer close(done)
				s.serveReplicaLoop(ctx, addr, opts, transport.changesLoop)
			}()
			t.Cleanup(func() { cancel(); <-done })
			awaitCluster(t, "initial bootstrap", s.isReady)
			for _, sql := range []string{
				"CREATE TABLE cluster_items (id INT PRIMARY KEY, name TEXT)",
				"INSERT INTO cluster_items VALUES (1, 'before')",
				"UPDATE cluster_items SET name = 'after' WHERE id = 1",
			} {
				resp, err := primary.Exec(ctx, &execRequest{SQL: sql})
				if err != nil || !resp.Success {
					t.Fatalf("primary %s: %+v %v", sql, resp, err)
				}
			}
			awaitCluster(t, "DDL and row convergence", func() bool {
				resp, err := s.Query(ctx, &queryRequest{SQL: "SELECT name FROM cluster_items"})
				return err == nil && resp.Error == "" && len(resp.Rows) == 1 && resp.Rows[0]["name"] == "after"
			})
			for _, path := range []string{"/readyz", "/readyz/read", "/healthz", "/api/status", "/metrics"} {
				if w := clusterRequest(t, handler, path, ""); w.Code != 200 {
					t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
				}
			}
			if got := clusterRequest(t, handler, "/readyz/write", "").Code; got != 503 {
				t.Fatalf("replica accepted write readiness: %d", got)
			}
			for _, path := range []string{"/api/exec", "/api/query", "/api/query/stream"} {
				for _, sql := range []string{"DELETE FROM cluster_items", "DROP TABLE cluster_items", "EXPLAIN ANALYZE DELETE FROM cluster_items"} {
					w := clusterRequest(t, handler, path, sql)
					if w.Code != 400 || !strings.Contains(w.Body.String(), "read-only") {
						t.Fatalf("mutation via %s (%s): %d %s", path, sql, w.Code, w.Body.String())
					}
				}
			}
			for _, path := range []string{"/api/query", "/api/query/stream"} {
				w := clusterRequest(t, handler, path, "SELECT name FROM cluster_items")
				if w.Code != 200 || !strings.Contains(w.Body.String(), "after") {
					t.Fatalf("read via %s: %d %s", path, w.Code, w.Body.String())
				}
			}
			unauthorized := httptest.NewRecorder()
			handler.ServeHTTP(unauthorized, httptest.NewRequest("GET", "/api/status", nil))
			if unauthorized.Code != 401 {
				t.Fatalf("replica auth = %d", unauthorized.Code)
			}
			replicaAddr := startTestGRPCServer(t, s, s.db)
			conn, err := dialPeerGRPC(replicaAddr, defaultMaxGRPCMsgBytes, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			callCtx, callCancel := replicaCallContext(ctx, opts)
			defer callCancel()
			var result queryResponse
			if err := conn.Invoke(callCtx, "/tinysql.TinySQL/Query", &queryRequest{SQL: "SELECT name FROM cluster_items"}, &result); err != nil || result.Error != "" || result.Count != 1 {
				t.Fatalf("gRPC read: %+v %v", result, err)
			}
			var write execResponse
			if err := conn.Invoke(callCtx, "/tinysql.TinySQL/Exec", &execRequest{SQL: "DELETE FROM cluster_items"}, &write); err != nil || write.Success || !strings.Contains(write.Error, "read-only") {
				t.Fatalf("gRPC write: %+v %v", write, err)
			}
			// An idle primary must continue renewing readiness (also verifies
			// that the 300ms unary timeout does not terminate the stream).
			until := time.Now().Add(2300 * time.Millisecond)
			for time.Now().Before(until) {
				if !s.isReady() {
					t.Fatal("idle primary lost replica readiness")
				}
				time.Sleep(20 * time.Millisecond)
			}
			grpcSrv.Stop()
			awaitCluster(t, "primary outage withdraws readiness", func() bool { return !s.isReady() })
			if got := clusterRequest(t, handler, "/api/query", "SELECT 1").Code; got != 503 {
				t.Fatalf("unavailable query = %d", got)
			}
			if _, err := s.Query(ctx, &queryRequest{SQL: "SELECT 1"}); status.Code(err) != codes.Unavailable {
				t.Fatalf("unavailable gRPC status: %v", err)
			}
			grpcSrv, _, err = startGRPCServer(primary, primaryDB, addr, tls.VersionTLS12, make(chan error, 2))
			if err != nil {
				t.Fatal(err)
			}
			awaitCluster(t, "reconnect", s.isReady)
		})
	}
}

func TestReplicaBootstrapRetriesWhilePrimaryUnavailable(t *testing.T) {
	primary, db := newAdvancedWALTestServer(t)
	primary.authToken = "cluster-secret"
	primary.ready.Store(false)
	addr := startTestGRPCServer(t, primary, db)
	s := newServingReplica(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	opts := replicaOptions{AuthToken: s.authToken, CallTimeout: time.Second, MaxRecvMsgSize: defaultMaxGRPCMsgBytes, Observe: s.replica.observe}
	go func() { defer close(done); s.serveReplicaLoop(ctx, addr, opts, runReplicaPollLoop) }()
	t.Cleanup(func() { cancel(); <-done })
	awaitCluster(t, "bootstrap failure", func() bool {
		return strings.Contains(s.replica.snapshot()["last_error"].(string), "not ready")
	})
	if s.isReady() {
		t.Fatal("replica ready without bootstrap")
	}
	primary.ready.Store(true)
	awaitCluster(t, "bootstrap retry", s.isReady)
}

func TestReplicaSilentConnectionExpiresAndRecovers(t *testing.T) {
	s := newServingReplica(t)
	s.replica.observe(42, 7, nil)
	if !s.isReady() {
		t.Fatal("successful response did not enable readiness")
	}
	s.replica.mu.Lock()
	s.replica.lastOK = time.Now().Add(-2 * s.replica.timeout)
	s.replica.mu.Unlock()
	if s.isReady() {
		t.Fatal("silent connection stayed ready")
	}
	s.replica.observe(43, 7, nil)
	if !s.isReady() {
		t.Fatal("heartbeat did not recover readiness")
	}
	s.replica.observe(0, 0, errors.New("disconnected"))
	if s.isReady() {
		t.Fatal("disconnect stayed ready")
	}
}

func TestReplicaSnapshotReplacementDrainsStreams(t *testing.T) {
	s := newServingReplica(t)
	s.execSem = make(chan struct{}, 1)
	s.replica.observe(1, 1, nil)
	stream, err := s.openQueryStream(context.Background(), &queryRequest{SQL: "SELECT 42 AS answer"})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	s.replica.observe(0, 0, errReplicaNeedsRebootstrap)
	done := make(chan struct{})
	go func() { s.replaceReplicaDatabase(storage.NewDB()); close(done) }()
	select {
	case <-done:
		t.Fatal("replaced snapshot before stream closed")
	case <-time.After(20 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := s.Query(ctx, &queryRequest{SQL: "SELECT 1"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("re-bootstrap accepted new query: %v", err)
	}
	if got := clusterRequest(t, s.httpHandler(), "/readyz/read", "").Code; got != 503 {
		t.Fatalf("re-bootstrap readiness: %d", got)
	}
	stream.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("snapshot replacement did not finish after stream close")
	}
}

func TestPrimaryWriteReadiness(t *testing.T) {
	s, _ := newAdvancedWALTestServer(t)
	if w := clusterRequest(t, s.httpHandler(), "/readyz/write", ""); w.Code != 200 {
		t.Fatalf("primary write readiness: %d", w.Code)
	}
	// A listener failure must be returned to the runtime that owns the DB.
	_, _, err := startGRPCServer(s, s.db, "invalid-address", tls.VersionTLS12, make(chan error, 1))
	if err == nil || !s.db.HealthCheck().OK {
		t.Fatalf("listener failure closed DB: %v", err)
	}
}
