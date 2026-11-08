package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBootstrapStreamExceedsUnaryLimit(t *testing.T) {
	s, db := newAdvancedWALTestServer(t)
	s.authToken = "bootstrap-secret"
	table := storage.NewTable("large_bootstrap", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "payload", Type: storage.BlobType}}, false)
	table.Rows = make([][]any, 12000)
	for i := range table.Rows {
		payload := make([]byte, 512)
		payload[0] = byte(i)
		table.Rows[i] = []any{i, payload}
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	addr := startTestGRPCServer(t, s, db)
	opts := replicaOptions{AuthToken: s.authToken, MaxRecvMsgSize: 8192, CallTimeout: 5 * time.Second}
	loaded, _, epoch, err := runReplicaBootstrap(t.Context(), addr, "default", opts)
	if err != nil || epoch == 0 {
		t.Fatal("chunked bootstrap under an 8 KiB message limit", epoch, err)
	}
	defer loaded.Close()
	got, err := loaded.Get("default", table.Name)
	if err != nil || len(got.Rows) != len(table.Rows) || got.Rows[11999][1].([]byte)[0] != byte(11999%256) {
		t.Fatal("large bootstrap lost rows", err)
	}
	// The same peer and receive limit cannot accept the historical unary image.
	conn, err := dialPeerGRPC(addr, opts.MaxRecvMsgSize, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := replicaCallContext(t.Context(), opts)
	defer cancel()
	err = conn.Invoke(ctx, "/tinysql.TinySQL/Bootstrap", &bootstrapRequest{}, &bootstrapResponse{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unary bootstrap unexpectedly fits the receive limit", err)
	}
	opts.AuthToken = "wrong-token"
	if _, _, _, err := runReplicaBootstrap(t.Context(), addr, "default", opts); status.Code(err) != codes.Unauthenticated {
		t.Fatal("stream bootstrap bypassed authentication", err)
	}
}

func TestBootstrapStreamFallsBackToLegacyPrimary(t *testing.T) {
	s, db := newAdvancedWALTestServer(t)
	if err := db.Put("default", storage.NewTable("legacy", []storage.Column{{Name: "id", Type: storage.IntType}}, false)); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	legacy := grpc.NewServer(grpc.ForceServerCodec(protobufCodec{}))
	legacy.RegisterService(&grpc.ServiceDesc{
		ServiceName: "tinysql.TinySQL", HandlerType: (*TinySQLServer)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Bootstrap", Handler: _TinySQL_Bootstrap_Handler}},
	}, s)
	go legacy.Serve(lis)
	t.Cleanup(func() { legacy.Stop(); lis.Close() })
	loaded, _, epoch, err := runReplicaBootstrap(t.Context(), lis.Addr().String(), "default", replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes, CallTimeout: 3 * time.Second})
	if err != nil || epoch == 0 {
		t.Fatal("old primary fallback", epoch, err)
	}
	defer loaded.Close()
	if _, err := loaded.Get("default", "legacy"); err != nil {
		t.Fatal(err)
	}
}

type scriptedBootstrapClient struct {
	grpc.ClientStream
	chunks []bootstrapChunk
	end    error
}

func (s *scriptedBootstrapClient) RecvMsg(v any) error {
	if len(s.chunks) == 0 {
		if s.end != nil {
			return s.end
		}
		return io.EOF
	}
	*v.(*bootstrapChunk) = s.chunks[0]
	s.chunks = s.chunks[1:]
	return nil
}

func TestBootstrapChunkReaderRequiresCompletionAndSuccessfulRPC(t *testing.T) {
	for _, test := range []struct {
		name   string
		chunks []bootstrapChunk
		end    error
		ok     bool
	}{
		{"complete", []bootstrapChunk{{Data: []byte("snapshot")}, {Complete: true, Epoch: 1}}, nil, true},
		{"truncated", []bootstrapChunk{{Data: []byte("snapshot")}}, nil, false},
		{"empty", nil, nil, false},
		{"bad-completion", []bootstrapChunk{{Complete: true}}, nil, false},
		{"mixed-completion", []bootstrapChunk{{Data: []byte("x"), Complete: true, Epoch: 1}}, nil, false},
		{"extra-message", []bootstrapChunk{{Complete: true, Epoch: 1}, {Data: []byte("x")}}, nil, false},
		{"late-error", []bootstrapChunk{{Complete: true, Epoch: 1}}, status.Error(codes.Unavailable, "disconnected"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &bootstrapChunkReader{stream: &scriptedBootstrapClient{chunks: test.chunks, end: test.end}}
			_, err := io.ReadAll(reader)
			if (err == nil) != test.ok {
				t.Fatal("completion result", err)
			}
		})
	}
}

func TestBootstrapAdmissionStopsOnDrain(t *testing.T) {
	s, _ := newAdvancedWALTestServer(t)
	release, err := s.acquireBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := s.replicationContext(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		undo, err := s.acquireBootstrap(ctx)
		if undo != nil {
			undo()
		}
		done <- err
	}()
	s.beginShutdown()
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatal("waiting bootstrap ignored drain", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting bootstrap did not stop")
	}
}

func TestClosingServerWaitsForDatabaseLease(t *testing.T) {
	s, db := newAdvancedWALTestServer(t)
	unlock, err := s.acquireDatabase()
	if err != nil {
		t.Fatal(err)
	}
	s.beginShutdown()
	done := make(chan error, 1)
	go func() { done <- s.closeDatabase() }()
	select {
	case err := <-done:
		unlock()
		t.Fatal("closed a leased database", err)
	case <-time.After(40 * time.Millisecond):
	}
	if db.HealthCheck().Closed {
		unlock()
		t.Fatal("active database was closed")
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not finish after lease release")
	}
}

func TestShutdownDrainsReplicationBeforeClosingDatabase(t *testing.T) {
	oldTimeout := *flagShutdownTimeout
	*flagShutdownTimeout = 5 * time.Second
	defer func() { *flagShutdownTimeout = oldTimeout }()
	cfg := storage.StorageConfig{Mode: storage.ModeAdvancedWAL, Path: filepath.Join(t.TempDir(), "shutdown")}
	db, err := storage.OpenDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s := newServer(db, "default", "", nil, nil, nil)
	for _, sql := range []string{"CREATE TABLE durable (id INT)", "INSERT INTO durable VALUES (1)"} {
		if resp, err := s.Exec(t.Context(), &execRequest{SQL: sql}); err != nil || !resp.Success {
			t.Fatal(resp, err)
		}
	}
	grpcSrv, addr, err := startGRPCServer(s, db, "127.0.0.1:0", tls.VersionTLS12, make(chan error, 2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(grpcSrv.Stop)
	conn, err := dialPeerGRPC(addr, defaultMaxGRPCMsgBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := db.AdvancedWAL().Checkpoint(db); err != nil {
		t.Fatal(err)
	}
	watermark := uint64(db.AdvancedWAL().GetNextLSN() - 1)
	stream, streamCancel, err := openChangesStream(ctx, conn, "default", watermark, replicaOptions{MaxRecvMsgSize: defaultMaxGRPCMsgBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer streamCancel()
	if err := stream.RecvMsg(&getChangesSinceResponse{}); err != nil {
		t.Fatal(err)
	}
	s.beginShutdown()
	s.ready.Store(true) // a concurrent bootstrap/heartbeat cannot restore readiness
	if s.isReady() {
		t.Fatal("draining node became ready again")
	}
	done := make(chan error, 1)
	go func() { done <- errors.Join(shutdownRunServers(nil, grpcSrv, nil), s.closeDatabase()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		grpcSrv.Stop()
		<-done
		t.Fatal("idle replication consumed the graceful timeout")
	}
	if !db.HealthCheck().Closed {
		t.Fatal("database was not closed")
	}
	reopened, err := storage.OpenDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	table, err := reopened.Get("default", "durable")
	if err != nil || len(table.Rows) != 1 || table.Rows[0][0] != 1 {
		t.Fatal("shutdown lost committed data", table, err)
	}
	if err := stream.RecvMsg(&getChangesSinceResponse{}); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("replication feed did not terminate during drain", err)
	}
}
