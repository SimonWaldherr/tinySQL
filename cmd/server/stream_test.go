package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func newQueryStreamTestServer(t testing.TB) *server {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	s := newServer(db, "default", "", nil, nil, nil)
	// Tests set an explicit cap only when they are testing that behavior.
	s.maxResponseRows = 0
	s.maxResponseBytes = 0
	return s
}

func seedQueryStreamRows(t testing.TB, s *server, rows int) {
	t.Helper()
	ctx := context.Background()
	resp, err := s.Exec(ctx, &execRequest{SQL: "CREATE TABLE users (id INT, name TEXT)"})
	if err != nil || !resp.Success {
		t.Fatalf("create table: resp=%+v err=%v", resp, err)
	}
	for i := 0; i < rows; i++ {
		resp, err = s.Exec(ctx, &execRequest{SQL: fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i)})
		if err != nil || !resp.Success {
			t.Fatalf("insert %d: resp=%+v err=%v", i, resp, err)
		}
	}
}

func decodeNDJSONEvents(t *testing.T, r io.Reader) []queryStreamResponse {
	t.Helper()
	dec := json.NewDecoder(r)
	var events []queryStreamResponse
	for {
		var event queryStreamResponse
		err := dec.Decode(&event)
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("decode NDJSON: %v", err)
		}
		events = append(events, event)
	}
}

func TestHandleQueryStreamNDJSON(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 3)

	req := httptest.NewRequest(http.MethodPost, "/api/query/stream", strings.NewReader(`{"sql":"SELECT id, name FROM users"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	// Exercise the actual instrumentation wrapper too: statusRecorder must
	// remain transparent to ResponseController.Flush for NDJSON streaming.
	s.instrumentHTTP("/api/query/stream", s.handleQueryStream)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/x-ndjson") {
		t.Fatalf("Content-Type = %q, want application/x-ndjson", got)
	}
	if !rec.Flushed {
		t.Fatal("expected the NDJSON response to be flushed incrementally")
	}

	events := decodeNDJSONEvents(t, rec.Body)
	if len(events) != 5 { // header + 3 rows + end
		t.Fatalf("event count = %d, want 5: %#v", len(events), events)
	}
	if got := events[0]; got.Type != "header" || !equalStringSlices(got.Columns, []string{"id", "name"}) {
		t.Fatalf("header = %#v, want id/name columns", got)
	}
	for i := 0; i < 3; i++ {
		if got := events[i+1]; got.Type != "row" {
			t.Fatalf("event %d type = %q, want row", i+1, got.Type)
		}
		if got := fmt.Sprint(events[i+1].Row["id"]); got != fmt.Sprint(i) {
			t.Fatalf("row %d id = %q, want %d", i, got, i)
		}
	}
	end := events[len(events)-1]
	if end.Type != "end" || end.Count != 3 || end.Truncated {
		t.Fatalf("end = %#v, want count=3 and not truncated", end)
	}
}

func TestHandleQueryStreamHonorsResponseCapAndReleasesSlot(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 4)
	s.maxResponseRows = 2
	s.execSem = make(chan struct{}, 1)

	req := httptest.NewRequest(http.MethodPost, "/api/query/stream", strings.NewReader(`{"sql":"SELECT id FROM users"}`))
	rec := httptest.NewRecorder()
	s.handleQueryStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	events := decodeNDJSONEvents(t, rec.Body)
	if len(events) != 4 { // header + 2 rows + end
		t.Fatalf("event count = %d, want 4: %#v", len(events), events)
	}
	end := events[len(events)-1]
	if end.Type != "end" || end.Count != 2 || !end.Truncated {
		t.Fatalf("end = %#v, want count=2 and truncated", end)
	}
	if got := len(s.execSem); got != 0 {
		t.Fatalf("execution slot remains held after capped stream: %d", got)
	}
}

func TestHandleQueryStreamAuthAndPreflightErrors(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 1)
	s.authToken = "secret"
	h := s.withAuth(s.handleQueryStream)

	unauthorized := httptest.NewRecorder()
	h(unauthorized, httptest.NewRequest(http.MethodPost, "/api/query/stream", strings.NewReader(`{"sql":"SELECT id FROM users"}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	invalidReq := httptest.NewRequest(http.MethodPost, "/api/query/stream", strings.NewReader(`{"sql":"SELECT FROM"}`))
	invalidReq.Header.Set("Authorization", "Bearer secret")
	invalid := httptest.NewRecorder()
	h(invalid, invalidReq)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid SQL status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
	if got := invalid.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("preflight failure Content-Type = %q, want application/json", got)
	}
}

func TestOpenQueryStreamCloseCancelsAndReleasesSlot(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 100)
	s.execSem = make(chan struct{}, 1)

	query, err := s.openQueryStream(context.Background(), &queryRequest{SQL: "SELECT id FROM users"})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if got := len(s.execSem); got != 1 {
		t.Fatalf("execution slots in use = %d, want 1", got)
	}

	// The producer has more rows than its bounded buffer and may be blocked
	// behind the consumer. Close must cancel it and wait for its read lock.
	done := make(chan struct{})
	go func() {
		query.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock the producer")
	}
	if got := len(s.execSem); got != 0 {
		t.Fatalf("execution slot remains held after Close: %d", got)
	}
}

func openTestQueryStream(ctx context.Context, conn *grpc.ClientConn, req *queryRequest) (grpc.ClientStream, error) {
	desc := &grpc.StreamDesc{StreamName: "QueryStream", ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, "/tinysql.TinySQL/QueryStream")
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(req); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	return stream, nil
}

func recvQueryStreamEvents(t *testing.T, stream grpc.ClientStream) ([]queryStreamResponse, error) {
	t.Helper()
	var events []queryStreamResponse
	for {
		var event queryStreamResponse
		err := stream.RecvMsg(&event)
		if errors.Is(err, io.EOF) {
			return events, nil
		}
		if err != nil {
			return events, err
		}
		events = append(events, event)
	}
}

func TestGRPCQueryStream(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 3)
	addr := startTestGRPCServer(t, s, s.db)

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		t.Fatalf("dial gRPC server: %v", err)
	}
	defer conn.Close()

	stream, err := openTestQueryStream(context.Background(), conn, &queryRequest{SQL: "SELECT id FROM users"})
	if err != nil {
		t.Fatalf("open QueryStream: %v", err)
	}
	events, err := recvQueryStreamEvents(t, stream)
	if err != nil {
		t.Fatalf("receive QueryStream: %v", err)
	}
	if len(events) != 5 { // header + 3 rows + end
		t.Fatalf("event count = %d, want 5: %#v", len(events), events)
	}
	if events[0].Type != "header" || !equalStringSlices(events[0].Columns, []string{"id"}) {
		t.Fatalf("header = %#v, want id column", events[0])
	}
	if got := events[len(events)-1]; got.Type != "end" || got.Count != 3 || got.Truncated {
		t.Fatalf("end = %#v", got)
	}
}

func TestGRPCQueryStreamAuthAndErrorStatus(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 1)
	s.authToken = "secret"
	addr := startTestGRPCServer(t, s, s.db)

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		t.Fatalf("dial gRPC server: %v", err)
	}
	defer conn.Close()

	unauthenticated, err := openTestQueryStream(context.Background(), conn, &queryRequest{SQL: "SELECT id FROM users"})
	if err != nil {
		t.Fatalf("open unauthenticated stream: %v", err)
	}
	if _, err := recvQueryStreamEvents(t, unauthenticated); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated stream status = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	authedCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer secret")
	invalid, err := openTestQueryStream(authedCtx, conn, &queryRequest{SQL: "SELECT FROM"})
	if err != nil {
		t.Fatalf("open invalid statement stream: %v", err)
	}
	if _, err := recvQueryStreamEvents(t, invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid statement stream status = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

// BenchmarkServerStreamFirstRow measures the server's local request path
// through compilation/cache lookup, the execution semaphore and stream
// startup. Transport write time is deliberately excluded; HTTP and gRPC both
// consume the same ResultStream synchronously after this point.
func BenchmarkServerStreamFirstRow(b *testing.B) {
	s := newQueryStreamTestServer(b)
	seedQueryStreamRows(b, s, 256)
	ctx := context.Background()
	req := &queryRequest{SQL: "SELECT id FROM users"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query, err := s.openQueryStream(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if !query.stream.Next() {
			err := query.stream.Err()
			query.Close()
			b.Fatalf("first row: %v", err)
		}
		query.Close()
	}
}
