package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	serverpb "github.com/SimonWaldherr/tinySQL/cmd/server/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestProtobufCodecRoundTripsTransportMessages(t *testing.T) {
	codec := protobufCodec{}
	tests := []struct {
		name string
		in   any
		out  any
	}{
		{"exec request", &execRequest{Tenant: "tenant", SQL: "UPDATE t SET n=1", TimeoutMS: 250}, &execRequest{}},
		{"exec response", &execResponse{Success: true, RowsAffected: 3, LastInsertID: 9, Duration: "1ms"}, &execResponse{}},
		{"query request", &queryRequest{Tenant: "tenant", SQL: "SELECT * FROM t", TimeoutMS: 10, PeerTimeoutMS: 20}, &queryRequest{}},
		{"query response", &queryResponse{SQL: "SELECT", Columns: []string{"id", "blob"}, Rows: []map[string]any{{"id": float64(1), "blob": "base64:AQI="}}, Duration: "2ms", Count: 1, Truncated: true}, &queryResponse{}},
		{"stream response", &queryStreamResponse{Type: "row", Columns: []string{"id"}, Row: map[string]any{"id": float64(1)}, Count: 1}, &queryStreamResponse{}},
		{"bootstrap", &bootstrapResponse{SnapshotGob: []byte{0, 1, 2, 3}, WatermarkLSN: 42, Epoch: 7}, &bootstrapResponse{}},
		{"changes", &getChangesSinceResponse{RecordsGob: []byte{9, 8, 7}, ResumeLSN: 99, Epoch: 7}, &getChangesSinceResponse{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := codec.Marshal(test.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if err := codec.Unmarshal(encoded, test.out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(test.out, test.in) {
				t.Fatalf("round trip = %#v, want %#v", test.out, test.in)
			}
		})
	}
}

func TestProtobufCodecSkipsUnknownFields(t *testing.T) {
	codec := protobufCodec{}
	encoded, err := codec.Marshal(&queryRequest{Tenant: "default", SQL: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	encoded = protowire.AppendTag(encoded, 99, protowire.BytesType)
	encoded = protowire.AppendString(encoded, "future")
	var decoded queryRequest
	if err := codec.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Tenant != "default" || decoded.SQL != "SELECT 1" {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestProtobufReplicationEnvelopeAvoidsBase64Expansion(t *testing.T) {
	payload := bytes.Repeat([]byte{0x00, 0xff, 0x7f, 0x80}, 1024)
	message := &bootstrapResponse{SnapshotGob: payload, WatermarkLSN: 42, Epoch: 7}
	protoData, err := (protobufCodec{}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	jsonData, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if len(protoData) >= len(jsonData) {
		t.Fatalf("protobuf payload = %d bytes, JSON/base64 = %d bytes", len(protoData), len(jsonData))
	}
}

func TestProtobufCodecInteroperatesWithGeneratedMessages(t *testing.T) {
	codec := protobufCodec{}
	generated := &serverpb.BootstrapResponse{SnapshotGob: []byte{1, 2, 3}, WatermarkLsn: 42, Epoch: 7}
	encoded, err := codec.Marshal(generated)
	if err != nil {
		t.Fatalf("Marshal generated: %v", err)
	}
	var plain bootstrapResponse
	if err := codec.Unmarshal(encoded, &plain); err != nil {
		t.Fatalf("Unmarshal plain: %v", err)
	}
	if !bytes.Equal(plain.SnapshotGob, generated.SnapshotGob) || plain.WatermarkLSN != generated.WatermarkLsn || plain.Epoch != generated.Epoch {
		t.Fatalf("plain response = %#v, generated = %#v", plain, generated)
	}

	encoded, err = codec.Marshal(&getChangesSinceRequest{Tenant: "default", SinceLSN: 99})
	if err != nil {
		t.Fatalf("Marshal plain: %v", err)
	}
	var generatedRequest serverpb.GetChangesSinceRequest
	if err := codec.Unmarshal(encoded, &generatedRequest); err != nil {
		t.Fatalf("Unmarshal generated: %v", err)
	}
	if generatedRequest.Tenant != "default" || generatedRequest.SinceLsn != 99 {
		t.Fatalf("generated request tenant/since = %q/%d", generatedRequest.Tenant, generatedRequest.SinceLsn)
	}
}

func TestGeneratedProtobufClientQueriesServer(t *testing.T) {
	s := newQueryStreamTestServer(t)
	seedQueryStreamRows(t, s, 2)
	addr := startTestGRPCServer(t, s, s.db)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()

	response, err := serverpb.NewTinySQLClient(conn).Query(context.Background(), &serverpb.QueryRequest{Sql: "SELECT id, name FROM users ORDER BY id"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if response.Count != 2 || len(response.RowsJson) != 2 {
		t.Fatalf("response count/rows = %d/%d", response.Count, len(response.RowsJson))
	}
	var first map[string]any
	if err := json.Unmarshal(response.RowsJson[0], &first); err != nil {
		t.Fatalf("decode first row: %v", err)
	}
	if first["name"] != "user_0" {
		t.Fatalf("first row = %#v", first)
	}
}

func BenchmarkReplicationEnvelopeCodecs(b *testing.B) {
	message := &getChangesSinceResponse{
		RecordsGob: bytes.Repeat([]byte{0x00, 0xff, 0x7f, 0x80}, 64*1024),
		ResumeLSN:  1000,
		Epoch:      7,
	}
	protobuf := protobufCodec{}
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := protobuf.Marshal(message); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json-base64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(message); err != nil {
				b.Fatal(err)
			}
		}
	})
}
