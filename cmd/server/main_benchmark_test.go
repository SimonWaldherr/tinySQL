package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func benchmarkServerWithData(b *testing.B, rows int) *server {
	b.Helper()
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })

	s := newServer(db, "default", "", nil, nil, nil)
	if _, err := s.Exec(context.Background(), &execRequest{
		Tenant: "default",
		SQL:    "CREATE TABLE users (id INT, name TEXT)",
	}); err != nil {
		b.Fatalf("create table: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err := s.Exec(context.Background(), &execRequest{
			Tenant: "default",
			SQL:    fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i),
		}); err != nil {
			b.Fatalf("insert row %d: %v", i, err)
		}
	}
	return s
}

func BenchmarkServerQueryPointLookup(b *testing.B) {
	s := benchmarkServerWithData(b, 1000)
	req := &queryRequest{Tenant: "default", SQL: "SELECT id, name FROM users WHERE id = 500"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := s.Query(context.Background(), req)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if resp.Error != "" {
			b.Fatalf("query error: %s", resp.Error)
		}
		if resp.Count != 1 {
			b.Fatalf("expected 1 row, got %d", resp.Count)
		}
	}
}

func BenchmarkParsePeerList_Dedup(b *testing.B) {
	const peers = "node1:9090,node2:9090,node3:9090,node1:9090,node2:9090,node4:9090,node5:9090,node3:9090,node6:9090"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := parsePeerList(peers)
		if len(out) != 6 {
			b.Fatalf("unexpected peer count: %d", len(out))
		}
	}
}
