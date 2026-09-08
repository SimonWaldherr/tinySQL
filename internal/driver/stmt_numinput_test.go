package driver

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestPreparedNumInput(t *testing.T) {
	c := new(conn)
	for _, tc := range []struct {
		sql  string
		want int
	}{
		{"SELECT ?", 1},
		{"SELECT ?, 'it''s ?', ?", 2},
		{"SELECT 1", -1},
		{"SELECT $1, $1", 1},
		{"SELECT :2, :1", 2},
		{"SELECT ?, $1", -1},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			s, err := c.PrepareContext(context.Background(), tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if got := s.NumInput(); got != tc.want {
				t.Fatalf("NumInput=%d, want %d", got, tc.want)
			}
		})
	}
}

func TestSQLPreparedArgumentCount(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=prepared_count")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := db.Prepare("SELECT ? AS value")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, args := range [][]any{nil, {1, 2}} {
		var v int
		err := s.QueryRow(args...).Scan(&v)
		if err == nil || !strings.Contains(err.Error(), "sql: expected 1 arguments") {
			t.Fatalf("expected database/sql count error, got %v", err)
		}
	}
	var v int
	if err := s.QueryRow(42).Scan(&v); err != nil || v != 42 {
		t.Fatalf("value=%d err=%v", v, err)
	}
}
