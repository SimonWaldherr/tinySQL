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
		{"SELECT -- ? $2\r?", 1},
		{"SELECT -- ? $2\r$1", 1},
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

func TestParameterBindingAfterCRLineComment(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=cr_comment_binding")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{"SELECT -- ? $2\r? AS value", "SELECT -- ? $2\r$1 AS value"} {
		t.Run(query, func(t *testing.T) {
			var got int
			if err := db.QueryRow(query, 42).Scan(&got); err != nil || got != 42 {
				t.Fatalf("direct query: value=%d err=%v", got, err)
			}
			stmt, err := db.Prepare(query)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			if err := stmt.QueryRow(42).Scan(&got); err != nil || got != 42 {
				t.Fatalf("prepared query: value=%d err=%v", got, err)
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
