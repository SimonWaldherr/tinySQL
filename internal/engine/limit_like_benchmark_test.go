package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkLimitLike(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { db.Close() })
	table := storage.NewTable("events", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 50000; i++ {
		body := "INFO_request/" + strings.Repeat("Received Payload ", 16)
		if i == 45000 {
			body = "ERROR_request/timeout"
		}
		table.Rows = append(table.Rows, []any{i, body})
	}
	if err := table.CreateSecondaryIndex("events_id", []string{"id"}, false); err != nil {
		b.Fatal(err)
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"zero_scan", `SELECT * FROM events WHERE body LIKE '%missing%' LIMIT 0`},
		{"zero_index", `SELECT * FROM events WHERE id >= 0 LIMIT 0`},
		{"escaped_prefix", `SELECT * FROM events WHERE body LIKE 'ERROR\_%' LIMIT 1`},
		{"escaped_contains", `SELECT * FROM events WHERE body LIKE '%ERROR\_%' LIMIT 1`},
		{"ilike_prefix", `SELECT * FROM events WHERE body ILIKE 'error%' LIMIT 1`},
		{"like_prefix", `SELECT * FROM events WHERE body LIKE 'ERROR%' LIMIT 1`},
		{"small_page", `SELECT * FROM events WHERE body LIKE 'INFO%' LIMIT 10 OFFSET 10`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
