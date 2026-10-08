package driver

import (
	"bytes"
	"context"
	"database/sql/driver"
	"io"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompletedStreamReleasesResourcesBeforeConsumption(t *testing.T) {
	for _, rowCount := range []int{0, 1, 64} {
		t.Run(strconv.Itoa(rowCount), func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			srv := newServer(db, cfg{tenant: "default", maxReaders: 1})
			c := &conn{srv: srv, tenant: "default"}
			if _, err := c.execSQL(context.Background(), `CREATE TABLE completed_rows (id INT, payload BLOB)`); err != nil {
				t.Fatal(err)
			}
			table, err := db.Get("default", "completed_rows")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < rowCount; i++ {
				table.Rows = append(table.Rows, []any{i, []byte{byte(i), 255}})
			}
			table.Version++
			prepared, err := buildPreparedQuery(`SELECT id, ? AS bound_payload, payload FROM completed_rows`)
			if err != nil || prepared == nil {
				t.Fatalf("prepare: %v", err)
			}
			exec, err := prepared.acquire()
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte{1, 2, 255}
			prepared.bind(exec, []driver.NamedValue{{Ordinal: 1, Value: payload}})
			var releases atomic.Int32
			rawRows, err := c.queryStatementWithCleanup(context.Background(), exec.statement, func() {
				prepared.release(exec)
				releases.Add(1)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer rawRows.Close()
			if got := releases.Load(); got != 1 {
				t.Fatalf("cleanup calls before consuming rows = %d, want 1", got)
			}
			if got := len(srv.readerPool); got != 0 {
				t.Fatalf("reserved readers = %d, want 0", got)
			}
			// Rebind the released AST before reading its buffered output. Those
			// rows must retain the original values independently of the AST.
			reused, err := prepared.acquire()
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.release(reused)
			prepared.bind(reused, []driver.NamedValue{{Ordinal: 1, Value: []byte{9}}})
			dest := make([]driver.Value, 3)
			for i := 0; i < rowCount; i++ {
				if err := rawRows.Next(dest); err != nil {
					t.Fatal(err)
				}
				if dest[0] != int64(i) || !bytes.Equal(dest[1].([]byte), payload) || !bytes.Equal(dest[2].([]byte), []byte{byte(i), 255}) {
					t.Fatalf("row %d changed after releasing its AST: %v", i, dest)
				}
			}
			if err := rawRows.Next(dest); err != io.EOF {
				t.Fatalf("terminal Next = %v, want EOF", err)
			}
			if err := rawRows.Close(); err != nil {
				t.Fatal(err)
			}
			if got := releases.Load(); got != 1 {
				t.Fatalf("cleanup calls after Close = %d, want 1", got)
			}
		})
	}
}

func BenchmarkDriverCompletedSelect(b *testing.B) {
	db := openStreamingDriverDB(b, 64)
	if _, err := db.Exec(`CREATE UNIQUE INDEX completed_id ON stream_rows(id)`); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name, query string
		want        int
	}{
		{"Point", `SELECT id FROM stream_rows WHERE id = 42`, 1},
		{"Limit1", `SELECT id FROM stream_rows LIMIT 1`, 1},
		{"Limit64", `SELECT id FROM stream_rows LIMIT 64`, 64},
		{"Empty", `SELECT id FROM stream_rows WHERE id = -1`, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rows, err := db.QueryContext(b.Context(), tc.query)
				if err != nil {
					b.Fatal(err)
				}
				count := 0
				for rows.Next() {
					var id int64
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						b.Fatal(err)
					}
					count++
				}
				if err := rows.Err(); err != nil {
					b.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					b.Fatal(err)
				}
				if count != tc.want {
					b.Fatalf("rows = %d, want %d", count, tc.want)
				}
			}
		})
	}
}
