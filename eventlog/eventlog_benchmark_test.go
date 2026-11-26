package eventlog_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/eventlog"
)

func BenchmarkReadEventBacklog(b *testing.B) {
	const retained = 100_000
	db, err := driver.OpenInMemory("")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	log, err := eventlog.Open(b.Context(), db)
	if err != nil {
		b.Fatal(err)
	}
	tx, err := db.BeginTx(b.Context(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	var batch strings.Builder
	for start := 1; start <= retained; start += 1000 {
		batch.Reset()
		batch.WriteString("INSERT INTO _tiny_event_log VALUES ")
		for seq := start; seq < start+1000; seq++ {
			if seq > start {
				batch.WriteByte(',')
			}
			fmt.Fprintf(&batch, "(%d, 'key', 'topic', X'CAFE', 0)", seq)
		}
		if _, err := tx.ExecContext(b.Context(), batch.String()); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(b.Context(), "UPDATE _tiny_event_state SET value = ? WHERE name = 'sequence'", retained); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		cursor int64
		limit  int
	}{
		{"backlog", 1000, 100},
		{"large-batch", 1000, 10_000},
		{"tail", retained - 16, 100},
		{"idle", retained, 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if err := log.RegisterConsumer(b.Context(), tc.name, tc.cursor); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				got, err := log.Read(b.Context(), tc.name, tc.limit)
				if err != nil {
					b.Fatal(err)
				}
				if want := min(tc.cursor+int64(tc.limit), retained); got.To != want || int64(len(got.Events)) != want-tc.cursor {
					b.Fatalf("read %d events through %d, want %d through %d", len(got.Events), got.To, want-tc.cursor, want)
				}
			}
		})
	}

	b.Run("publish-read-ack", func(b *testing.B) {
		if err := log.RegisterConsumer(b.Context(), "live", retained); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			seq, err := log.Publish(b.Context(), "key", "topic", []byte("message"))
			if err != nil {
				b.Fatal(err)
			}
			batch, err := log.Read(b.Context(), "live", 100)
			if err != nil {
				b.Fatal(err)
			}
			if len(batch.Events) != 1 || batch.To != seq {
				b.Fatalf("read %d events through %d, want 1 through %d", len(batch.Events), batch.To, seq)
			}
			if err := batch.Ack(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
	})
}
