package driver_test

import (
	"bytes"
	"testing"

	"github.com/SimonWaldherr/tinySQL/testutil"
)

func BenchmarkNumberedDriverQuery(b *testing.B) {
	for _, q := range []struct{ name, sql string }{
		{"question", "SELECT ? AS payload, ? AS label"},
		{"numbered", "SELECT $1 AS payload, $2 AS label"},
		{"repeated", "SELECT $2 AS label, $1 AS payload, $2 AS echoed"},
	} {
		b.Run(q.name, func(b *testing.B) {
			db := testutil.Open(b)
			payload := bytes.Repeat([]byte{0, 1, 2, 255}, 1024)
			var got []byte
			var label, echo string
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := db.QueryRowContext(b.Context(), q.sql, payload, "O'Reilly")
				var err error
				if q.name == "repeated" {
					err = row.Scan(&label, &got, &echo)
				} else {
					err = row.Scan(&got, &label)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if !bytes.Equal(got, payload) || label != "O'Reilly" {
				b.Fatal("incorrect result")
			}
		})
	}
}
