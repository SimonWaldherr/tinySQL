package driver_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/testutil"
)

func TestNativeTimestampScan(t *testing.T) {
	db := testutil.Open(t, "CREATE TABLE timestamp_inputs (id INT, value TIMESTAMP)")
	input := "2026-09-08T12:34:56.123456789+02:30"
	if _, err := db.Exec("INSERT INTO timestamp_inputs VALUES (?, ?)", 1, input); err != nil {
		t.Fatal(err)
	}
	want, err := time.Parse(time.RFC3339Nano, input)
	if err != nil {
		t.Fatal(err)
	}
	want = want.AddDate(0, 0, 1)
	for _, query := range []string{
		"SELECT DATE_ADD(value, 1, 'DAY') AS value FROM timestamp_inputs",
		"SELECT DATE_ADD(value, 1, 'DAY') AS value FROM timestamp_inputs ORDER BY id",
		"SELECT DATE_ADD(?, 1, 'DAY') AS value",
		"SELECT DATE_ADD($1, 1, 'DAY') AS value",
	} {
		t.Run(query, func(t *testing.T) {
			var args []any
			if query == "SELECT DATE_ADD(?, 1, 'DAY') AS value" || query == "SELECT DATE_ADD($1, 1, 'DAY') AS value" {
				args = []any{input}
			}
			var got time.Time
			if err := db.QueryRow(query, args...).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if !got.Equal(want) || got.Format(time.RFC3339Nano) != want.Format(time.RFC3339Nano) {
				t.Fatalf("got %v, want %v", got, want)
			}
			var nullable sql.NullTime
			if err := db.QueryRow(query, args...).Scan(&nullable); err != nil {
				t.Fatal(err)
			}
			if !nullable.Valid || !nullable.Time.Equal(want) {
				t.Fatalf("got %+v, want %v", nullable, want)
			}
			var text string
			if err := db.QueryRow(query, args...).Scan(&text); err != nil {
				t.Fatal(err)
			}
			if text != want.Format(time.RFC3339Nano) {
				t.Fatalf("text changed: %q", text)
			}
		})
	}
	var null sql.NullTime
	if err := db.QueryRow("SELECT NULL AS value").Scan(&null); err != nil || null.Valid {
		t.Fatalf("NULL: %+v, %v", null, err)
	}
}
