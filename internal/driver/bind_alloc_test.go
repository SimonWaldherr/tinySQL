package driver

import (
	"database/sql/driver"
	"strings"
	"testing"
)

func TestBindingStorageBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 8, 9, 64} {
		args := make([]driver.NamedValue, count)
		query := "SELECT 1"
		want := query
		if count > 0 {
			query = "SELECT " + strings.TrimSuffix(strings.Repeat("?,", count), ",")
			want = "SELECT " + strings.TrimSuffix(strings.Repeat("'a''b',", count), ",")
		}
		for i := range args {
			args[i] = driver.NamedValue{Ordinal: i + 1, Value: "a'b"}
		}
		got, err := bindPlaceholders(query, args)
		if err != nil || got != want {
			t.Fatalf("count=%d: got %q, err=%v", count, got, err)
		}
	}
	for _, query := range []string{"SELECT ?", "SELECT $1", "SELECT :1"} {
		if _, err := bindPlaceholders(query, nil); err == nil {
			t.Fatalf("missing argument accepted: %s", query)
		}
	}
}

func BenchmarkBindWithoutParameters(b *testing.B) {
	const query = "SELECT id, name FROM users WHERE active = TRUE ORDER BY name"
	b.ReportAllocs()
	for b.Loop() {
		got, err := bindPlaceholders(query, nil)
		if err != nil || got != query {
			b.Fatal(err)
		}
	}
}
