package engine

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var regexpComparisonCases = []struct {
	name   string
	regexp string
	like   string
	substr string
}{
	{"prefix", `body REGEXP '^ERROR_'`, `body LIKE 'ERROR\_%'`, `SUBSTR(body, 1, 6) = 'ERROR_'`},
	{"suffix", `body REGEXP 'timeout$'`, `body LIKE '%timeout'`, `SUBSTR(body, -7) = 'timeout'`},
	// (?s) makes dot include newlines, like LIKE's %. These two literals
	// cannot overlap, so the pair of SUBSTR checks has the same meaning too.
	{"segments", `body REGEXP '(?s)^ERROR_.*timeout$'`, `body LIKE 'ERROR\_%timeout'`, `SUBSTR(body, 1, 6) = 'ERROR_' AND SUBSTR(body, -7) = 'timeout'`},
}

func TestRegexpLikeSubstrEquivalentFilters(t *testing.T) {
	db := regexpComparisonDB(t, 1000)
	table, err := db.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []any{"", "ERROR_", "timeout", "ERROR_timeout", "ERROR_é🙂\ntimeout", "ERROR_timeout\n", "error_timeout", "ERROR_timeout detail", "ERROR_\xfftimeout", "K漢timeout", 123, []byte("ERROR_timeout"), nil} {
		table.Rows = append(table.Rows, []any{len(table.Rows), body})
	}
	table.Version++
	for _, tc := range regexpComparisonCases {
		want := execSQL(t, db, `SELECT id FROM events WHERE `+tc.regexp)
		for _, predicate := range []string{tc.like, tc.substr} {
			got := execSQL(t, db, `SELECT id FROM events WHERE `+predicate)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %s returned different rows", tc.name, predicate)
			}
		}
	}
}

func regexpComparisonDB(tb testing.TB, n int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { db.Close() })
	table := storage.NewTable("events", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "body", Type: storage.TextType}}, false)
	payload := strings.Repeat("request payload ", 16)
	for i := 0; i < n; i++ {
		var body any = "INFO_" + payload + "accepted"
		switch i % 100 {
		case 0:
			body = "ERROR_" + payload + "timeout"
		case 1:
			body = "ERROR_" + payload + "denied"
		case 2:
			body = "INFO_" + payload + "timeout"
		case 3:
			body = "INFO_" + payload + "ERROR_timeout detail"
		case 4:
			body = nil
		}
		table.Rows = append(table.Rows, []any{i, body})
	}
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	return db
}

// All three expressions in a group must return the same count. COUNT avoids
// burying predicate costs under result-map allocations for thousands of rows.
func BenchmarkRegexpLikeSubstr(b *testing.B) {
	db := regexpComparisonDB(b, 50000)
	for _, tc := range regexpComparisonCases {
		for _, variant := range []struct{ name, predicate string }{{"regexp", tc.regexp}, {"like", tc.like}, {"substr", tc.substr}} {
			b.Run(tc.name+"/"+variant.name, func(b *testing.B) {
				stmt := mustParse(`SELECT COUNT(*) AS hits FROM events WHERE ` + variant.predicate)
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
}
