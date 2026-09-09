package tinysql

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBuilderLiteralGenerationCompatibility(t *testing.T) {
	values := []any{nil, "", "O'Reilly''", "a\x00b\n漢字", true, false, int(-1000), int64(math.MinInt64), float32(1.2345), math.MaxFloat64, math.SmallestNonzeroFloat64, math.Copysign(0, -1), time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.FixedZone("offset", 9000)), uint64(math.MaxUint64)}
	for _, n := range []int{0, 1, 255, 256, 257, 512, 513, 4096} {
		values = append(values, bytes.Repeat([]byte{0xff}, n))
	}
	for _, value := range values {
		// Reference the former formatting contract, independently of the streamed writer.
		want := fmt.Sprintf("%v", value)
		switch x := value.(type) {
		case nil:
			want = "NULL"
		case string:
			want = "'" + strings.ReplaceAll(x, "'", "''") + "'"
		case []byte:
			want = "X'" + hex.EncodeToString(x) + "'"
		case time.Time:
			want = "'" + x.Format(time.RFC3339Nano) + "'"
		case bool:
			if x {
				want = "TRUE"
			} else {
				want = "FALSE"
			}
		case float32:
			want = strconv.FormatFloat(float64(x), 'f', -1, 32)
		case float64:
			want = strconv.FormatFloat(x, 'f', -1, 64)
		}
		if got := literalToSQL(value); got != want {
			t.Fatalf("%T literal mismatch: got %q want %q", value, got, want)
		}
		query := ToSQL(Select(Val(value)).Build())
		if query != "SELECT "+want {
			t.Fatalf("%T SELECT mismatch", value)
		}
	}
}

func TestGeneratedInsertRoundTrip(t *testing.T) {
	db := NewDB()
	defer db.Close()
	payload := bytes.Repeat([]byte{0, 1, 0xfe, 0xff}, 200)
	sql := ToSQL(InsertInto("generated").Columns("id", "label", "payload").
		Values(Val(1000), Val("O'Reilly\n漢字"), Val(payload)).
		Values(Val(1001), Val("''"), Val([]byte{})).Build())
	for _, query := range []string{"CREATE TABLE generated (id INT, label TEXT, payload BLOB)", sql} {
		stmt, err := ParseSQL(query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Execute(t.Context(), db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	table, err := db.Get("default", "generated")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 2 || table.Rows[0][1] != "O'Reilly\n漢字" || !bytes.Equal(table.Rows[0][2].([]byte), payload) {
		t.Fatal(table.Rows)
	}
}
