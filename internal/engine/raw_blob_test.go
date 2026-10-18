package engine

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRawBlobFunctionsMatchEvaluator(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"a": 0, "b": 1, "h": 2, "n": 3}}
	raw := []any{[]byte{0, 255, 1, 128}, []byte{0, 255, 1, 128}, "00ff0180", nil}
	row := Row{"a": raw[0], "b": raw[1], "h": raw[2], "n": nil}
	for _, text := range []string{
		`BLOB_LENGTH(a)`, `BLOB_HEX(a)`, `BLOB_FROM_HEX(h)`,
		`BLOB_SUBSTR(a, 1, 2)`, `BLOB_SUBSTR(a, 0, -1)`, `BLOB_SUBSTR(a, 0, 0)`,
		`BLOB_CONCAT(a, b)`, `BLOB_CONCAT(a, n)`, `BLOB_CONCAT(n, n)`,
		`BLOB_TO_BASE64(a)`, `BLOB_FROM_BASE64('AP8BgA==')`,
		`BLOB_EQUAL(a, b)`, `BLOB_EQUAL(a, h)`, `BLOB_EQUAL(n, n)`, `BLOB_EQUAL(a, n)`,
		`BLOB_HEX(BLOB_SUBSTR(BLOB_FROM_HEX(h), 1, 2))`,
		`BLOB_LENGTH(n)`, `BLOB_HEX(n)`, `BLOB_SUBSTR(n, 1/0, 1)`,
		`BLOB_FROM_HEX('bad hex')`, `BLOB_FROM_BASE64('invalid!')`,
		`BLOB_EQUAL(a, 123)`, `BLOB_LENGTH(1/0, 1/0)`, `BLOB_SUBSTR(1/0)`,
	} {
		t.Run(text, func(t *testing.T) {
			expr := mustParse("SELECT " + text).(*Select).Projs[0].Expr
			want, wantErr := evalExpr(ExecEnv{}, expr, row)
			got, gotErr := evalRawExpr(plan, raw, expr)
			if (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Fatalf("raw=%v, %v; ordinary=%v, %v", got, gotErr, want, wantErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("raw=%#v; ordinary=%#v", got, want)
			}
		})
	}
	if !bytes.Equal(raw[0].([]byte), []byte{0, 255, 1, 128}) {
		t.Fatal("BLOB evaluation changed the source")
	}
}

func BenchmarkRawBlobFunctions(b *testing.B) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"a": 0, "b": 1}}
	raw := []any{bytes.Repeat([]byte{0, 255, 1, 128}, 2048), bytes.Repeat([]byte{0, 255, 1, 128}, 2048)}
	for _, text := range []string{`BLOB_LENGTH(a)`, `BLOB_EQUAL(a, b)`, `BLOB_SUBSTR(a, 4096, 16)`, `BLOB_CONCAT(a, b)`} {
		b.Run(text, func(b *testing.B) {
			expr := mustParse("SELECT " + text).(*Select).Projs[0].Expr
			b.ReportAllocs()
			for b.Loop() {
				if _, err := evalRawExpr(plan, raw, expr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
