package engine

import (
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestConvertSQL(t *testing.T) {
	for _, tc := range []struct {
		expression string
		want       any
	}{
		{`CONVERT(INT, ' 42 ')`, 42},
		{`CONVERT(BIGINT, 9007199254740993)`, int64(9007199254740993)},
		{`CONVERT(INT, '-9223372036854775808')`, int64(math.MinInt64)},
		{`CONVERT(FLOAT, '1.25')`, 1.25},
		{`CONVERT(BOOL, 'true')`, true},
		{`CONVERT(BOOL, ' false ')`, false},
		{`TRY_CONVERT(BOOL, 'bad')`, nil},
		{`CONVERT(VARCHAR(3), 'äöüß')`, "äöü"},
		{`CONVERT(CHAR(4), 'ab')`, "ab  "},
		{`CONVERT(TEXT, 123)`, "123"},
		{`DATE(CONVERT(DATE, '29.02.2024', 104))`, "2024-02-29"},
		{`DATE(CONVERT(DATE, '29/02/2024', 103))`, "2024-02-29"},
		{`DATE(CONVERT(DATE, '20240229', 112))`, "2024-02-29"},
		{`CONVERT(TEXT, MAKE_DATE(2024, 2, 29), 104)`, "29.02.2024"},
		{`CONVERT(VARCHAR(8), MAKE_DATE(2024, 2, 29), 112)`, "20240229"},
		{`CONVERT(TIME, '12:34:56', 108)`, "12:34:56"},
		{`CONVERT(TIME, '2024-02-29T12:34:56.123456789+05:45')`, "12:34:56.123456789"},
		{`CONVERT(TEXT, CONVERT(DATETIME, '2024-02-29 12:34:56.123456', 121), 121)`, "2024-02-29 12:34:56.123"},
		{`CONVERT(TEXT, CONVERT(DATETIME2, '2024-02-29T12:34:56', 126), 126)`, "2024-02-29T12:34:56"},
		{`CONVERT(TEXT, CONVERT(DATETIME, '2024-02-29T12:34:56.123+05:45', 127), 127)`, "2024-02-29T12:34:56.123+05:45"},
		{`TRY_CONVERT(INT, 'bad')`, nil},
		{`TRY_CONVERT(INT, '9223372036854775808')`, nil},
		{`TRY_CONVERT(FLOAT, 'NaN')`, nil},
		{`TRY_CONVERT(DATE, '29.02.2023', 104)`, nil},
		{`TRY_CONVERT(INT, NULL)`, nil},
		{`CONVERT(DATE, 'invalid', NULL)`, nil},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			got := evalScalarSQL(t, "SELECT "+tc.expression+" AS v")
			if !rawEqual(got, tc.want) && !(got == nil && tc.want == nil) {
				t.Fatalf("got %#v (%T); want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
	date := evalScalarSQL(t, `SELECT CONVERT(DATE, '2024-02-29T12:34:56+05:45') AS v`).(time.Time)
	if date.Format(time.RFC3339) != "2024-02-29T00:00:00+05:45" {
		t.Fatalf("DATE conversion lost location: %v", date)
	}
}

func TestConvertErrorsAndRawParity(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0, "d": 1, "s": 2}}
	raw := []any{"bad", "29.02.2024", 104}
	row := Row{"v": raw[0], "d": raw[1], "s": raw[2]}
	for _, expression := range []string{
		`TRY_CONVERT(INT, v)`, `CONVERT(DATE, d, s)`, `TRY_CONVERT(INT, 1/0)`,
		`TRY_CONVERT(DATE, d, 1/0)`, `TRY_CONVERT(unknown, NULL)`,
		`CONVERT(INT, 'bad')`, `CONVERT(INT, 9223372036854775808.0)`,
		`CONVERT(INT, POWER(10,400))`, `CONVERT(FLOAT, 'Inf')`,
		`TRY_CONVERT(DATE, d, 130)`, `CONVERT(DATE, d, -1)`, `CONVERT(DATE, d, 104.5)`,
		`CONVERT(VARCHAR(8001), v)`, `TRY_CONVERT(INT(10), v)`,
	} {
		t.Run(expression, func(t *testing.T) {
			expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
			want, wantErr := evalExpr(ExecEnv{}, expr, row)
			got, gotErr := evalRawExpr(plan, raw, expr)
			if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Fatalf("raw=%v, %v; ordinary=%v, %v", got, gotErr, want, wantErr)
			}
			if expression != `TRY_CONVERT(INT, v)` && expression != `CONVERT(DATE, d, s)` && wantErr == nil {
				t.Fatal("expected an execution error")
			}
		})
	}
	for _, query := range []string{`SELECT CONVERT()`, `SELECT CONVERT(INT)`, `SELECT CONVERT(INT, 1, 2, 3)`, `SELECT CONVERT(VARCHAR(-1), 'a')`} {
		if _, err := NewParser(query).ParseStatement(); err == nil {
			t.Errorf("%s: expected syntax error", query)
		}
	}
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE dirty_dates (d TEXT, n TEXT)`)
	execSQL(t, db, `INSERT INTO dirty_dates VALUES ('29.02.2024', '12'), ('31.02.2024', 'bad'), (NULL, NULL)`)
	result := execSQL(t, db, `SELECT CONVERT(DATE, d, 104) AS d, TRY_CONVERT(INT, n) AS n FROM dirty_dates WHERE TRY_CONVERT(DATE, d, 104) IS NOT NULL`)
	if len(result.Rows) != 1 || result.Rows[0]["n"] != 12 {
		t.Fatalf("conversion filter: %#v", result.Rows)
	}
}

func TestConversionIntegerCompatibility(t *testing.T) {
	for _, text := range []string{"", "+", "-", "0", "-0", "+42", "001", " 1", "1.0", "1_000", "12x", "１２", "9223372036854775807", "9223372036854775808", "-9223372036854775808", "-9223372036854775809"} {
		got, ok := parseConversionInteger(text)
		want, err := strconv.ParseInt(text, 10, 64)
		if ok != (err == nil) || ok && got != want {
			t.Errorf("%q = %d, %v; strconv=%d, %v", text, got, ok, want, err)
		}
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0x1p63, math.Nextafter(-0x1p63, math.Inf(-1))} {
		if _, ok := conversionIntegerValue(value); ok {
			t.Errorf("accepted out-of-range float %v", value)
		}
	}
}

func FuzzConversionInteger(f *testing.F) {
	for _, input := range []string{"42", "-9223372036854775808", "9223372036854775808", "bad", "+000001"} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, text string) {
		got, ok := parseConversionInteger(text)
		want, err := strconv.ParseInt(text, 10, 64)
		if ok != (err == nil) || ok && got != want {
			t.Fatalf("%q = %d, %v; strconv=%d, %v", text, got, ok, want, err)
		}
	})
}

func BenchmarkTryConvertInteger(b *testing.B) {
	for _, input := range []string{"42", "not a number", "9223372036854775808"} {
		b.Run(input, func(b *testing.B) {
			plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
			raw := []any{input}
			expr := mustParse("SELECT TRY_CONVERT(INT, v)").(*Select).Projs[0].Expr
			b.ReportAllocs()
			for b.Loop() {
				if _, err := evalRawExpr(plan, raw, expr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
