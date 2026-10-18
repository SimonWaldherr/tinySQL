package engine

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCalendarFunctionsSQL(t *testing.T) {
	for _, tc := range []struct {
		expression string
		want       any
	}{
		{`DATE(MAKE_DATE(2024, 2, 29))`, "2024-02-29"},
		{`DATE(DATEFROMPARTS(2000, 2, 29))`, "2000-02-29"},
		{`DATE(MAKE_DATE(1, 1, 1))`, "0001-01-01"},
		{`DATE(MAKE_DATE(9999, 12, 31))`, "9999-12-31"},
		{`DATE(MAKE_DATE(2024.0, 2.0, 29.0))`, "2024-02-29"},
		{`DATE(LAST_DAY('2024-02-01'))`, "2024-02-29"},
		{`DATE(LAST_DAY('1900-02-28'))`, "1900-02-28"},
		{`DATE(LAST_DAY('2024-12-31'))`, "2024-12-31"},
		{`MONTHNAME('2024-02-29')`, "February"},
		{`DAYNAME('2024-02-29')`, "Thursday"},
		{`DAYNAME('2024-03-03')`, "Sunday"},
		{`DAYNAME('2024-03-04T00:15:00+14:00')`, "Monday"},
		{`MONTHNAME('2024-03-01T00:15:00+14:00')`, "March"},
		{`MAKE_DATE(NULL, 1/0, 1/0)`, nil},
		{`MAKE_DATE(2024, NULL, 1/0)`, nil},
		{`DATEFROMPARTS(2024, 2, NULL)`, nil},
		{`LAST_DAY(NULL)`, nil}, {`DAYNAME(NULL)`, nil}, {`MONTHNAME(NULL)`, nil},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			if got := evalScalarSQL(t, "SELECT "+tc.expression+" AS v"); got != tc.want {
				t.Fatalf("got %#v; want %#v", got, tc.want)
			}
		})
	}
	got := evalScalarSQL(t, `SELECT LAST_DAY('2024-02-01T12:34:56+05:45') AS v`).(time.Time)
	if got.Format(time.RFC3339) != "2024-02-29T00:00:00+05:45" {
		t.Fatalf("month end lost location or retained clock fields: %v", got)
	}
}

func TestCalendarFunctionsRawParity(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"d": 0, "y": 1, "m": 2, "n": 3}}
	raw := []any{"2024-02-15", int64(2024), 2, nil}
	row := Row{"d": raw[0], "y": raw[1], "m": raw[2], "n": nil}
	for _, expression := range []string{
		`MAKE_DATE(y, m, 29)`, `DATEFROMPARTS(y, m, 29)`,
		`LAST_DAY(d)`, `DAYNAME(d)`, `MONTHNAME(d)`,
		`MAKE_DATE(n, 1/0, 1/0)`, `MAKE_DATE(y, n, 1/0)`,
		`MAKE_DATE(0, 1/0, 1/0)`, `MAKE_DATE(y, 13, 1/0)`,
		`MAKE_DATE(1/0)`, `LAST_DAY(1/0, 1/0)`, `DAYNAME()`,
		`LAST_DAY(n)`, `DAYNAME(n)`, `MONTHNAME(n)`, `MONTHNAME('invalid')`,
	} {
		t.Run(expression, func(t *testing.T) {
			expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
			want, wantErr := evalExpr(ExecEnv{}, expr, row)
			got, gotErr := evalRawExpr(plan, raw, expr)
			if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Fatalf("raw=%v, %v; ordinary=%v, %v", got, gotErr, want, wantErr)
			}
		})
	}
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE calendar_dates (d TEXT, y INT, m INT)`)
	execSQL(t, db, `INSERT INTO calendar_dates VALUES ('2024-02-15', 2024, 2), ('2024-02-29', 2024, 2), (NULL, NULL, NULL)`)
	result := execSQL(t, db, `SELECT MONTHNAME(d) AS month, COUNT(*) AS n FROM calendar_dates WHERE LAST_DAY(d) = MAKE_DATE(y, m, 29) GROUP BY MONTHNAME(d)`)
	if len(result.Rows) != 1 || result.Rows[0]["month"] != "February" || result.Rows[0]["n"] != 2 {
		t.Fatalf("calendar filter/grouping: %#v", result.Rows)
	}
}

func TestCalendarFunctionsRejectInvalidInput(t *testing.T) {
	db := storage.NewDB()
	for _, expression := range []string{
		`MAKE_DATE(2023, 2, 29)`, `MAKE_DATE(1900, 2, 29)`, `MAKE_DATE(2024, 4, 31)`,
		`MAKE_DATE(0, 1, 1)`, `MAKE_DATE(10000, 1, 1)`, `MAKE_DATE(-1, 1, 1)`,
		`MAKE_DATE(2024, 0, 1)`, `MAKE_DATE(2024, 13, 1)`, `MAKE_DATE(2024, 1, 0)`,
		`MAKE_DATE(2024, 1, 32)`, `MAKE_DATE(2024.5, 1, 1)`, `MAKE_DATE('2024', 1, 1)`,
		`MAKE_DATE(2024, 1, 1.5)`, `DATEFROMPARTS(2023, 2, 29)`,
		`LAST_DAY('invalid')`, `DAYNAME('2024-02-30')`, `MONTHNAME('invalid')`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse("SELECT "+expression)); err == nil {
			t.Errorf("%s: expected error", expression)
		}
	}
	for _, invalid := range []any{math.NaN(), math.Inf(1), math.Inf(-1), int64(math.MaxInt64), true} {
		ex := &FuncCall{Name: "MAKE_DATE", Args: []Expr{&Literal{Val: invalid}, &Literal{Val: 1}, &Literal{Val: 1}}}
		if _, err := evalCalendarFunc(ExecEnv{}, ex, nil); err == nil {
			t.Errorf("MAKE_DATE(%v, 1, 1): expected error", invalid)
		}
	}
}
