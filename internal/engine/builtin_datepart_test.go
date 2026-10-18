package engine

import (
	"reflect"
	"testing"
)

func TestDatePartSQL(t *testing.T) {
	for _, tc := range []struct {
		part string
		want int
	}{
		{"year", 2024}, {"yy", 2024}, {"q", 1}, {"mm", 2}, {"day", 29}, {"dy", 60},
		{"week", 9}, {"weekday", 5}, {"hour", 12}, {"mi", 34}, {"ss", 56},
		{"ms", 123}, {"mcs", 123456}, {"ns", 123456789}, {"tz", 345}, {"iso_week", 9},
	} {
		for _, part := range []string{tc.part, "'" + tc.part + "'"} {
			query := "SELECT DATEPART(" + part + ", '2024-02-29T12:34:56.123456789+05:45') AS v"
			if got := evalScalarSQL(t, query); got != tc.want {
				t.Errorf("%s = %v; want %d", query, got, tc.want)
			}
		}
	}
	for _, tc := range []struct {
		expression string
		want       int
	}{
		{`DATEPART(iso_year, '2021-01-01')`, 2020},
		{`DATEPART(iso_week, '2021-01-01')`, 53},
		{`DATEPART(week, '2021-01-01')`, 1},
		{`DATEPART(week, '2021-01-03')`, 2},
		{`DATEPART(weekday, '2024-03-03')`, 1},
	} {
		if got := evalScalarSQL(t, "SELECT "+tc.expression+" AS v"); got != tc.want {
			t.Errorf("%s = %v; want %d", tc.expression, got, tc.want)
		}
	}
}

func TestDatePartRawParity(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"d": 0, "p": 1}}
	raw := []any{"2021-01-01", "ISO_YEAR"}
	row := Row{"d": raw[0], "p": raw[1]}
	for _, expression := range []string{
		`DATEPART(iso_week, d)`, `DATEPART('ISO_YEAR', d)`, `DATEPART(COALESCE('ISO_YEAR', p), d)`,
		`DATEPART(NULL, 1/0)`, `DATEPART(year, NULL)`, `DATEPART(year, 'invalid')`,
		`DATEPART(123, 1/0)`, `DATEPART('invalid', d)`,
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		want, wantErr := evalExpr(ExecEnv{}, expr, row)
		got, gotErr := evalRawExpr(plan, raw, expr)
		if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
			t.Errorf("%s: raw=%v, %v; ordinary=%v, %v", expression, got, gotErr, want, wantErr)
		}
	}
}
