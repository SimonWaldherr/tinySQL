package engine

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDateDiffSignedNanosecondLimits(t *testing.T) {
	a := time.Unix(0, 0).UTC()
	for _, n := range []int64{math.MaxInt64, math.MinInt64, math.MaxInt64 - 100, math.MinInt64 + 100} {
		b := a.Add(time.Duration(n))
		for _, diff := range []func(string, time.Time, time.Time) (int64, error){boundaryDateDiff, elapsedDateDiff} {
			got, err := diff("NANOSECOND", a, b)
			if err != nil || got != n {
				t.Fatalf("nanosecond difference %d: %d %v", n, got, err)
			}
		}
	}
}

func TestDateDiffBoundariesAndElapsedUnits(t *testing.T) {
	for _, tc := range []struct {
		unit, start, end    string
		boundaries, elapsed int
	}{
		{"month", "2024-01-31", "2024-02-29", 1, 0},
		{"months", "2024-01-29", "2024-02-29", 1, 1},
		{"year", "2024-02-29", "2025-02-28", 1, 0},
		{"year", "2024-02-29", "2025-03-01", 1, 1},
		{"quarter", "2024-03-31", "2024-04-01", 1, 0},
		{"day", "2024-02-28T23:59:59.999999999Z", "2024-02-29T00:00:00Z", 1, 0},
		{"hour", "2024-02-29T12:59:59Z", "2024-02-29T13:00:00Z", 1, 0},
		{"minute", "2024-02-29T12:34:59Z", "2024-02-29T12:35:00Z", 1, 0},
		{"second", "2024-02-29T12:34:59.999Z", "2024-02-29T12:35:00Z", 1, 0},
		{"ms", "2024-02-29T12:34:59.000999Z", "2024-02-29T12:34:59.001Z", 1, 0},
		{"mcs", "2024-02-29T12:34:59.000000999Z", "2024-02-29T12:34:59.000001Z", 1, 0},
		{"ns", "2024-02-29T12:34:59.999999999Z", "2024-02-29T12:35:00Z", 1, 1},
		{"week", "2024-03-02", "2024-03-03", 1, 0},
		{"iso_week", "2024-03-03", "2024-03-04", 1, 0},
		{"week", "1969-12-27", "1969-12-28", 1, 0},
		{"day", "2024-03-30T12:00:00+01:00", "2024-03-31T12:00:00+02:00", 1, 0},
		{"hour", "2024-03-30T12:00:00+01:00", "2024-03-31T12:00:00+02:00", 23, 23},
		{"hour", "2024-10-26T12:00:00+02:00", "2024-10-27T12:00:00+01:00", 25, 25},
		{"day", "2024-03-01T00:30:00+01:00", "2024-02-29T23:30:00Z", 0, 0},
		// A duration exceeding time.Duration's approximately 292-year range.
		{"day", "1000-01-01", "2000-01-01", 365242, 365242},
	} {
		for _, reverse := range []bool{false, true} {
			start, end, sign := tc.start, tc.end, 1
			if reverse {
				start, end, sign = end, start, -1
			}
			for _, fn := range []struct {
				name string
				want int
			}{
				{"DATEDIFF", tc.boundaries}, {"TIMESTAMPDIFF", tc.elapsed},
			} {
				sql := fmt.Sprintf("SELECT %s(%s, '%s', '%s') AS v", fn.name, tc.unit, start, end)
				if got := evalScalarSQL(t, sql); got != sign*fn.want {
					t.Errorf("%s = %v, want %d", sql, got, sign*fn.want)
				}
			}
		}
	}
}

func TestDateDiffRawParity(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"a": 0, "b": 1, "u": 2}}
	raw := []any{"2024-01-31", "2024-02-29", "MONTH"}
	row := Row{"a": raw[0], "b": raw[1], "u": raw[2]}
	for _, name := range []string{"DATEDIFF", "TIMESTAMPDIFF"} {
		for _, args := range []string{
			"'MONTH', a, b", "COALESCE(NULL, u), a, b", "'DAY', NULL, 1/0",
			"NULL, 1/0, 1/0", "1, 1/0, 1/0", "'DAY', 'invalid', NULL",
			"'DAY', a, 'invalid'", "'invalid', a, b", "1/0",
			"'ns', '1000-01-01', '2000-01-01'",
		} {
			expr := mustParse("SELECT " + name + "(" + args + ")").(*Select).Projs[0].Expr
			want, wantErr := evalExpr(ExecEnv{}, expr, row)
			got, gotErr := evalRawExpr(plan, raw, expr)
			if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Errorf("%s(%s): raw=%v/%v, ordinary=%v/%v", name, args, got, gotErr, want, wantErr)
			}
		}
	}
}

func TestDateDiffOverflow(t *testing.T) {
	for _, name := range []string{"DATEDIFF", "TIMESTAMPDIFF"} {
		call := &FuncCall{Name: name, Args: []Expr{&Literal{Val: "ns"}, &Literal{Val: "1000-01-01"}, &Literal{Val: "2000-01-01"}}}
		if _, err := evalDateDiff(ExecEnv{}, call, nil); err == nil || !strings.Contains(err.Error(), "out of integer range") {
			t.Fatalf("%s overflow = %v", name, err)
		}
	}
}

func TestISOWeekdayLocalDate(t *testing.T) {
	for _, input := range []string{"2024-03-01T00:30:00+14:00", "2024-03-01T23:30:00-12:00"} {
		for _, expression := range []string{"ISODOW('%s')", "DATEPART(iso_weekday, '%s')", "DATEPART(isodow, '%s')", "EXTRACT('ISODOW', '%s')"} {
			sql := "SELECT " + fmt.Sprintf(expression, input) + " AS v"
			if got := evalScalarSQL(t, sql); got != 5 {
				t.Errorf("%s = %v, want 5", sql, got)
			}
		}
	}
	for weekday := 1; weekday <= 7; weekday++ {
		input := time.Date(2024, 2, 26+weekday-1, 12, 0, 0, 0, time.UTC).Format("2006-01-02")
		if got := evalScalarSQL(t, "SELECT ISODOW('"+input+"') AS v"); got != weekday {
			t.Errorf("%s: %v, want %d", input, got, weekday)
		}
	}
}
