package engine

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestTimestampNumericSeconds(t *testing.T) {
	cases := []struct {
		name  string
		input any
		want  int64
	}{
		{"native int", int(1700000000), 1700000000},
		{"exact large integer", int64(9007199254740993), 9007199254740993},
		{"exact negative integer", int64(-9007199254740993), -9007199254740993},
		{"largest integer", int64(math.MaxInt64), math.MaxInt64},
		{"smallest integer", int64(math.MinInt64), math.MinInt64},
		{"largest float below limit", math.Nextafter(0x1p63, 0), 9223372036854774784},
		{"smallest integral float", -0x1p63, math.MinInt64},
		{"positive fraction", 1.75, 1},
		{"negative fraction", -1.75, -1},
		{"numeric string", " 1.75 ", 1},
		{"exponent string", "1e3", 1000},
		{"date string", "1970-01-02T00:00:00Z", 86400},
		{"time value", time.Unix(86400, 123), 86400},
		{"boolean", true, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := &FuncCall{Name: "TIMESTAMP", Args: []Expr{&Literal{Val: tc.input}}}
			got, err := evalTimestampFunc(ExecEnv{}, ex, nil)
			if err != nil || got != tc.want {
				t.Fatalf("TIMESTAMP(%v) = %v, %v; want %d", tc.input, got, err, tc.want)
			}
		})
	}
}

func TestFromTimestampSeconds(t *testing.T) {
	maxTimeSeconds := int64(math.MaxInt64) + (time.Time{}).Unix()
	cases := []struct {
		name  string
		input any
		sec   int64
		nsec  int
	}{
		{"native int", int(1700000000), 1700000000, 0},
		{"exact large integer", int64(9007199254740993), 9007199254740993, 0},
		{"exact negative integer", int64(-9007199254740993), -9007199254740993, 0},
		{"largest time seconds", maxTimeSeconds, maxTimeSeconds, 0},
		{"large representable float", math.Nextafter(float64(maxTimeSeconds), 0), int64(math.Nextafter(float64(maxTimeSeconds), 0)), 0},
		{"before year one", int64(-62135596801), -62135596801, 0},
		{"positive fraction", 1.75, 1, 750000000},
		{"negative fraction", -1.75, -2, 250000000},
		{"positive rounding carry", 0.9999999996, 1, 0},
		{"negative rounding carry", -0.9999999996, -1, 0},
		{"negative nanosecond", -0.0000000006, -1, 999999999},
		{"numeric string", " 1.75 ", 1, 750000000},
		{"exponent string", "1e3", 1000, 0},
		{"boolean", true, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := &FuncCall{Name: "FROM_TIMESTAMP", Args: []Expr{&Literal{Val: tc.input}}}
			value, err := evalFromTimestampFunc(ExecEnv{}, ex, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := value.(time.Time)
			if !ok || got.Unix() != tc.sec || got.Nanosecond() != tc.nsec {
				t.Fatalf("FROM_TIMESTAMP(%v) = %v; want seconds=%d nanoseconds=%d", tc.input, value, tc.sec, tc.nsec)
			}
			if tc.sec > 0 && got.Before(time.Unix(0, 0)) {
				t.Fatalf("positive timestamp wrapped before the epoch: %v", got)
			}
		})
	}
}

func TestTimestampConversionRejectsOutOfRange(t *testing.T) {
	invalid := []any{
		math.NaN(), math.Inf(1), math.Inf(-1), 0x1p63, math.Nextafter(-0x1p63, math.Inf(-1)),
		"NaN", "+Inf", "-Inf", "9223372036854775808", "-9223372036854777856", "1e300",
	}
	for _, name := range []string{"TIMESTAMP", "FROM_TIMESTAMP"} {
		handler := evalTimestampFunc
		if name == "FROM_TIMESTAMP" {
			handler = evalFromTimestampFunc
		}
		for _, input := range invalid {
			ex := &FuncCall{Name: name, Args: []Expr{&Literal{Val: input}}}
			if _, err := handler(ExecEnv{}, ex, nil); err == nil || !strings.Contains(err.Error(), "out of range") {
				t.Errorf("%s(%v) error = %v; want out of range", name, input, err)
			}
		}
		ex := &FuncCall{Name: name, Args: []Expr{&Literal{Val: nil}}}
		if got, err := handler(ExecEnv{}, ex, nil); got != nil || err != nil {
			t.Errorf("%s(NULL) = %v, %v", name, got, err)
		}
	}
	maxTimeSeconds := int64(math.MaxInt64) + (time.Time{}).Unix()
	for _, input := range []any{maxTimeSeconds + 1, int64(math.MaxInt64), int64(math.MinInt64), float64(math.MinInt64)} {
		ex := &FuncCall{Name: "FROM_TIMESTAMP", Args: []Expr{&Literal{Val: input}}}
		if _, err := evalFromTimestampFunc(ExecEnv{}, ex, nil); err == nil {
			t.Errorf("FROM_TIMESTAMP(%v) accepted seconds outside time.Time's range", input)
		}
	}
	ex := &FuncCall{Name: "FROM_TIMESTAMP", Args: []Expr{&Literal{Val: float64(maxTimeSeconds + 1024)}}}
	if _, err := evalFromTimestampFunc(ExecEnv{}, ex, nil); err == nil {
		t.Error("FROM_TIMESTAMP accepted floating-point seconds outside time.Time's range")
	}
}

func TestTimestampConversionSQLPreservesInteger(t *testing.T) {
	for _, sql := range []string{
		"SELECT TIMESTAMP(9007199254740993) AS v",
		"SELECT TIMESTAMP(FROM_TIMESTAMP(9007199254740993)) AS v",
	} {
		if got := evalScalarSQL(t, sql); got != int64(9007199254740993) {
			t.Fatalf("%s = %v; want exact integer seconds", sql, got)
		}
	}
}

func BenchmarkTimestampNumericConversion(b *testing.B) {
	for _, name := range []string{"TIMESTAMP", "FROM_TIMESTAMP"} {
		handler := evalTimestampFunc
		if name == "FROM_TIMESTAMP" {
			handler = evalFromTimestampFunc
		}
		for _, input := range []struct {
			name  string
			value any
		}{
			{"integer", int64(1700000000)},
			{"fraction", 1700000000.125},
			{"numeric string", "1700000000.125"},
		} {
			b.Run(name+"/"+input.name, func(b *testing.B) {
				ex := &FuncCall{Name: name, Args: []Expr{&Literal{Val: input.value}}}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := handler(ExecEnv{}, ex, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
