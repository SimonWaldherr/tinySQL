package engine

import "testing"

// TestCompareInt64 pins compare()'s int64 handling. Before compareInt64
// existed, a left-hand int64 fell through compare()'s outer type switch
// entirely (despite compareInt already handling a right-hand int64, and
// numeric() listing int64 as numeric) to the text-equality-only fallback:
// two unequal int64 values returned an error, and compareForOrder — used by
// every generic-path ORDER BY — silently swallows that error into "equal",
// producing a wrong sort order with no visible failure.
func TestCompareInt64(t *testing.T) {
	cases := []struct {
		name    string
		a, b    any
		want    int
		wantErr bool
	}{
		{"int64 less than int64", int64(3), int64(5), -1, false},
		{"int64 greater than int64", int64(5), int64(3), 1, false},
		{"int64 equal to int64", int64(5), int64(5), 0, false},
		{"int64 less than int", int64(3), 5, -1, false},
		{"int64 greater than int", int64(5), 3, 1, false},
		{"int less than int64", 3, int64(5), -1, false},
		{"int64 less than float64", int64(3), 5.5, -1, false},
		{"int64 equal float64", int64(5), 5.0, 0, false},
		{"int64 incomparable with string", int64(5), "abc", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := compare(tc.a, tc.b)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("compare(%#v, %#v) = %d, nil; want an error", tc.a, tc.b, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("compare(%#v, %#v) unexpected error: %v", tc.a, tc.b, err)
			}
			if got != tc.want {
				t.Fatalf("compare(%#v, %#v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestCompareForOrderInt64DoesNotSilentlyTie is the regression this bug
// actually causes in practice: compareForOrder is the comparator behind every
// generic-path ORDER BY, and it discards compare()'s error entirely (folding
// it into "equal", i.e. 0) rather than propagating it. Before compareInt64,
// two DIFFERENT int64 values compared this way silently reported as equal —
// a wrong, unsorted-looking result with no error surfaced anywhere.
func TestCompareForOrderInt64DoesNotSilentlyTie(t *testing.T) {
	if got := compareForOrder(int64(3), int64(5), false); got == 0 {
		t.Fatalf("compareForOrder(int64(3), int64(5), false) = 0; two distinct int64 values must not compare equal")
	}
	if got := compareForOrder(int64(5), int64(3), false); got <= 0 {
		t.Fatalf("compareForOrder(int64(5), int64(3), false) = %d; want > 0", got)
	}
	if got := compareForOrder(int64(3), int64(5), false); got >= 0 {
		t.Fatalf("compareForOrder(int64(3), int64(5), false) = %d; want < 0", got)
	}
}
