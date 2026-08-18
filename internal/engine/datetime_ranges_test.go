// Tests for the date/time additions inspired by (not copied from) two Go
// libraries the user pointed at: github.com/senseyeio/duration (ISO 8601
// duration parsing, DST-safe calendar shifts) and github.com/senseyeio/
// spaniel (interval union via sort-then-sweep). See builtin_datetime.go for
// the implementations: applyDateUnitShift's new WEEK case, ISO 8601 duration
// parsing for DATE_ADD/DATE_SUB's 2-argument form, OVERLAPS, and RANGE_MERGE.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ─────────────────────────── WEEK unit ─────────────────────────────────────

func TestApplyDateUnitShiftWeek(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	got, err := applyDateUnitShift(base, 2, "WEEK")
	if err != nil {
		t.Fatalf("applyDateUnitShift WEEK error: %v", err)
	}
	want := base.AddDate(0, 0, 14)
	if !got.Equal(want) {
		t.Fatalf("2 WEEK shift = %v, want %v", got, want)
	}
	got, err = applyDateUnitShift(base, 3, "WEEKS")
	if err != nil {
		t.Fatalf("applyDateUnitShift WEEKS error: %v", err)
	}
	if want := base.AddDate(0, 0, 21); !got.Equal(want) {
		t.Fatalf("3 WEEKS shift = %v, want %v", got, want)
	}
}

func TestDateAddDateSubWeekSQL(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT DATE_ADD('2024-01-01', 2, 'WEEK') AS d`)
	got, ok := rs.Rows[0]["d"].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T (%v)", rs.Rows[0]["d"], rs.Rows[0]["d"])
	}
	want, _ := time.Parse("2006-01-02", "2024-01-15")
	if !got.Equal(want) {
		t.Fatalf("DATE_ADD(..., 2, 'WEEK') = %v, want %v", got, want)
	}

	rs = execSQL(t, db, `SELECT DATE_SUB('2024-01-15', 2, 'WEEKS') AS d`)
	got, ok = rs.Rows[0]["d"].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", rs.Rows[0]["d"])
	}
	want, _ = time.Parse("2006-01-02", "2024-01-01")
	if !got.Equal(want) {
		t.Fatalf("DATE_SUB(..., 2, 'WEEKS') = %v, want %v", got, want)
	}
}

// ─────────────────────────── ISO 8601 duration parsing ────────────────────

func TestParseISO8601Duration(t *testing.T) {
	cases := []struct {
		in   string
		want isoDuration
	}{
		{"P1Y", isoDuration{years: 1}},
		{"P1Y2M3D", isoDuration{years: 1, months: 2, days: 3}},
		{"P2W", isoDuration{weeks: 2}},
		{"PT1H30M", isoDuration{hours: 1, minutes: 30}},
		{"PT45S", isoDuration{seconds: 45}},
		{"P1DT12H", isoDuration{days: 1, hours: 12}},
		{"P1Y2M3W4DT5H6M7S", isoDuration{years: 1, months: 2, weeks: 3, days: 4, hours: 5, minutes: 6, seconds: 7}},
		{"-P1D", isoDuration{days: -1}},
		{"-P1Y2M", isoDuration{years: -1, months: -2}},
		{"-PT1H", isoDuration{hours: -1}},
		{"P0D", isoDuration{days: 0}}, // explicit zero component is valid, unlike bare "P"
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseISO8601Duration(tc.in)
			if err != nil {
				t.Fatalf("parseISO8601Duration(%q) error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("parseISO8601Duration(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

// TestParseISO8601DurationRejectsMalformed pins the cases that must error:
// a bare "P"/"PT" (every component group is individually optional, so the
// regex alone would otherwise accept these as an all-zero duration), and
// strings that are not ISO 8601 durations at all.
func TestParseISO8601DurationRejectsMalformed(t *testing.T) {
	for _, in := range []string{"P", "PT", "", "1D", "P1", "PXD", "P1Y2", "garbage", "P1.5D", "P1DT"} {
		t.Run(in, func(t *testing.T) {
			if _, err := parseISO8601Duration(in); err == nil {
				t.Fatalf("parseISO8601Duration(%q) should have errored", in)
			}
		})
	}
}

// TestParseISO8601DurationRejectsOverflow pins a bug an adversarial review
// pass caught in this same change: isoDurationRE's digit groups have no
// length cap, so a component with enough digits to exceed int range is
// syntactically valid input, and strconv.Atoi correctly reports that as a
// range error -- which the original code discarded on the mistaken belief a
// regex-captured digit run "cannot fail" to parse. The silently-clamped
// component (years = math.MaxInt64) then overflowed time.Time.AddDate's
// internal arithmetic and produced a date *before* the input, with no error
// anywhere.
func TestParseISO8601DurationRejectsOverflow(t *testing.T) {
	if _, err := parseISO8601Duration("P99999999999999999999Y"); err == nil {
		t.Fatalf("parseISO8601Duration with a component too large for int should error")
	}

	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`SELECT DATE_ADD('2024-01-15', 'P99999999999999999999Y') AS d`)); err == nil {
		t.Fatalf("DATE_ADD with an overflowing ISO 8601 year component should error, not silently return a garbage date")
	}
}

// TestApplyDateUnitShiftFractionalWeek pins the other bug the same review
// pass caught: the WEEK case used to truncate the *week count* to an
// integer before scaling by 7 (int(interval)*7), instead of scaling to days
// first and truncating once like every other unit here does. 1.9 WEEK
// became int(1.9)*7 = 7 days instead of the 13 days the equivalent
// DATE_ADD(d, 13.3, 'DAY') gives -- up to nearly 7 days silently discarded.
func TestApplyDateUnitShiftFractionalWeek(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	gotWeek, err := applyDateUnitShift(base, 1.9, "WEEK")
	if err != nil {
		t.Fatalf("applyDateUnitShift error: %v", err)
	}
	gotDay, err := applyDateUnitShift(base, 1.9*7, "DAY")
	if err != nil {
		t.Fatalf("applyDateUnitShift error: %v", err)
	}
	if !gotWeek.Equal(gotDay) {
		t.Fatalf("1.9 WEEK = %v, want the same as 1.9*7 DAY = %v (a fractional week must scale to days before truncating, not after)", gotWeek, gotDay)
	}
	wantDays := base.AddDate(0, 0, 13) // int(1.9*7) == int(13.3) == 13
	if !gotWeek.Equal(wantDays) {
		t.Fatalf("1.9 WEEK = %v, want %v (13 days)", gotWeek, wantDays)
	}
}

func TestISODurationShiftBasic(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	d, err := parseISO8601Duration("P1Y2M3DT4H5M6S")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	got := d.shift(base, false)
	want := base.AddDate(1, 2, 3).Add(4*time.Hour + 5*time.Minute + 6*time.Second)
	if !got.Equal(want) {
		t.Fatalf("shift = %v, want %v", got, want)
	}

	// negate=true (DATE_SUB's 2-arg form) must invert every component.
	got = d.shift(base, true)
	want = base.AddDate(-1, -2, -3).Add(-(4*time.Hour + 5*time.Minute + 6*time.Second))
	if !got.Equal(want) {
		t.Fatalf("negated shift = %v, want %v", got, want)
	}
}

// TestISODurationShiftIsDSTSafe is the whole point of separating calendar
// components (AddDate) from clock components (Add) instead of converting
// everything to a fixed time.Duration: shifting by "P1D" across a spring-
// forward DST transition must land on the same wall-clock hour the next day,
// not lose an hour the way adding a fixed 24*time.Hour would.
func TestISODurationShiftIsDSTSafe(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata not available on this system: %v", err)
	}
	// 2024-03-09 is the day before the US spring-forward transition
	// (2024-03-10 02:00 -> 03:00 EDT).
	base := time.Date(2024, 3, 9, 9, 0, 0, 0, loc)

	d, err := parseISO8601Duration("P1D")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	shifted := d.shift(base, false)
	if shifted.Hour() != 9 {
		t.Fatalf("P1D shift across DST: got wall-clock hour %d, want 9 (AddDate must preserve local time-of-day)", shifted.Hour())
	}
	if shifted.Day() != 10 {
		t.Fatalf("P1D shift: got day %d, want 10", shifted.Day())
	}

	// Contrast: a naive fixed-duration add of 24h DOES skew the wall clock
	// across this exact transition, which is precisely the bug DATE_ADD/
	// DATE_SUB's existing per-unit split (and this duration shift) exists to
	// avoid.
	naive := base.Add(24 * time.Hour)
	if naive.Hour() == 9 {
		t.Fatalf("test fixture invalid: expected the naive 24h add to demonstrate DST skew on this date/location, but it landed on hour 9 too")
	}
}

func TestDateAddSubISO8601FormSQL(t *testing.T) {
	db := storage.NewDB()

	rs := execSQL(t, db, `SELECT DATE_ADD('2024-01-15', 'P1M') AS d`)
	got, ok := rs.Rows[0]["d"].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", rs.Rows[0]["d"])
	}
	want, _ := time.Parse("2006-01-02", "2024-02-15")
	if !got.Equal(want) {
		t.Fatalf("DATE_ADD(date, 'P1M') = %v, want %v", got, want)
	}

	rs = execSQL(t, db, `SELECT DATE_SUB('2024-02-15', 'P1M') AS d`)
	got, ok = rs.Rows[0]["d"].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", rs.Rows[0]["d"])
	}
	want, _ = time.Parse("2006-01-02", "2024-01-15")
	if !got.Equal(want) {
		t.Fatalf("DATE_SUB(date, 'P1M') = %v, want %v", got, want)
	}

	// A non-string second argument must be rejected, not silently miscoerced.
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(`SELECT DATE_ADD('2024-01-15', 5) AS d`)); err == nil {
		t.Fatalf("DATE_ADD with a non-string 2-argument form should error")
	}

	// A malformed duration string must be rejected with a clear error.
	if _, err := Execute(ctx, db, "default", mustParse(`SELECT DATE_ADD('2024-01-15', 'not-a-duration') AS d`)); err == nil {
		t.Fatalf("DATE_ADD with an invalid ISO 8601 duration should error")
	}
}

// ─────────────────────────── OVERLAPS ──────────────────────────────────────

func TestEvalOverlaps(t *testing.T) {
	env := ExecEnv{}
	row := Row{}
	lit := func(s string) Expr { return &Literal{Val: s} }

	cases := []struct {
		name           string
		s1, e1, s2, e2 string
		want           bool
	}{
		{"clearly overlapping", "2024-01-01", "2024-01-10", "2024-01-05", "2024-01-15", true},
		{"identical ranges", "2024-01-01", "2024-01-10", "2024-01-01", "2024-01-10", true},
		{"one contains the other", "2024-01-01", "2024-01-31", "2024-01-10", "2024-01-15", true},
		{"touching endpoints do not overlap", "2024-01-01", "2024-01-10", "2024-01-10", "2024-01-20", false},
		{"completely disjoint", "2024-01-01", "2024-01-05", "2024-02-01", "2024-02-05", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := evalOverlaps(env, []Expr{lit(tc.s1), lit(tc.e1), lit(tc.s2), lit(tc.e2)}, row)
			if err != nil {
				t.Fatalf("evalOverlaps error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("OVERLAPS(%s,%s,%s,%s) = %v, want %v", tc.s1, tc.e1, tc.s2, tc.e2, got, tc.want)
			}
		})
	}
}

func TestEvalOverlapsNullPropagation(t *testing.T) {
	env := ExecEnv{}
	row := Row{}
	v, err := evalOverlaps(env, []Expr{
		&Literal{Val: nil},
		&Literal{Val: "2024-01-10"},
		&Literal{Val: "2024-01-05"},
		&Literal{Val: "2024-01-15"},
	}, row)
	if err != nil {
		t.Fatalf("evalOverlaps with NULL arg should not error, got: %v", err)
	}
	if v != nil {
		t.Fatalf("evalOverlaps with a NULL argument should return NULL, got %v", v)
	}
}

func TestEvalOverlapsRejectsReversedRange(t *testing.T) {
	env := ExecEnv{}
	row := Row{}
	_, err := evalOverlaps(env, []Expr{
		&Literal{Val: "2024-01-10"}, // start1 after end1
		&Literal{Val: "2024-01-01"},
		&Literal{Val: "2024-01-05"},
		&Literal{Val: "2024-01-15"},
	}, row)
	if err == nil {
		t.Fatalf("evalOverlaps should reject a range whose end precedes its start")
	}
}

// TestEvalOverlapsInstantRange pins the degenerate (start==end) case, which
// the naive positive-width overlap test cannot handle correctly on its own
// (see evalOverlaps' comment): a first, broken version of this function
// treated an instant strictly interior to the other range as never
// overlapping, which was wrong -- it disagreed with that range's own
// half-open convention at the start boundary. These cases were what caught
// it: containment must match "start2 <= instant < end2" exactly.
func TestEvalOverlapsInstantRange(t *testing.T) {
	rangeStart, rangeEnd := "2024-01-01", "2024-01-10"
	cases := []struct {
		name    string
		instant string
		want    bool
	}{
		{"instant strictly inside the range", "2024-01-05", true},
		{"instant exactly at range start (inclusive)", rangeStart, true},
		{"instant exactly at range end (exclusive)", rangeEnd, false},
		{"instant before the range", "2023-12-01", false},
		{"instant after the range", "2024-02-01", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := ExecEnv{}
			row := Row{}
			got, err := evalOverlaps(env, []Expr{
				&Literal{Val: tc.instant},
				&Literal{Val: tc.instant},
				&Literal{Val: rangeStart},
				&Literal{Val: rangeEnd},
			}, row)
			if err != nil {
				t.Fatalf("evalOverlaps with an instant range should not error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("instant %s vs [%s,%s): got %v, want %v", tc.instant, rangeStart, rangeEnd, got, tc.want)
			}
		})
	}
}

// TestEvalOverlapsTwoInstants pins that two degenerate ranges never overlap,
// even when they represent the same point in time: each is defined as the
// empty set [x,x), and the intersection of two empty sets is empty
// regardless of where they sit.
func TestEvalOverlapsTwoInstants(t *testing.T) {
	env := ExecEnv{}
	row := Row{}
	got, err := evalOverlaps(env, []Expr{
		&Literal{Val: "2024-01-05"},
		&Literal{Val: "2024-01-05"},
		&Literal{Val: "2024-01-05"},
		&Literal{Val: "2024-01-05"},
	}, row)
	if err != nil {
		t.Fatalf("evalOverlaps with two identical instants should not error: %v", err)
	}
	if got != false {
		t.Fatalf("two instants, even identical ones, should never overlap, got %v", got)
	}
}

// ─────────────────────────── RANGE_MERGE ───────────────────────────────────
//
// Exercised only via direct evalRangeMerge calls, not through SQL: this
// engine has no array-literal syntax and JSON_GET does not parse a JSON
// *string* (it expects an already-decoded map[string]any/[]any), so there is
// no SQL-level way to construct a []any argument today. evalArraySort et al.
// (extended_functions.go) have the same testing gap; this file's direct
// calls match that existing precedent (funcs_test.go's JSON_GET/JSON_SET
// tests use the same style).

func rangePair(start, end string) []any {
	return []any{start, end}
}

func TestEvalRangeMergeOverlapping(t *testing.T) {
	env := ExecEnv{}
	row := Row{}
	arr := []any{
		rangePair("2024-01-01", "2024-01-10"),
		rangePair("2024-01-05", "2024-01-15"), // overlaps the first
		rangePair("2024-02-01", "2024-02-05"), // disjoint
	}
	got, err := evalRangeMerge(env, []Expr{&Literal{Val: arr}}, row)
	if err != nil {
		t.Fatalf("evalRangeMerge error: %v", err)
	}
	merged, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged ranges, got %d: %+v", len(merged), merged)
	}
	first := merged[0].([]any)
	wantStart, _ := time.Parse("2006-01-02", "2024-01-01")
	wantEnd, _ := time.Parse("2006-01-02", "2024-01-15")
	if !first[0].(time.Time).Equal(wantStart) || !first[1].(time.Time).Equal(wantEnd) {
		t.Fatalf("first merged range = %v, want [%v, %v]", first, wantStart, wantEnd)
	}
}

func TestEvalRangeMergeTouchingRangesCoalesce(t *testing.T) {
	// [1,2) and [2,3) touch at an endpoint: under this engine's half-open
	// convention (matching evalOverlaps and documented on evalRangeMerge)
	// they merge into one [1,3) range instead of staying separate.
	env := ExecEnv{}
	row := Row{}
	arr := []any{
		rangePair("2024-01-01", "2024-01-05"),
		rangePair("2024-01-05", "2024-01-10"),
	}
	got, err := evalRangeMerge(env, []Expr{&Literal{Val: arr}}, row)
	if err != nil {
		t.Fatalf("evalRangeMerge error: %v", err)
	}
	merged := got.([]any)
	if len(merged) != 1 {
		t.Fatalf("touching ranges should merge into 1, got %d: %+v", len(merged), merged)
	}
}

func TestEvalRangeMergeUnsortedInput(t *testing.T) {
	// Input given out of start order must still merge and sort correctly --
	// the sweep sorts by start internally rather than assuming pre-sorted
	// input.
	env := ExecEnv{}
	row := Row{}
	arr := []any{
		rangePair("2024-03-01", "2024-03-05"),
		rangePair("2024-01-01", "2024-01-05"),
		rangePair("2024-02-01", "2024-02-05"),
	}
	got, err := evalRangeMerge(env, []Expr{&Literal{Val: arr}}, row)
	if err != nil {
		t.Fatalf("evalRangeMerge error: %v", err)
	}
	merged := got.([]any)
	if len(merged) != 3 {
		t.Fatalf("expected 3 disjoint ranges, got %d", len(merged))
	}
	prevStart := time.Time{}
	for i, m := range merged {
		start := m.([]any)[0].(time.Time)
		if i > 0 && !start.After(prevStart) {
			t.Fatalf("merged ranges not sorted by start: range %d start %v not after previous %v", i, start, prevStart)
		}
		prevStart = start
	}
}

func TestEvalRangeMergeEmptyAndNil(t *testing.T) {
	env := ExecEnv{}
	row := Row{}

	got, err := evalRangeMerge(env, []Expr{&Literal{Val: []any{}}}, row)
	if err != nil {
		t.Fatalf("evalRangeMerge([]) error: %v", err)
	}
	if arr, ok := got.([]any); !ok || len(arr) != 0 {
		t.Fatalf("evalRangeMerge([]) = %v, want empty array", got)
	}

	got, err = evalRangeMerge(env, []Expr{&Literal{Val: nil}}, row)
	if err != nil {
		t.Fatalf("evalRangeMerge(NULL) error: %v", err)
	}
	if got != nil {
		t.Fatalf("evalRangeMerge(NULL) = %v, want nil", got)
	}
}

func TestEvalRangeMergeRejectsReversedElement(t *testing.T) {
	env := ExecEnv{}
	row := Row{}
	arr := []any{rangePair("2024-01-10", "2024-01-01")} // end before start
	if _, err := evalRangeMerge(env, []Expr{&Literal{Val: arr}}, row); err == nil {
		t.Fatalf("evalRangeMerge should reject an element with end before start")
	}
}
