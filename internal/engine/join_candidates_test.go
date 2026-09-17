package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestJoinCandidateLookupMatchesNestedLoops(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right func(int) any
		change      func([]Row, []Row)
		indexed     bool
	}{
		{name: "integers", left: func(i int) any { return i % 7 }, right: func(i int) any { return int64(i % 9) }, indexed: true},
		{name: "exact-large-integers", left: func(i int) any { return int64(1<<53) + int64(i%7) }, right: func(i int) any { return int64(1<<53) + int64(i%9) }, indexed: true},
		{name: "floats", left: func(i int) any { return float64(i % 7) }, right: func(i int) any { return float64(i % 9) }, indexed: true},
		{name: "strings", left: func(i int) any { return fmt.Sprint(i % 7) }, right: func(i int) any { return fmt.Sprint(i % 9) }, indexed: true},
		{name: "booleans", left: func(i int) any { return i%2 == 0 }, right: func(i int) any { return i%3 == 0 }, indexed: true},
		{name: "nulls", left: func(i int) any {
			if i%2 == 0 {
				return nil
			}
			return i % 7
		}, right: func(i int) any {
			if i%2 == 0 {
				return nil
			}
			return i % 9
		}, indexed: true},
		{name: "all-null", left: func(int) any { return nil }, right: func(int) any { return nil }, indexed: true},
		{name: "mixed-numbers", left: func(i int) any { return i % 7 }, right: func(i int) any { return float64(i % 9) }},
		{name: "blobs", left: func(i int) any { return []byte{byte(i % 7)} }, right: func(i int) any { return []byte{byte(i % 9)} }},
		{name: "incomparable", left: func(i int) any { return i % 7 }, right: func(i int) any { return fmt.Sprint(i % 9) }},
		{name: "missing-last-key", left: func(i int) any { return i % 7 }, right: func(i int) any { return i % 9 }, change: func(l, r []Row) { delete(r[len(r)-1], "r.id") }},
		{name: "shadowed-last-key", left: func(i int) any { return i % 7 }, right: func(i int) any { return i % 9 }, change: func(l, r []Row) { r[len(r)-1]["l.id"] = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := make([]Row, 40), make([]Row, 40)
			for i := range left {
				left[i] = Row{"l.id": tc.left(i), "l.label": i}
				right[i] = Row{"r.id": tc.right(i), "r.label": i}
			}
			if tc.change != nil {
				tc.change(left, right)
			}
			env := ExecEnv{ctx: context.Background()}
			metadata := &storage.Table{Cols: []storage.Column{{Name: "id"}, {Name: "label"}}}
			for _, on := range []string{"l.id = r.id", "r.id = l.id"} {
				condition := mustParse("SELECT * FROM l JOIN r ON " + on).(*Select).Joins[0].On
				for _, reverse := range []bool{false, true} {
					outer, inner := left, right
					if reverse {
						outer, inner = right, left
					}
					lookup, err := buildJoinCandidateLookup(env, compileJoinEquality(condition), outer, inner)
					if err != nil || (lookup != nil) != tc.indexed {
						t.Fatalf("%s reverse=%v: indexed=%v error=%v", on, reverse, lookup != nil, err)
					}
				}
				reference := &Binary{Op: "AND", Left: condition, Right: &Literal{Val: true}}
				for _, kind := range []string{"inner", "left", "right", "full"} {
					run := func(on Expr) ([]Row, error) {
						switch kind {
						case "inner":
							return processInnerJoin(env, left, right, on)
						case "left":
							return processLeftJoin(env, left, right, on, "r", metadata)
						case "right":
							return processRightJoin(env, left, right, on)
						default:
							return processFullOuterJoin(env, left, right, on, "r", metadata)
						}
					}
					got, err := run(condition)
					want, wantErr := run(reference)
					if fmt.Sprint(err) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
						t.Fatalf("%s %s: result or error differs from nested loop (errors %v / %v)", kind, on, err, wantErr)
					}
				}
			}
		})
	}
}

func TestJoinLookupSpecialFloatKeys(t *testing.T) {
	if _, kind := joinLookupKey(math.NaN()); kind != 255 {
		t.Fatal("NaN must use SQL comparison fallback")
	}
	positive, kind := joinLookupKey(0.0)
	negative, otherKind := joinLookupKey(math.Copysign(0, -1))
	if kind != otherKind || positive != negative {
		t.Fatal("signed zeros must share a key")
	}
}

func TestJoinCandidateLookupCancellationAndRowLimit(t *testing.T) {
	left, right := make([]Row, 40), make([]Row, 40)
	for i := range left {
		left[i] = Row{"l.id": 1}
		right[i] = Row{"r.id": 1}
	}
	condition := mustParse("SELECT * FROM l JOIN r ON l.id = r.id").(*Select).Joins[0].On
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := buildJoinCandidateLookup(ExecEnv{ctx: ctx}, compileJoinEquality(condition), left, right); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context cancellation", err)
	}
	env := ExecEnv{ctx: context.Background()}
	if lookup, err := buildJoinCandidateLookup(env, compileJoinEquality(condition), left, right); lookup == nil || err != nil {
		t.Fatalf("fixture did not enable lookup: %v", err)
	}
	previous := maxJoinRows
	maxJoinRows = 100
	defer func() { maxJoinRows = previous }()
	if _, err := processInnerJoin(env, left, right, condition); err == nil || !strings.Contains(err.Error(), "row limit") {
		t.Fatalf("indexed duplicate matches must honor the row limit, got %v", err)
	}
}
