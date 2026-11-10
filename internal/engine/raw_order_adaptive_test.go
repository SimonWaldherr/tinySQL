package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawAdaptiveSortRequiresTotalOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []any
		full   bool
	}{
		{"integer", []any{1, nil, 2}, true},
		{"int64", []any{int64(math.MaxInt64), nil, int64(math.MinInt64)}, true},
		{"float", []any{math.Inf(-1), math.Copysign(0, -1), 0.0, math.Inf(1)}, true},
		{"text", []any{"ä", nil, "a"}, true},
		{"boolean", []any{false, nil, true}, true},
		{"blob", []any{[]byte{255}, nil, []byte{0}}, true},
		{"null", []any{nil, nil, nil}, true},
		{"nan", []any{1.0, math.NaN(), 2.0}, false},
		{"mixed_numeric", []any{int64(9007199254740993), float64(9007199254740992), int64(9007199254740992)}, false},
		{"mixed_text", []any{"a", 1, "b"}, false},
		{"decimal", []any{big.NewRat(1, 3), big.NewRat(2, 3)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([][]any, 128)
			for i := range rows {
				rows[i] = []any{tc.values[i%len(tc.values)]}
			}
			plan := &simpleSelectPlan{orderCols: []int{0}}
			got, err := preferFullRawSort(ExecEnv{ctx: t.Context()}, plan, rows, len(rows))
			if err != nil || got != tc.full {
				t.Fatalf("full sort=%v / %v, want %v", got, err, tc.full)
			}
		})
	}
	rows := make([][]any, 100)
	for i := range rows {
		rows[i] = []any{i}
	}
	for _, tc := range []struct {
		plan *simpleSelectPlan
		keep int
	}{
		{&simpleSelectPlan{orderCols: []int{0}}, 20},
		{&simpleSelectPlan{orderCols: []int{0}}, 90},
		{&simpleSelectPlan{orderCols: []int{0}, where: &Literal{Val: true}}, 100},
		{&simpleSelectPlan{orderCols: []int{0}, rowIDs: []int{0}}, 100},
		{&simpleSelectPlan{}, 100},
		{&simpleSelectPlan{orderCols: []int{1}}, 100},
	} {
		if full, err := preferFullRawSort(ExecEnv{ctx: t.Context()}, tc.plan, rows, tc.keep); err != nil || full {
			t.Fatalf("ineligible full sort: %v / %v", full, err)
		}
	}
	if full, err := preferFullRawSort(ExecEnv{ctx: t.Context()}, &simpleSelectPlan{orderCols: []int{0}}, rows[:63], 63); err != nil || full {
		t.Fatalf("small input should retain its existing sort: %v / %v", full, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := preferFullRawSort(ExecEnv{ctx: ctx}, &simpleSelectPlan{orderCols: []int{0}}, rows, 100); !errors.Is(err, context.Canceled) {
		t.Fatal("validation ignored cancellation", err)
	}
}

func TestRawAdaptivePagesMatchFullSort(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE adaptive_order (id INT, grp TEXT, val FLOAT, nullable INT, flag BOOL, payload BLOB)")
	table, _ := db.Get("default", "adaptive_order")
	const size = 257
	for i := range size {
		var nullable any = (i * 97) % 17
		if i%13 == 0 {
			nullable = nil
		}
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("group-%d", i%7), float64((i * 97) % 17), nullable, i%3 == 0, []byte{byte(i % 11), 0}})
	}
	table.Version++
	for _, order := range []string{
		"id DESC", "grp, val DESC", "val DESC", "val ASC", "nullable ASC NULLS FIRST",
		"nullable DESC NULLS LAST", "flag DESC, grp", "payload ASC, id DESC",
	} {
		base := "SELECT id FROM adaptive_order ORDER BY " + order
		want := execSQL(t, db, base)
		cutover := size - size/10
		for _, limit := range []int{1, size / 2, cutover - 1, cutover, cutover + 1, size - 1, size, size + 1} {
			for _, offset := range []int{0, 5, size - 10, size + 1} {
				got := execSQL(t, db, fmt.Sprintf("%s LIMIT %d OFFSET %d", base, limit, offset))
				start, end := min(offset, size), min(offset+limit, size)
				if !reflect.DeepEqual(got.Rows, want.Rows[start:end]) {
					t.Fatalf("%s limit=%d offset=%d: got %v, want %v", order, limit, offset, got.Rows, want.Rows[start:end])
				}
			}
		}
	}
	// Switching algorithms must never reorder the stored row slice.
	for i, row := range table.Rows {
		if row[0] != i {
			t.Fatalf("source row %d changed: %v", i, row)
		}
	}
	// Projection still follows page selection; sorting alone must not evaluate
	// a discarded divide-by-zero, while a visible one must still fail.
	execSQL(t, db, "SELECT 1 / (id - 256) AS inverse FROM adaptive_order ORDER BY id LIMIT 255")
	if _, err := Execute(t.Context(), db, "default", mustParse("SELECT 1 / (id - 256) AS inverse FROM adaptive_order ORDER BY id LIMIT 256 OFFSET 1")); err == nil {
		t.Fatal("visible projection error was lost")
	}
}

func TestRawAdaptiveSortAcrossKeyChunks(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE adaptive_chunks (id INT, grp TEXT, val INT)")
	table, _ := db.Get("default", "adaptive_chunks")
	const size = 2*rawKeyArenaChunkRows + 17
	for i := range size {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("group-%d", i%47), (i * 97) % 127})
	}
	table.Version++
	base := "SELECT id FROM adaptive_chunks ORDER BY grp, val DESC"
	want := execSQL(t, db, base)
	for _, page := range [][2]int{{size - 1, 0}, {20, size - 20}} {
		got := execSQL(t, db, fmt.Sprintf("%s LIMIT %d OFFSET %d", base, page[0], page[1]))
		if !reflect.DeepEqual(got.Rows, want.Rows[page[1]:page[0]+page[1]]) {
			t.Fatal("sorting across key chunks changed the page")
		}
	}
}
