package engine

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawJoinLookupMatchesSliceBuckets(t *testing.T) {
	keys := []any{1, 2, 1, nil, int64(1), 1.0, "1", []byte("a"),
		1, 2, []byte("a"), math.NaN(), 0.0, math.Copysign(0, -1), true, true}
	rows := make([][]any, len(keys))
	want := make(map[any][][]any)
	for i, key := range keys {
		keep := i%3 != 2
		rows[i] = []any{key, i, keep}
		if key != nil && keep {
			canonical := comparableKeyPart(key)
			want[canonical] = append(want[canonical], rows[i])
		}
	}
	var visited []int
	plan := &simpleJoinPlan{right: &storage.Table{Rows: rows, Version: 1}, rightKey: 0,
		rightFilter: func(row []any) (bool, error) {
			visited = append(visited, row[1].(int))
			return row[2].(bool), nil
		}}
	index, err := plan.rightRowsByKey()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range append(keys, "missing") {
		canonical := comparableKeyPart(key)
		if got := collectRawJoinMatches(index, canonical); !reflect.DeepEqual(got, want[canonical]) {
			t.Fatalf("key %T(%v): got %#v, want %#v", key, key, got, want[canonical])
		}
	}
	for i := range rows {
		if len(visited) != len(rows) || visited[i] != i {
			t.Fatalf("right predicate changed evaluation count/order: %v", visited)
		}
	}
}

func TestRawJoinLookupVersionAndTableIdentity(t *testing.T) {
	plan := &simpleJoinPlan{right: &storage.Table{Rows: [][]any{{"k", 1}, {"k", 2}}, Version: 1}}
	first, err := plan.rightRowsByKey()
	if err != nil {
		t.Fatal(err)
	}
	if again, err := plan.rightRowsByKey(); err != nil || again != first {
		t.Fatalf("unchanged input did not reuse index: %v", err)
	}
	plan.right.Rows = [][]any{{"k", 3}, {"other", 4}, {"k", 5}}
	plan.right.Version++
	second, err := plan.rightRowsByKey()
	if err != nil || second == first {
		t.Fatalf("version change did not rebuild index: %v", err)
	}
	if got := collectRawJoinMatches(first, "k"); !reflect.DeepEqual(got, [][]any{{"k", 1}, {"k", 2}}) {
		t.Fatalf("rebuild changed an old index: %v", got)
	}
	if got := collectRawJoinMatches(second, "k"); !reflect.DeepEqual(got, [][]any{{"k", 3}, {"k", 5}}) {
		t.Fatal(got)
	}
	plan.right = &storage.Table{Rows: [][]any{{"k", 6}}, Version: plan.right.Version}
	third, err := plan.rightRowsByKey()
	if err != nil || third == second || third.next != nil {
		t.Fatalf("table replacement/unique-key index: %v", err)
	}
	if got := collectRawJoinMatches(third, "k"); !reflect.DeepEqual(got, [][]any{{"k", 6}}) {
		t.Fatal(got)
	}
}

func TestRawJoinLookupConcurrentColdBuild(t *testing.T) {
	const size = 20000
	rows := make([][]any, size)
	for i := range rows {
		rows[i] = []any{i}
	}
	var calls atomic.Int64
	plan := &simpleJoinPlan{right: &storage.Table{Rows: rows, Version: 1},
		rightFilter: func([]any) (bool, error) { calls.Add(1); return true, nil }}
	type outcome struct {
		index *simpleJoinRightIndex
		err   error
	}
	start := make(chan struct{})
	results := make(chan outcome, 8)
	var wg sync.WaitGroup
	for range cap(results) {
		wg.Go(func() {
			<-start
			index, err := plan.rightRowsByKey()
			results <- outcome{index, err}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	var first *simpleJoinRightIndex
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if first == nil {
			first = result.index
		}
		if result.index != first || len(result.index.byKey) != size || result.index.next != nil {
			t.Fatal("concurrent queries did not share a complete immutable unique-key index")
		}
	}
	if calls.Load() != size {
		t.Fatalf("right predicate evaluated %d times, want one build of %d rows", calls.Load(), size)
	}
}

func TestRawJoinLookupFailedBuildDoesNotPublish(t *testing.T) {
	plan := &simpleJoinPlan{right: &storage.Table{Rows: [][]any{{1}, {1}}, Version: 1}}
	first, err := plan.rightRowsByKey()
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("right filter failed")
	plan.right.Version++
	plan.rightFilter = func([]any) (bool, error) { return false, wantErr }
	if index, err := plan.rightRowsByKey(); index != nil || !errors.Is(err, wantErr) {
		t.Fatalf("failed build: index=%v, error=%v", index, err)
	}
	if plan.rightLookup.index != first || plan.rightLookup.version != 1 {
		t.Fatal("failed build published an incomplete index")
	}
	plan.rightFilter = nil
	if index, err := plan.rightRowsByKey(); err != nil || index == first || len(collectRawJoinMatches(index, 1)) != 2 {
		t.Fatalf("retry did not rebuild: %v", err)
	}
}

func TestRawJoinLargeDuplicateOrderAndAggregates(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	left := storage.NewTable("lookup_left", []storage.Column{{Name: "id", Type: storage.IntType}}, false)
	left.Rows = [][]any{{1}, {2500}, {nil}}
	right := storage.NewTable("lookup_right", []storage.Column{
		{Name: "id", Type: storage.IntType}, {Name: "seq", Type: storage.IntType},
		{Name: "keep", Type: storage.BoolType}, {Name: "amount", Type: storage.Float64Type},
	}, false)
	for i := 0; i < 5000; i++ {
		right.Rows = append(right.Rows, []any{i, i, i%3 != 0, float64(i + 1)})
	}
	right.Rows = append(right.Rows, []any{1, 5001, true, 3.0}, []any{1, 5002, false, 4.0},
		[]any{2500, 5003, true, 5.0}, []any{nil, 5004, true, 99.0})
	for _, table := range []*storage.Table{left, right} {
		if err := db.Put("default", table); err != nil {
			t.Fatal(err)
		}
	}
	tail := " FROM lookup_left l JOIN lookup_right r ON l.id = r.id WHERE r.keep = TRUE"
	stmt := mustParse("SELECT l.id AS id, r.seq AS seq" + tail)
	want := []Row{{"id": 1, "seq": 1}, {"id": 1, "seq": 5001}, {"id": 2500, "seq": 2500}, {"id": 2500, "seq": 5003}}
	for range 2 {
		got, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || !reflect.DeepEqual(got.Rows, want) {
			t.Fatalf("duplicate encounter order: %#v, %v", got, err)
		}
	}
	for _, query := range []string{
		"SELECT r.id AS g, COUNT(*) AS n" + tail + " GROUP BY r.id",
		"SELECT r.id AS g, COUNT(*) AS n, SUM(r.amount) AS total" + tail + " GROUP BY r.id",
	} {
		got := execSQL(t, db, query)
		if len(got.Rows) != 2 || got.Rows[0]["g"] != 1 || got.Rows[1]["g"] != 2500 || got.Rows[0]["n"] != 2 || got.Rows[1]["n"] != 2 {
			t.Fatal(got.Rows)
		}
		if len(got.Cols) == 3 && (got.Rows[0]["total"] != 5.0 || got.Rows[1]["total"] != 2506.0) {
			t.Fatal(got.Rows)
		}
	}
	execSQL(t, db, "INSERT INTO lookup_right VALUES (1, 6000, TRUE, 7.0)")
	execSQL(t, db, "UPDATE lookup_right SET keep = FALSE WHERE seq = 5001")
	execSQL(t, db, "DELETE FROM lookup_right WHERE seq = 1")
	got, err := Execute(t.Context(), db, "default", stmt)
	want = []Row{{"id": 1, "seq": 6000}, {"id": 2500, "seq": 2500}, {"id": 2500, "seq": 5003}}
	if err != nil || !reflect.DeepEqual(got.Rows, want) {
		t.Fatalf("DML did not invalidate cached row IDs: %#v, %v", got, err)
	}
}

func collectRawJoinMatches(index *simpleJoinRightIndex, key any) [][]any {
	var rows [][]any
	for rowID := index.first(key); rowID != 0; rowID = index.after(rowID) {
		rows = append(rows, index.rows[rowID-1])
	}
	return rows
}
