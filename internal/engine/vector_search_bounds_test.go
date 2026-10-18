package engine

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecSearchExplicitIndexKeepsMetric(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE metric_points (id INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO metric_points VALUES (1, VEC_FROM_JSON('[100,0]')), (2, VEC_FROM_JSON('[1,1]')), (3, VEC_FROM_JSON('[0,2]'))`)
	for _, index := range []string{"flat", "ivf", "hnsw"} {
		for _, tc := range []struct {
			metric string
			id     int
			dist   float64
		}{
			{"cosine", 1, 0}, {"l2", 2, 1}, {"manhattan", 2, 1}, {"dot", 1, -100},
			{"euclidean", 2, 1}, {"l1", 2, 1}, {"inner_product", 1, -100},
		} {
			query := fmt.Sprintf(`SELECT id, _vec_distance AS d FROM VEC_SEARCH('metric_points', 'embedding', '[1,0]', 1, '%s', '%s')`, tc.metric, index)
			rs := execSQL(t, db, query)
			if len(rs.Rows) != 1 || rs.Rows[0]["id"] != tc.id {
				t.Fatalf("%s: got %v, want id=%d", query, rs.Rows, tc.id)
			}
			if got := rs.Rows[0]["d"].(float64); math.Abs(got-tc.dist) > 1e-12 {
				t.Errorf("%s: distance %v, want %v", query, got, tc.dist)
			}
		}
	}
	for _, metric := range []string{"'invalid'", "123", "NULL"} {
		query := `SELECT * FROM VEC_SEARCH('metric_points', 'embedding', '[1,0]', 1, ` + metric + `, 'flat')`
		if _, err := Execute(context.Background(), db, "default", mustParse(query)); err == nil || !strings.Contains(err.Error(), "metric") {
			t.Errorf("%s: expected metric error, got %v", query, err)
		}
	}
}

func TestVecSearchParallelLargeKIsBounded(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	const rows = 4096
	if vecSearchWorkerCount(rows, 2) != 2 {
		t.Fatal("fixture must use the parallel merge")
	}
	db := makeVecSearchBenchmarkTable(rows, 2)
	table, err := db.Get("default", "vec_docs")
	if err != nil {
		t.Fatal(err)
	}
	cache := getVecColumnCache("default", table, 1, false)
	query := []float64{0.3, 0.7}
	result, err := vecSearchTopK(context.Background(), table.Rows, 2, math.MaxInt, cache, buildVecDistanceFunc("l2", query, 0), false)
	if err != nil || len(result) != rows {
		t.Fatalf("large k: got %d rows, error=%v", len(result), err)
	}
	for i := 1; i < len(result); i++ {
		if vecScoredRowLess(result[i], result[i-1]) {
			t.Fatalf("result not ordered at %d", i)
		}
	}
}

func TestVecSliceLargeLengthOwnsResult(t *testing.T) {
	input := []float64{1, 2, 3, 4}
	ex := &FuncCall{Name: "VEC_SLICE", Args: []Expr{&Literal{Val: input}, &Literal{Val: 1}, &Literal{Val: math.MaxInt}}}
	value, err := evalVecSlice(ExecEnv{}, ex, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := value.([]float64)
	if len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("slice result = %v", got)
	}
	got[0] = 99
	if input[1] != 2 {
		t.Fatal("slice aliases input")
	}
}

func TestVecSearchCosineExtremeFiniteValues(t *testing.T) {
	for _, scale := range []float64{1e300, 1e-300, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		db := storage.NewDB()
		table := storage.NewTable("extreme_vectors", []storage.Column{
			{Name: "id", Type: storage.IntType}, {Name: "embedding", Type: storage.VectorType},
		}, false)
		table.Rows = [][]any{
			{1, []float64{scale, scale}},
			{2, []float64{-scale, -scale}},
			{3, []float64{scale, -scale}},
		}
		if err := db.Put("default", table); err != nil {
			t.Fatal(err)
		}
		for _, index := range []string{"flat", "ivf", "hnsw"} {
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			args := []Expr{
				&Literal{Val: table.Name}, &Literal{Val: "embedding"},
				&Literal{Val: []float64{scale, scale}}, &Literal{Val: 3},
				&Literal{Val: "cosine"}, &Literal{Val: index},
			}
			result, err := (&VecSearchTableFunc{}).Execute(t.Context(), args, env, nil)
			if err != nil || len(result.Rows) != 3 {
				t.Fatalf("scale=%g index=%s: result=%#v, err=%v", scale, index, result, err)
			}
			for i, id := range []int{1, 3, 2} {
				got := result.Rows[i]
				if got["id"] != id || math.Abs(got["_vec_distance"].(float64)-float64(i)) > 1e-12 {
					t.Errorf("scale=%g index=%s rank=%d: got %v, want id=%d distance=%d", scale, index, i, got, id, i)
				}
			}
		}
	}
}
