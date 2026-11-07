package engine

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestConstantVectorScorerMatchesEvaluator(t *testing.T) {
	limit := 2
	plan := &simpleSelectPlan{limit: &limit, colIndex: map[string]int{"v": 0}}
	for _, name := range []string{"VEC_DOT", "VEC_DOT_PRODUCT", "VEC_INNER_PRODUCT", "VEC_COSINE_SIMILARITY", "VEC_COSINE_DISTANCE", "VEC_L2_DISTANCE", "VEC_EUCLIDEAN_DISTANCE", "VEC_MANHATTAN_DISTANCE", "VEC_L1_DISTANCE", "VEC_HAMMING_DISTANCE"} {
		for _, query := range []any{[]float64{1, 2, 3}, "[1,2,3]", []any{1.0, 2.0, 3.0}, []float64{}, []float64{0, 0, 0}} {
			call := &FuncCall{Name: name, Args: []Expr{&VarRef{Name: "v"}, &Literal{Val: query}}}
			plan.orderExprs = []Expr{call}
			scorer, ok := simpleFloatOrderScorer(plan)
			if !ok {
				t.Fatal("constant vector was not prepared", name, query)
			}
			for _, value := range []any{[]float64{3, 2, 1}, []float64{0, 0, 0}, []float64{1}, []float64{math.NaN(), 1, 2}, []float64{math.Inf(1), 1, 2}, "[3,2,1]", nil, "invalid"} {
				raw := []any{value}
				want, wantErr := evalRawFuncCall(plan, raw, call)
				got, gotErr := scorer(raw)
				expected, numericResult := numeric(want)
				if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || gotErr == nil && (!numericResult || !conversionValuesEqual(got, expected)) {
					t.Fatalf("%s(%v,%v): %v / %v; want %v / %v", name, value, query, got, gotErr, want, wantErr)
				}
			}
		}
	}
}

func TestConstantVectorOrderKeepsFiltersAliasesAndErrors(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE constant_vectors (id INT, embedding VECTOR)")
	execSQL(t, db, "INSERT INTO constant_vectors VALUES (3,'[1,0]'),(1,'[1,0]'),(2,'[0,1]'),(4,NULL)")
	execSQL(t, db, "CREATE INDEX constant_vector_id ON constant_vectors(id)")
	for _, name := range []string{"VEC_DOT_PRODUCT", "VEC_COSINE_DISTANCE", "VEC_EUCLIDEAN_DISTANCE", "VEC_L1_DISTANCE", "VEC_HAMMING_DISTANCE"} {
		for _, tail := range []string{" LIMIT 2", " DESC LIMIT 1 OFFSET 1", " LIMIT 100 OFFSET 9"} {
			selectSQL := "SELECT id," + name + "(embedding,'[1,0]') AS score FROM "
			whereSQL := " WHERE id BETWEEN 1 AND 3 ORDER BY score" + tail
			got := execSQL(t, db, selectSQL+"constant_vectors"+whereSQL)
			want := execSQL(t, db, selectSQL+"(SELECT * FROM constant_vectors) q"+whereSQL)
			if !reflect.DeepEqual(got.Rows, want.Rows) || !reflect.DeepEqual(got.Cols, want.Cols) {
				t.Fatal(name, tail, got, want)
			}
		}
	}
	// Invalid queries are not raised during preparation: no surviving rows
	// means no distance evaluation, and the first vector argument still wins.
	for _, input := range []string{"'bad'", "NULL"} {
		rs := execSQL(t, db, "SELECT id,VEC_L2_DISTANCE(embedding,"+input+") AS score FROM constant_vectors WHERE id<0 ORDER BY score LIMIT 1")
		if len(rs.Rows) != 0 {
			t.Fatal(rs)
		}
	}
	for _, call := range []string{"VEC_L2_DISTANCE(embedding,'[1,0]',99)", "VEC_EUCLIDEAN_DISTANCE(embedding,'[1,0]',99)"} {
		_, gotErr := Execute(t.Context(), db, "default", mustParse("SELECT "+call+" AS score FROM constant_vectors WHERE id=1 ORDER BY score LIMIT 1"))
		_, wantErr := Execute(t.Context(), db, "default", mustParse("SELECT "+call+" AS score FROM (SELECT * FROM constant_vectors) q WHERE id=1 ORDER BY score LIMIT 1"))
		if gotErr == nil || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
			t.Fatal("scorer bypassed argument validation", call, gotErr, wantErr)
		}
	}
	stmt := mustParse("SELECT id,VEC_L2_DISTANCE(embedding,'[1,0]') AS score FROM constant_vectors WHERE id<4 ORDER BY score LIMIT 1").(*Select)
	query := stmt.Projs[1].Expr.(*FuncCall).Args[1].(*Literal)
	query.Parameter = true
	for _, input := range []struct {
		value any
		id    int
	}{{"[1,0]", 3}, {"[0,1]", 2}, {[]any{1.0, 0.0}, 3}, {[]float64{0, 1}, 2}} {
		query.Val = input.value
		got, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || len(got.Rows) != 1 || got.Rows[0]["id"] != input.id {
			t.Fatal("search retained the previous parameter vector", input, got, err)
		}
	}
}
