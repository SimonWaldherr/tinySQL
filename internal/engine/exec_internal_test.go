package engine

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func newTestExecEnv() ExecEnv {
	return ExecEnv{ctx: context.Background(), tenant: "tenant", db: storage.NewDB()}
}

//nolint:gocyclo // Exhaustively exercises JSON helper branches.
func TestEvalJSONExtended(t *testing.T) {
	env := newTestExecEnv()
	row := Row{}

	base := map[string]any{
		"user": map[string]any{"name": "Alice"},
	}

	updatedAny, err := evalFuncCall(env, &FuncCall{
		Name: "JSON_SET",
		Args: []Expr{
			&Literal{Val: base},
			&Literal{Val: "user.age"},
			&Literal{Val: 30},
		},
	}, row)
	if err != nil {
		t.Fatalf("JSON_SET map path failed: %v", err)
	}
	updated, ok := updatedAny.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", updatedAny)
	}
	user, ok := updated["user"].(map[string]any)
	if !ok || user["age"] != 30 {
		t.Fatalf("expected age=30 in nested map, got %#v", updated["user"])
	}

	arrayAny, err := evalFuncCall(env, &FuncCall{
		Name: "JSON_SET",
		Args: []Expr{
			&Literal{Val: nil},
			&Literal{Val: "[2]"},
			&Literal{Val: "foo"},
		},
	}, row)
	if err != nil {
		t.Fatalf("JSON_SET array path failed: %v", err)
	}
	arr, ok := arrayAny.([]any)
	if !ok {
		t.Fatalf("expected slice result, got %T", arrayAny)
	}
	if len(arr) != 3 || arr[2] != "foo" {
		t.Fatalf("expected array with foo at index 2, got %#v", arr)
	}

	nestedAny, err := evalFuncCall(env, &FuncCall{
		Name: "JSON_SET",
		Args: []Expr{
			&Literal{Val: updated},
			&Literal{Val: "user.tags[1]"},
			&Literal{Val: "go"},
		},
	}, row)
	if err != nil {
		t.Fatalf("JSON_SET nested array path failed: %v", err)
	}
	nested, ok := nestedAny.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", nestedAny)
	}
	tags, ok := nested["user"].(map[string]any)["tags"].([]any)
	if !ok || len(tags) < 2 || tags[1] != "go" {
		t.Fatalf("expected tags array with 'go', got %#v", nested["user"])
	}

	nameAny, err := evalFuncCall(env, &FuncCall{
		Name: "JSON_EXTRACT",
		Args: []Expr{
			&Literal{Val: nested},
			&Literal{Val: "user.name"},
		},
	}, row)
	if err != nil {
		t.Fatalf("JSON_EXTRACT failed: %v", err)
	}
	if nameAny != "Alice" {
		t.Fatalf("expected extracted name 'Alice', got %#v", nameAny)
	}
}

//nolint:gocyclo // Aggregation helper coverage requires multiple branch assertions.
func TestAggregateHelpers(t *testing.T) {
	env := newTestExecEnv()
	rows := []Row{
		{"val": 1},
		{"val": 2},
		{"val": nil},
	}

	countRes, err := evalAggregate(env, &FuncCall{Name: "COUNT", Args: []Expr{&VarRef{Name: "val"}}}, rows)
	if err != nil {
		t.Fatalf("COUNT aggregate failed: %v", err)
	}
	if countRes != 2 {
		t.Fatalf("expected COUNT 2, got %#v", countRes)
	}

	sumRes, err := evalAggregate(env, &FuncCall{Name: "SUM", Args: []Expr{&VarRef{Name: "val"}}}, rows)
	if err != nil {
		t.Fatalf("SUM aggregate failed: %v", err)
	}
	if math.Abs(sumRes.(float64)-3.0) > 1e-9 {
		t.Fatalf("expected SUM 3, got %#v", sumRes)
	}

	avgRes, err := evalAggregate(env, &FuncCall{Name: "AVG", Args: []Expr{&VarRef{Name: "val"}}}, rows)
	if err != nil {
		t.Fatalf("AVG aggregate failed: %v", err)
	}
	if math.Abs(avgRes.(float64)-1.5) > 1e-9 {
		t.Fatalf("expected AVG 1.5, got %#v", avgRes)
	}

	plusRes, err := evalAggregate(env, &Unary{Op: "+", Expr: &FuncCall{Name: "SUM", Args: []Expr{&VarRef{Name: "val"}}}}, rows)
	if err != nil {
		t.Fatalf("Unary + aggregate failed: %v", err)
	}
	if math.Abs(plusRes.(float64)-3.0) > 1e-9 {
		t.Fatalf("expected unary + result 3, got %#v", plusRes)
	}

	minusRes, err := evalAggregate(env, &Unary{Op: "-", Expr: &FuncCall{Name: "SUM", Args: []Expr{&VarRef{Name: "val"}}}}, rows)
	if err != nil {
		t.Fatalf("Unary - aggregate failed: %v", err)
	}
	if math.Abs(minusRes.(float64)+3.0) > 1e-9 {
		t.Fatalf("expected unary - result -3, got %#v", minusRes)
	}

	notRes, err := evalAggregate(env, &Unary{Op: "NOT", Expr: &FuncCall{Name: "COUNT", Args: []Expr{&VarRef{Name: "val"}}}}, rows)
	if err != nil {
		t.Fatalf("Unary NOT aggregate failed: %v", err)
	}
	if notRes != false {
		t.Fatalf("expected unary NOT false, got %#v", notRes)
	}

	notUnknown, err := evalAggregate(env, &Unary{Op: "NOT", Expr: &FuncCall{Name: "AVG", Args: []Expr{&VarRef{Name: "val"}}}}, []Row{})
	if err != nil {
		t.Fatalf("Unary NOT aggregate on empty rows failed: %v", err)
	}
	if notUnknown != nil {
		t.Fatalf("expected unary NOT on nil to return nil, got %#v", notUnknown)
	}

	combinedRes, err := evalAggregate(env, &Binary{
		Op:    "+",
		Left:  &FuncCall{Name: "SUM", Args: []Expr{&VarRef{Name: "val"}}},
		Right: &FuncCall{Name: "COUNT", Args: []Expr{&VarRef{Name: "val"}}},
	}, rows)
	if err != nil {
		t.Fatalf("Binary aggregate failed: %v", err)
	}
	if math.Abs(combinedRes.(float64)-5.0) > 1e-9 {
		t.Fatalf("expected SUM+COUNT = 5, got %#v", combinedRes)
	}

	isNullRes, err := evalAggregate(env, &IsNull{Expr: &FuncCall{Name: "AVG", Args: []Expr{&VarRef{Name: "val"}}}}, []Row{})
	if err != nil {
		t.Fatalf("IS NULL aggregate failed: %v", err)
	}
	if isNullRes != true {
		t.Fatalf("expected AVG IS NULL to be true, got %#v", isNullRes)
	}

	isNotNullRes, err := evalAggregate(env, &IsNull{Expr: &FuncCall{Name: "SUM", Args: []Expr{&VarRef{Name: "val"}}}, Negate: true}, rows)
	if err != nil {
		t.Fatalf("IS NOT NULL aggregate failed: %v", err)
	}
	if isNotNullRes != true {
		t.Fatalf("expected SUM IS NOT NULL to be true, got %#v", isNotNullRes)
	}

	row := Row{"val": nil}
	starRes, err := evalFuncCall(env, &FuncCall{Name: "COUNT", Star: true}, row)
	if err != nil {
		t.Fatalf("COUNT(*) eval failed: %v", err)
	}
	if starRes != 1 {
		t.Fatalf("expected COUNT(*) to return 1, got %#v", starRes)
	}

	zeroRes, err := evalFuncCall(env, &FuncCall{Name: "COUNT", Args: []Expr{&VarRef{Name: "val"}}}, row)
	if err != nil {
		t.Fatalf("COUNT(column) eval failed: %v", err)
	}
	if zeroRes != 0 {
		t.Fatalf("expected COUNT(column) to return 0, got %#v", zeroRes)
	}

	singleRes, err := evalFuncCall(env, &FuncCall{Name: "SUM", Args: []Expr{&Literal{Val: 7}}}, row)
	if err != nil {
		t.Fatalf("SUM literal eval failed: %v", err)
	}
	if singleRes != 7 {
		t.Fatalf("expected SUM literal to return 7, got %#v", singleRes)
	}
}

//nolint:gocyclo // Coercion helper coverage spans many type branches.
func TestColumnsAndCoerceHelpers(t *testing.T) {
	rows := []Row{
		{"alpha": 1, "beta": 2},
		{"beta": 3, "gamma": 4},
	}
	cols := columnsFromRows(rows)
	expected := []string{"alpha", "beta", "gamma"}
	if !reflect.DeepEqual(cols, expected) {
		t.Fatalf("unexpected columns: got %v want %v", cols, expected)
	}

	f1, err := coerceToFloat(10)
	if err != nil || f1.(float64) != 10 {
		t.Fatalf("coerceToFloat int failed: %v %#v", err, f1)
	}
	f2, err := coerceToFloat("3.5")
	if err != nil || math.Abs(f2.(float64)-3.5) > 1e-9 {
		t.Fatalf("coerceToFloat string failed: %v %#v", err, f2)
	}
	f3, err := coerceToFloat(true)
	if err != nil || math.Abs(f3.(float64)-1.0) > 1e-9 {
		t.Fatalf("coerceToFloat bool failed: %v %#v", err, f3)
	}
	if _, err := coerceToFloat(struct{}{}); err == nil {
		t.Fatal("expected coerceToFloat to fail for struct value")
	}

	b1, err := coerceToBool("yes")
	if err != nil || b1 != true {
		t.Fatalf("coerceToBool string failed: %v %#v", err, b1)
	}
	b2, err := coerceToBool(0)
	if err != nil || b2 != false {
		t.Fatalf("coerceToBool zero failed: %v %#v", err, b2)
	}
	if _, err := coerceToBool([]byte("x")); err == nil {
		t.Fatal("expected coerceToBool to fail for []byte")
	}

	jsonVal, err := coerceToJson(`{"foo": 1}`)
	if err != nil {
		t.Fatalf("coerceToJson parse failed: %v", err)
	}
	if _, ok := jsonVal.(map[string]any); !ok {
		t.Fatalf("expected parsed JSON to be map, got %#v", jsonVal)
	}

	sameVal, err := coerceToJson(42)
	if err != nil || sameVal != 42 {
		t.Fatalf("coerceToJson passthrough failed: %v %#v", err, sameVal)
	}

	nilVal, err := coerceToTypeAllowNull(nil, storage.FloatType)
	if err != nil || nilVal != nil {
		t.Fatalf("coerceToTypeAllowNull expected nil, got %#v err=%v", nilVal, err)
	}
}
