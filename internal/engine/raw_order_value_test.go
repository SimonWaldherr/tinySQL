package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Force only sort-key evaluation through the interpreter. The unselected
// unknown arm prevents binding, while the chosen arm preserves the value and
// error of the original expression without changing its type.
func rawOrderInterpreterPlan(plan *simpleSelectPlan) *simpleSelectPlan {
	reference := *plan
	reference.orderCols = nil
	reference.orderExprs = make([]Expr, len(plan.orderExprs))
	for i, expr := range plan.orderExprs {
		reference.orderExprs[i] = &FuncCall{Name: "IF", Args: []Expr{
			&Literal{Val: true}, expr, &FuncCall{Name: "unknown_order_arm"},
		}}
	}
	return &reference
}

func checkRawOrderValueResult(t *testing.T, got, want *ResultSet, err, wantErr error) {
	t.Helper()
	if fmt.Sprint(err) != fmt.Sprint(wantErr) {
		t.Fatalf("different errors: %v / %v", err, wantErr)
	}
	if err != nil {
		return
	}
	if !reflect.DeepEqual(got.Cols, want.Cols) || len(got.Rows) != len(want.Rows) {
		t.Fatalf("different result shape: %v / %v", got, want)
	}
	for i, row := range got.Rows {
		for key, value := range row {
			other, present := want.Rows[i][key]
			if !present {
				t.Fatalf("missing output key %q", key)
			}
			if real, ok := value.(float64); ok {
				otherReal, ok := other.(float64)
				if !ok || math.Float64bits(real) != math.Float64bits(otherReal) {
					t.Fatalf("row %d key %s: float value changed", i, key)
				}
			} else if !reflect.DeepEqual(value, other) {
				t.Fatalf("row %d key %s: %T(%v), want %T(%v)", i, key, value, value, other, other)
			}
		}
	}
}

func TestRawOrderValuesMatchInterpreter(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("bound_order", []storage.Column{
		{Name: "id", Type: storage.IntType}, {Name: "v", Type: storage.InterfaceType}, {Name: "label", Type: storage.TextType},
	}, false)
	values := []any{nil, 0, int64(9007199254740993), int64(9007199254740992), 1.5,
		math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), "a", "z", false, true,
		[]byte{0, 255}, []byte{0}, big.NewRat(1, 3), big.NewRat(2, 3)}
	for i := range 257 {
		table.Rows = append(table.Rows, []any{i, values[i%len(values)], fmt.Sprintf("  key-%d  ", i%11)})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	for _, expression := range []string{
		"COALESCE(v,v)", "NULLIF(v,0)", "IF(id>10,v,NULL)",
		"CASE WHEN id>10 THEN v ELSE NULL END", "LOWER(label)", "CAST(id AS TEXT)", "(id+1)*2",
	} {
		for _, order := range []string{"score ASC NULLS FIRST", "score DESC NULLS LAST,id DESC"} {
			for _, page := range []string{"", " LIMIT 20", " LIMIT 20 OFFSET 237", " LIMIT 300", " LIMIT 0"} {
				query := "SELECT id," + expression + " AS score FROM bound_order ORDER BY " + order + page
				plan, handled, err := buildSimpleSelectPlan(env, mustParse(query).(*Select))
				if err != nil || !handled {
					t.Fatalf("plan %s: %v / %v", query, handled, err)
				}
				got, _, err := executeSimpleSelectOrderedFastPath(env, plan)
				want, _, wantErr := executeSimpleSelectOrderedFastPath(env, rawOrderInterpreterPlan(plan))
				checkRawOrderValueResult(t, got, want, err, wantErr)
			}
		}
	}
	for i, row := range table.Rows {
		if row[0] != i {
			t.Fatalf("source order changed at row %d", i)
		}
	}
}

func TestRawOrderValuesPreserveFilterErrorsAndFallback(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE bound_order (id INT,v INT)")
	execSQL(t, db, "INSERT INTO bound_order VALUES (0,0),(1,1),(2,2)")
	for _, query := range []string{
		"SELECT id,1/v AS score FROM bound_order WHERE id>0 ORDER BY score LIMIT 1",
		"SELECT id,1/v AS score FROM bound_order WHERE id<0 ORDER BY score LIMIT 1",
		"SELECT id,IF(TRUE,v,missing) AS score FROM bound_order ORDER BY score LIMIT 1",
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err != nil {
			t.Fatalf("excluded expression failed: %s: %v", query, err)
		}
	}
	if _, err := Execute(t.Context(), db, "default", mustParse(
		"SELECT id,1/v AS score FROM bound_order ORDER BY score LIMIT 1")); err == nil {
		t.Fatal("an invalid ordering key was hidden by pagination")
	}
}

func TestRawOrderValuesRefreshParametersAndClocks(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE bound_order (id INT,v INT)")
	execSQL(t, db, "INSERT INTO bound_order VALUES (1,1),(2,2),(3,3)")
	statement := mustParse("SELECT id,v*1 AS score FROM bound_order ORDER BY score DESC LIMIT 2").(*Select)
	parameter := statement.Projs[1].Expr.(*Binary).Right.(*Literal)
	parameter.Parameter = true
	for _, multiplier := range []any{1, -1, 0, nil} {
		parameter.Val = multiplier
		env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
		plan, _, err := buildSimpleSelectPlan(env, statement)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := executeSimpleSelectOrderedFastPath(env, plan)
		want, _, wantErr := executeSimpleSelectOrderedFastPath(env, rawOrderInterpreterPlan(plan))
		checkRawOrderValueResult(t, got, want, err, wantErr)
	}
	statement = mustParse("SELECT id,COALESCE(NULL,CAST(NOW() AS TEXT)) AS score,id+1 AS tie FROM bound_order ORDER BY score,tie DESC LIMIT 2").(*Select)
	first := time.Date(2024, 1, 1, 1, 2, 3, 0, time.UTC)
	for _, now := range []time.Time{first, first.Add(24 * time.Hour)} {
		env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", now: now}
		plan, _, err := buildSimpleSelectPlan(env, statement)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := executeSimpleSelectOrderedFastPath(env, plan)
		want, _, wantErr := executeSimpleSelectOrderedFastPath(env, rawOrderInterpreterPlan(plan))
		checkRawOrderValueResult(t, got, want, err, wantErr)
		clockText, _ := castValue(now, "TEXT")
		if len(got.Rows) != 2 || got.Rows[0]["id"] != 3 || got.Rows[0]["score"] != clockText {
			t.Fatal("statement clock or tie ordering changed", got)
		}
	}
}

func TestRawOrderValuesConcurrentSchemas(t *testing.T) {
	var databases []*storage.DB
	for _, schema := range []string{"id INT,v INT", "v INT,id INT"} {
		db := storage.NewDB()
		t.Cleanup(func() { _ = db.Close() })
		execSQL(t, db, "CREATE TABLE bound_order ("+schema+")")
		execSQL(t, db, "INSERT INTO bound_order (id,v) VALUES (1,1),(2,2),(3,3)")
		databases = append(databases, db)
	}
	statement := mustParse("SELECT id,COALESCE(v,0)*2 AS score FROM bound_order ORDER BY score DESC LIMIT 2")
	want := []Row{{"id": 3, "score": 6}, {"id": 2, "score": 4}}
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for range 20 {
				got, err := Execute(t.Context(), databases[worker%2], "default", statement)
				if err != nil || !reflect.DeepEqual(got.Rows, want) {
					t.Errorf("shared ordered statement: %v / %v", got, err)
					return
				}
			}
		})
	}
	workers.Wait()
}
