package engine

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAggregateBoundValuesMatchGeneral(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows [][]any
	}{
		{"empty", nil},
		{"nulls", [][]any{{nil, 2}, {3, nil}, {nil, nil}}},
		{"integer", [][]any{{2, 3}, {7, 4}, {-2, 3}, {2, 3}}},
		{"overflow", [][]any{{int64(math.MaxInt64), 2}, {int64(math.MinInt64), 1}, {4, 2}}},
		{"float", [][]any{{0.5, 2}, {math.NaN(), 1}, {math.Inf(1), 0}}},
		{"decimal", [][]any{{big.NewRat(1, 3), big.NewRat(2, 3)}, {big.NewRat(2, 7), big.NewRat(3, 7)}}},
		{"text", [][]any{{"12", 2}, {"bad", 1}, {nil, 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("bound_values", []storage.Column{{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.IntType}}, false)
			table.Rows = tc.rows
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			rows := make([]Row, len(tc.rows))
			for i, row := range tc.rows {
				rows[i] = Row{"a": row[0], "b": row[1]}
			}
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			for _, function := range []string{"COUNT(%s)", "COUNT(DISTINCT %s)", "SUM(%s)", "AVG(%s)", "MIN(%s)", "MAX(%s)"} {
				for _, expression := range []string{"(a + b) * 2", "(a + b) * (a - b)", "a / (b + 1)", "a % (b + 1)", "CAST(a AS FLOAT) + b * 2", "TRY_CONVERT(INT,a) + b * 2"} {
					call := fmt.Sprintf(function, expression)
					query := "SELECT " + call + " AS value FROM bound_values"
					statement := mustParse(query).(*Select)
					want, wantErr := evalAggregate(env, statement.Projs[0].Expr.(*FuncCall), rows)
					got, used, err := executeSimpleAggregateFastPath(env, statement)
					if !used {
						t.Fatal("aggregate fast path declined", query)
					}
					if fmt.Sprint(err) != fmt.Sprint(wantErr) {
						t.Fatalf("%s: error %v, want %v", query, err, wantErr)
					}
					if err == nil && (len(got.Rows) != 1 || !conversionValuesEqual(got.Rows[0]["value"], want)) {
						t.Fatalf("%s: got %v, want %v", query, got, want)
					}
				}
			}
		})
	}
}

func TestAggregateBoundValuesKeepFiltersAndErrors(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE bound_filter (id INT, a INT, b INT)")
	execSQL(t, db, "INSERT INTO bound_filter VALUES (1,10,2),(2,20,0),(3,NULL,3)")
	for _, query := range []string{
		"SELECT SUM((a + 1) / b) AS value FROM bound_filter WHERE id <> 2",
		"SELECT SUM((a + 1) / b) FILTER (WHERE id <> 2) AS value FROM bound_filter",
	} {
		if got := execSQL(t, db, query); got.Rows[0]["value"] != 5.5 {
			t.Fatal("filter changed aggregate input", query, got)
		}
	}
	for _, query := range []string{
		"SELECT SUM((a + 1) / b) FILTER (WHERE FALSE) FROM bound_filter",
		"SELECT MIN((a + 1) / b) FROM bound_filter WHERE FALSE",
		"SELECT COUNT((a + 1) / b) FROM bound_filter WHERE FALSE",
		"SELECT MIN((a + 1) / missing) FROM bound_filter WHERE FALSE",
	} {
		execSQL(t, db, query)
	}
	for _, query := range []string{
		"SELECT SUM((a + 1) / b) FROM bound_filter",
		"SELECT COUNT((a + 1) / b) FROM bound_filter",
		"SELECT MIN((a + 1) / missing) FROM bound_filter",
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatal("aggregate argument error was lost", query)
		}
	}
	clock := execSQL(t, db, "SELECT MIN(CAST(NOW() AS TEXT)) AS earliest, MAX(CAST(NOW() AS TEXT)) AS latest FROM bound_filter")
	if earliest, ok := clock.Rows[0]["earliest"].(string); !ok || earliest == "" || earliest != clock.Rows[0]["latest"] {
		t.Fatal("aggregate arguments lost the statement clock", clock)
	}
	table, _ := db.Get("default", "bound_filter")
	table.Rows = [][]any{{1, nil}}
	table.Version++
	if _, err := Execute(t.Context(), db, "default", mustParse("SELECT SUM((a + b) * 2) FROM bound_filter")); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatal("a NULL left operand hid the short-row error", err)
	}
}

func TestAggregateBoundValuesRefreshParameters(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE bound_parameters (a INT)")
	execSQL(t, db, "INSERT INTO bound_parameters VALUES (2),(4),(NULL)")
	statement := mustParse("SELECT SUM((a + 2) * 3) AS value FROM bound_parameters").(*Select)
	argument := statement.Projs[0].Expr.(*FuncCall).Args[0].(*Binary)
	parameter := argument.Left.(*Binary).Right.(*Literal)
	parameter.Parameter = true
	for _, tc := range []struct{ value, want any }{{1, 24}, {7, 60}, {nil, nil}, {0.5, 21.0}} {
		parameter.Val = tc.value
		got, err := Execute(t.Context(), db, "default", statement)
		if err != nil {
			t.Fatal(err)
		}
		if !conversionValuesEqual(got.Rows[0]["value"], tc.want) {
			t.Fatalf("parameter %v: got %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestAggregateBoundValuesSharedStatementAcrossSchemas(t *testing.T) {
	statement := mustParse("SELECT SUM((a + b) * 2) AS total, MIN(CAST(a AS FLOAT) + b * 2) AS smallest FROM bound_shared")
	var group sync.WaitGroup
	for worker := range 8 {
		group.Go(func() {
			db := storage.NewDB()
			defer db.Close()
			columns := []storage.Column{{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.IntType}}
			if worker%2 != 0 {
				columns[0], columns[1] = columns[1], columns[0]
			}
			table := storage.NewTable("bound_shared", columns, false)
			if err := db.Put("default", table); err != nil {
				t.Error(err)
				return
			}
			for execution := range 20 {
				row := []any{execution, 3}
				if worker%2 != 0 {
					row[0], row[1] = row[1], row[0]
				}
				table.Rows = [][]any{row}
				table.Version++
				got, err := Execute(t.Context(), db, "default", statement)
				if err != nil || got.Rows[0]["total"] != 2*(execution+3) || got.Rows[0]["smallest"] != float64(execution+6) {
					t.Errorf("worker %d, execution %d: %v / %v", worker, execution, got, err)
					return
				}
			}
		})
	}
	group.Wait()
}
