package engine

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAggregateBinaryMatchesGeneral(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows [][]any
	}{
		{"empty", nil},
		{"null", [][]any{{nil, nil}, {nil, 3}, {4, nil}}},
		{"integers", [][]any{{2, 3}, {int64(9007199254740993), int64(-9007199254740992)}, {-7, 11}}},
		{"overflow", [][]any{{int64(math.MaxInt64), 2}, {int64(math.MinInt64), -1}, {4, 9}}},
		{"mixed", [][]any{{7, 0.5}, {-2.5, int64(3)}, {int64(5), 6}, {nil, 8}}},
		{"rounding", [][]any{{-1.0, 1.0}, {1 + math.Ldexp(1, -27), 1 - math.Ldexp(1, -27)}}},
		{"nonfinite", [][]any{{math.Inf(1), 0.0}, {math.Inf(-1), 3}, {math.NaN(), 1}}},
		{"decimal", [][]any{{2, 3}, {big.NewRat(1, 3), 2}, {7, 0.5}, {4, 5}, {nil, 3}}},
		{"decimal-right", [][]any{{2, 3}, {2, big.NewRat(1, 3)}, {7, 0.5}, {4, 5}}},
		{"strings", [][]any{{2, 3}, {"4", 2}, {nil, "tail"}, {5, 6}}},
		{"invalid", [][]any{{2, 3}, {true, 2}, {5, 6}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("t", []storage.Column{{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.FloatType}}, false)
			table.Rows = tc.rows
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			env := ExecEnv{ctx: context.Background(), db: db, tenant: "default"}
			rows := make([]Row, len(tc.rows))
			for i, row := range tc.rows {
				rows[i] = Row{"a": row[0], "b": row[1]}
			}
			for _, fn := range []string{"SUM", "AVG"} {
				for _, expr := range []string{"a + b", "a - b", "a * b", "a * 2", "2 - a", "b + 0.5", "NULL + b", "a * (b + 1)", "a / b", "a % b"} {
					query := fmt.Sprintf("SELECT %s(%s) AS v FROM t", fn, expr)
					stmt := mustParse(query).(*Select)
					want, wantErr := evalAggregate(env, stmt.Projs[0].Expr.(*FuncCall), rows)
					got, used, err := executeSimpleAggregateFastPath(env, stmt)
					if !used {
						t.Fatalf("fast path declined %s", query)
					}
					if (err == nil) != (wantErr == nil) || (err != nil && err.Error() != wantErr.Error()) {
						t.Fatalf("%s: error %v, want %v", query, err, wantErr)
					}
					if err == nil && (len(got.Rows) != 1 || typed(got.Rows[0]["v"]) != typed(want)) {
						t.Fatalf("%s: got %v, want %s", query, got.Rows, typed(want))
					}
				}
			}
		})
	}
}

func TestAggregateBinaryGroupsAndErrors(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, "CREATE TABLE t (g TEXT, a INT, b FLOAT)")
	execSQL(t, db, "INSERT INTO t VALUES ('x', 2, 0.5), ('x', 4, 2.5), ('y', NULL, 2), ('z', 7, 1.5)")
	for _, expr := range []string{"t.a + t.b", "a - b", "a * b"} {
		query := "SELECT g, SUM(" + expr + ") AS s, AVG(" + expr + ") AS a FROM t GROUP BY g"
		fast := execSQL(t, db, query+" ORDER BY g")
		general := execSQL(t, db, query+" HAVING LENGTH('x') > 0 ORDER BY g")
		if fmt.Sprint(fast.Rows) != fmt.Sprint(general.Rows) {
			t.Fatalf("%s: fast %v, general %v", query, fast.Rows, general.Rows)
		}
	}
	// Binding must not turn missing operands into NULL or suppress their errors
	// when the other operand is NULL. Empty input still evaluates no arguments.
	for _, query := range []string{"SELECT SUM(NULL * missing) FROM t", "SELECT AVG(missing + a) FROM t"} {
		if _, err := Execute(context.Background(), db, "default", mustParse(query)); err == nil {
			t.Fatalf("expected missing-column error: %s", query)
		}
	}
	table, err := db.Get("default", "t")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows = [][]any{{"x", nil}}
	if _, err := Execute(context.Background(), db, "default", mustParse("SELECT SUM(a * b) FROM t")); err == nil {
		t.Fatal("expected out-of-range column error after NULL left operand")
	}
}

func TestAggregateBinarySharedStatement(t *testing.T) {
	stmt := mustParse("SELECT SUM(a * b) AS s, AVG(a + 2) AS v FROM t")
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			db := storage.NewDB()
			defer db.Close()
			columns := []storage.Column{{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.IntType}}
			if worker%2 != 0 {
				columns[0], columns[1] = columns[1], columns[0]
			}
			table := storage.NewTable("t", columns, false)
			if err := db.Put("default", table); err != nil {
				t.Error(err)
				return
			}
			for execution := 1; execution <= 20; execution++ {
				row := []any{execution, 3}
				if worker%2 != 0 {
					row[0], row[1] = row[1], row[0]
				}
				table.Rows = [][]any{row}
				rs, err := Execute(context.Background(), db, "default", stmt)
				if err != nil {
					t.Error(err)
					return
				}
				if rs.Rows[0]["s"] != execution*3 || rs.Rows[0]["v"] != float64(execution+2) {
					t.Errorf("worker %d, execution %d: %v", worker, execution, rs.Rows)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}
