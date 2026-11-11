package engine

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func generalCTEAggregation(env ExecEnv, stmt *Select) (*ResultSet, error) {
	rows := rowsFromCTEResult(env, env.ctes[strings.ToLower(stmt.From.Table)], stmt.From)
	rows, err := applyWhereClause(env, stmt.Where, rows)
	if err != nil {
		return nil, err
	}
	rows, columns, err := processGroupByHaving(env, stmt, rows)
	if err != nil {
		return nil, err
	}
	rows = applySortOrderWithLimit(stmt.OrderBy, rows, stmt.Limit, stmt.Offset)
	rows = applyOffsetLimit(stmt, rows)
	if len(columns) == 0 {
		columns = columnsFromRows(rows)
	}
	return &ResultSet{Cols: columns, Rows: rows}, nil
}

func TestCTEAggregateMatchesGeneralPipeline(t *testing.T) {
	full := &ResultSet{Cols: []string{"id", "grp", "v"}}
	for i, v := range []any{nil, 1, int64(9007199254740993), 1.5, big.NewRat(1, 2), "2", false, []byte("x")} {
		full.Rows = append(full.Rows, Row{"id": i, "grp": i % 3, "v": v})
	}
	for _, source := range []*ResultSet{
		full, {Cols: full.Cols},
		{Cols: full.Cols, Rows: []Row{{"id": 0, "grp": nil, "v": nil}, {"id": 1, "grp": nil, "v": nil}}},
		{Cols: full.Cols, Rows: []Row{{"id": 0, "grp": 1, "v": 2}, {"id": 1, "grp": 2}}},
		{Cols: []string{"id", "q.v"}, Rows: []Row{{"id": 0, "q.v": 1}}},
	} {
		env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
		for _, query := range []string{
			"SELECT COUNT(*) AS n FROM c q",
			"SELECT COUNT(q.v),SUM(q.v),AVG(c.v),MIN(v),MAX(q.v) FROM c q",
			"SELECT COUNT(NULL) AS n,SUM(2),AVG(1),MIN('x'),MAX(TRUE) FROM c q",
			"SELECT COUNT(*) AS X,COUNT(v) AS x FROM c q",
			"SELECT q.grp,COUNT(*) AS n,SUM(q.v) AS total FROM c q GROUP BY q.grp ORDER BY n DESC,q.grp",
			"SELECT grp,COUNT(v) AS n,AVG(v) AS mean FROM c q WHERE q.id>=1 AND NOT(q.v IS NULL) GROUP BY grp ORDER BY grp",
			"SELECT q.grp,c.id,COUNT(*) FROM c q GROUP BY q.grp,c.id ORDER BY c.id DESC",
			"SELECT grp FROM c GROUP BY grp ORDER BY grp",
			"SELECT q.grp,COUNT(*) FROM c q GROUP BY c.grp",
			"SELECT grp,COUNT(*) FROM c q GROUP BY q.grp",
			"SELECT SUM(missing) FROM c q",
			"SELECT SUM(wrong.v) FROM c q",
			"SELECT COUNT(*) FROM c q WHERE q.id<0 AND missing>0",
			"SELECT SUM(v) FROM c q WHERE q.v>0",
			"SELECT COUNT(*) FROM c q WHERE id IN (1,2,NULL)",
			"SELECT COUNT(DISTINCT v) FROM c q",
			"SELECT COUNT(*) FILTER(WHERE id>0) FROM c q",
			"SELECT SUM(v+1) FROM c q",
			"SELECT COUNT(*) AS n FROM c q HAVING COUNT(*)>0",
		} {
			for _, tail := range []string{"", " LIMIT 0", " LIMIT 1 OFFSET 1"} {
				stmt := mustParse(query + tail).(*Select)
				want, wantErr := generalCTEAggregation(env, stmt)
				got, gotErr := executeSelect(env, stmt)
				if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || (gotErr == nil && (!reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows))) {
					t.Fatalf("%s%s, source=%v: got %#v / %v, want %#v / %v", query, tail, source, got, gotErr, want, wantErr)
				}
			}
		}
	}
}

type cteAggregateStringProbe struct{ calls *int }

func (p cteAggregateStringProbe) String() string { *p.calls++; return "a" }

type cteAggregateJSONProbe struct{ calls *int }

func (p cteAggregateJSONProbe) MarshalJSON() ([]byte, error) { *p.calls++; return []byte(`"g"`), nil }

func TestCTEAggregateDoesNotReplayHostCallbacks(t *testing.T) {
	for _, query := range []string{
		"SELECT SUM(v) FROM c WHERE grp<'a'",
		"SELECT grp,SUM(v) FROM c GROUP BY grp",
	} {
		for _, last := range []Row{{"grp": "x"}, {"grp": 0, "v": 2}} {
			calls := 0
			var value any = cteAggregateStringProbe{&calls}
			if strings.Contains(query, "GROUP BY") {
				value = cteAggregateJSONProbe{&calls}
			}
			source := &ResultSet{Cols: []string{"grp", "v"}, Rows: []Row{{"grp": value, "v": 1}, last}}
			env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
			stmt := mustParse(query).(*Select)
			_, wantErr := generalCTEAggregation(env, stmt)
			wantCalls := calls
			calls = 0
			_, gotErr := executeSelect(env, stmt)
			if calls != wantCalls || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s: %d calls / %v, want %d / %v", query, calls, gotErr, wantCalls, wantErr)
			}
		}
	}
}

func TestCTEAggregateOwnsOutputAndStatement(t *testing.T) {
	source := &ResultSet{Cols: []string{"grp", "v"}, Rows: []Row{{"grp": "a", "v": 1}, {"grp": "a", "v": 2}}}
	env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
	query := "SELECT q.grp AS category,SUM(q.v) AS total FROM c q WHERE c.v>0 GROUP BY q.grp ORDER BY total"
	stmt, original := mustParse(query).(*Select), mustParse(query)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			got, handled, err := executeCTEAggregateFastPath(env, stmt)
			if err != nil || !handled || len(got.Rows) != 1 || expectAsInt(t, got.Rows[0]["total"]) != 3 {
				t.Errorf("aggregate: %#v / %v / %v", got, handled, err)
				return
			}
			got.Rows[0]["category"] = "changed"
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(stmt, original) || source.Rows[0]["grp"] != "a" || len(source.Rows[0]) != 2 {
		t.Fatal("shared statement or materialized rows changed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	env.ctx = ctx
	if _, _, err := executeCTEAggregateFastPath(env, stmt); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled aggregate", err)
	}
}

func TestCTEAggregatePreservesMaterialization(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	got := execSQL(t, db, "WITH c AS (SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 LIMIT 2), d AS (SELECT SUM(q.n) AS total FROM c q), e AS (SELECT n FROM c LIMIT 1) SELECT d.total,e.n FROM d JOIN e ON 1=1")
	if len(got.Rows) != 1 || expectAsInt(t, got.Rows[0]["d.total"]) != 4 || got.Rows[0]["e.n"] != 3 {
		t.Fatal("producer limit or sibling source order changed", got)
	}
}

type cteAggregateParameterProbe struct {
	parameter *Literal
	calls     *int
}

func (p cteAggregateParameterProbe) String() string {
	*p.calls++
	p.parameter.Val = 7
	return "a"
}

func (p cteAggregateParameterProbe) MarshalJSON() ([]byte, error) {
	*p.calls++
	p.parameter.Val = 7
	return []byte(`"g"`), nil
}

func TestCTEAggregateParameterEvaluationOrder(t *testing.T) {
	for _, query := range []string{
		"SELECT SUM(0) AS total FROM c WHERE grp<'z'",
		"SELECT q.grp,SUM(0) AS total FROM c q GROUP BY q.grp",
		"SELECT q.grp,SUM(0) AS total FROM c q GROUP BY c.grp",
	} {
		parameter := &Literal{Parameter: true}
		stmt := mustParse(query).(*Select)
		stmt.Projs[len(stmt.Projs)-1].Expr.(*FuncCall).Args[0] = parameter
		calls := 0
		source := &ResultSet{Cols: []string{"grp"}, Rows: []Row{{"grp": cteAggregateParameterProbe{parameter, &calls}}}}
		env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
		for _, value := range []any{nil, 2, 3.5, big.NewRat(1, 2)} {
			parameter.Val, calls = value, 0
			want, wantErr := generalCTEAggregation(env, stmt)
			wantCalls := calls
			parameter.Val, calls = value, 0
			got, gotErr := executeSelect(env, stmt)
			if calls != wantCalls || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || (gotErr == nil && !reflect.DeepEqual(got, want)) {
				t.Fatalf("%s, parameter %v: %#v / %v (%d calls), want %#v / %v (%d calls)", query, value, got, gotErr, calls, want, wantErr, wantCalls)
			}
		}
	}
}

type cteAggregateRowMutationProbe struct {
	target Row
	calls  *int
}

func (p cteAggregateRowMutationProbe) String() string {
	*p.calls++
	p.target["v"] = 100
	return "g"
}

func (p cteAggregateRowMutationProbe) MarshalJSON() ([]byte, error) {
	*p.calls++
	p.target["v"] = 100
	return []byte(`"g"`), nil
}

func TestCTEAggregateSnapshotsRowsBeforeHostCallbacks(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"group", "SELECT SUM(v) AS total FROM c GROUP BY grp"},
		{"nested-group", "SELECT SUM(v) AS total FROM c GROUP BY grp"},
		{"where-source", "SELECT SUM(v) AS total FROM c WHERE grp<='g'"},
		{"where-literal", "SELECT SUM(v) AS total FROM c WHERE 'g'<=grp"},
		{"aggregate-literal", "SELECT MIN(0) AS smallest,SUM(v) AS total FROM c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			rows := []Row{{"grp": "g", "v": 1}, {"grp": "g", "v": 2}, {"grp": "g", "v": 3}}
			probe := cteAggregateRowMutationProbe{target: rows[2], calls: &calls}
			stmt := mustParse(tc.query).(*Select)
			switch tc.name {
			case "group":
				for _, row := range rows {
					row["grp"] = probe
				}
			case "nested-group":
				for _, row := range rows {
					row["grp"] = map[string]any{"nested": probe}
				}
			case "where-source":
				rows[0]["grp"] = probe
			case "where-literal":
				stmt.Where.(*Binary).Left = &Literal{Val: probe, Parameter: true}
			case "aggregate-literal":
				stmt.Projs[0].Expr.(*FuncCall).Args[0] = &Literal{Val: probe, Parameter: true}
			}
			source := &ResultSet{Cols: []string{"grp", "v"}, Rows: rows}
			env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
			if _, handled, err := executeCTEAggregateFastPath(env, stmt); handled || err != nil || calls != 0 || rows[2]["v"] != 3 {
				t.Fatalf("callback values were evaluated before fallback: handled=%v err=%v calls=%d last=%v", handled, err, calls, rows[2])
			}
			want, wantErr := generalCTEAggregation(env, stmt)
			wantCalls := calls
			if wantErr != nil || wantCalls == 0 || len(want.Rows) != 1 || expectAsInt(t, want.Rows[0]["total"]) != 6 {
				t.Fatalf("reference did not exercise callback snapshot: %v / %v, calls=%d", want, wantErr, wantCalls)
			}
			rows[2]["v"], calls = 3, 0
			got, gotErr := executeSelect(env, stmt)
			if calls != wantCalls || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v / %v (%d calls), want %v / %v (%d calls)", got, gotErr, calls, want, wantErr, wantCalls)
			}
		})
	}
}

func TestCTEAggregateBorrowsStorageValues(t *testing.T) {
	for _, value := range []any{nil, false, 1, int64(2), 3.5, "g", []byte("blob"), []float64{1, 2},
		time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), *big.NewRat(1, 3), big.NewRat(2, 3)} {
		source := &ResultSet{Cols: []string{"v"}, Rows: []Row{{"v": value}, {"v": value}}}
		env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
		stmt := mustParse("SELECT COUNT(v) AS n FROM c GROUP BY v").(*Select)
		got, handled, err := executeCTEAggregateFastPath(env, stmt)
		want, wantErr := generalCTEAggregation(env, stmt)
		if !handled || fmt.Sprint(err) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
			t.Fatalf("storage value %T: handled=%v, got %v / %v, want %v / %v", value, handled, got, err, want, wantErr)
		}
	}
}
