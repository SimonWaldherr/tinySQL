package engine

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRemainingScalarRawMatchesGeneral(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, sql := range []string{`TRIM(v)`, `LTRIM(v)`, `RTRIM(v)`, `TRIM(v,1/0)`, `TRIM(v,42)`, `TRIM(v,'x')`, `CONCAT_WS(v,1/0)`, `COLUMNS_TO_TEXT(v,1/0)`, `CONCAT_WS(';',v,NULL,'',v)`, `CONCAT_WS(';',v)`, `CONCAT_WS(';',NULL,NULL)`, `JSON_GET(v,'a')`, `JSON_EXTRACT(v,1/0)`, `JSON_GET(v)`, `CONCAT_WS()`} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
		for _, v := range []any{nil, "", 1, "text", "  x  ", "\u2003x\u2003", map[string]any{"a": 1}} {
			got, err := evalRawExpr(plan, []any{v}, expr)
			want, werr := evalExpr(ExecEnv{}, expr, Row{"v": v})
			if !reflect.DeepEqual(got, want) || (err != nil) != (werr != nil) || err != nil && err.Error() != werr.Error() {
				t.Fatalf("%s %v: %v/%v want %v/%v", sql, v, got, err, want, werr)
			}
		}
	}
}

func TestRawScratchReleasesArguments(t *testing.T) {
	for _, n := range []int{3, 300} {
		sc := &rawCallScratch{args: make([]Expr, n), lits: make([]Literal, n)}
		for i := range sc.args {
			sc.lits[i].Val = map[string]any{"payload": strings.Repeat("x", 1024)}
			sc.args[i] = &sc.lits[i]
		}
		sc.call = FuncCall{Name: "TRIM", Args: sc.args}
		sc.reset()
		if sc.call.Args != nil || sc.call.Name != "" {
			t.Fatal("retained call")
		}
		for i := range sc.args {
			if sc.args[i] != nil || sc.lits[i].Val != nil {
				t.Fatal("retained value")
			}
		}
		if n > 256 && (cap(sc.args) != 0 || cap(sc.lits) != 0) {
			t.Fatal("retained oversized buffers")
		}
	}
}

func TestJSONSetSelectDoesNotMutateStorage(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE docs (id INT, doc JSON)`)
	execSQL(t, db, `INSERT INTO docs VALUES (1,'{"a":{"v":1},"items":[{"v":2}]}')`)
	stmt := mustParse(`SELECT JSON_SET(doc,'a.v',3) AS changed, JSON_SET(doc,'items.0.v',4) AS other FROM docs`)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 20; j++ {
				rs, err := Execute(t.Context(), db, "default", stmt)
				if err != nil {
					t.Error(err)
					return
				}
				if !rawEqual(jsonGet(rs.Rows[0]["changed"], "a.v"), 3) || !rawEqual(jsonGet(rs.Rows[0]["other"], "items.0.v"), 4) {
					t.Error("missing update")
				}
			}
		})
	}
	wg.Wait()
	rs := execSQL(t, db, `SELECT JSON_GET(doc,'a.v') AS a, JSON_GET(doc,'items.0.v') AS b FROM docs`)
	if !rawEqual(rs.Rows[0]["a"], 1) || !rawEqual(rs.Rows[0]["b"], 2) {
		t.Fatalf("SELECT mutated storage: %v", rs)
	}
	execSQL(t, db, `UPDATE docs SET doc=JSON_SET(doc,'items.1',5)`)
	rs = execSQL(t, db, `SELECT JSON_GET(doc,'items.1') AS v FROM docs`)
	if !rawEqual(rs.Rows[0]["v"], 5) {
		t.Fatal(rs)
	}
}

func TestJSONLinesLargeRecord(t *testing.T) {
	value := strings.Repeat("x", 200000)
	for _, suffix := range []string{"", "\n"} {
		rs, err := parseJSONLinesToTable(`{"payload":"` + value + `"}` + suffix)
		if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["payload"] != value {
			t.Fatalf("large record: %v", err)
		}
	}
}
