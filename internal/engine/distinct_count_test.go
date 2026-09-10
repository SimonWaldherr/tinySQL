package engine

import (
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDistinctCountSetMatchesValueText(t *testing.T) {
	values := []any{nil, 1, int64(1), 1.0, "1", 0, math.Copysign(0, -1), "-0", true, "true", false, "false", math.NaN(), math.Float64frombits(0x7ff8000000000001), math.Inf(1), "+Inf", []byte("abc"), []byte(nil), big.NewRat(1, 2), "1/2", map[string]any{"x": 1}}
	var set distinctCountSet
	seen := map[string]bool{}
	for round := 0; round < 2; round++ {
		for _, v := range values {
			want := false
			if v != nil {
				key := valueText(v)
				want = !seen[key]
				seen[key] = true
			}
			if got := set.add(v); got != want {
				t.Fatalf("%T(%v): add=%v want %v", v, v, got, want)
			}
		}
	}
}

func TestDistinctScalarKeysMatchCanonical(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("mixed", []storage.Column{{Name: "v", Type: storage.TextType}}, false)
	values := []any{nil, 1, int64(1), "1", true, "true", 0, false, 1.0, math.Copysign(0, -1), 0.0, math.NaN(), math.Float64frombits(0x7ff8000000000001), []byte("abc"), big.NewRat(1, 2)}
	for round := 0; round < 2; round++ {
		for _, v := range values {
			table.Rows = append(table.Rows, []any{v})
		}
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	rs := execSQL(t, db, `SELECT DISTINCT v FROM mixed`)
	seen := map[string]bool{}
	var want []string
	for _, v := range values {
		key := string(appendDistinctKey(nil, []any{v}))
		if !seen[key] {
			seen[key] = true
			want = append(want, key)
		}
	}
	var got []string
	for _, r := range rs.Rows {
		got = append(got, string(appendDistinctKey(nil, []any{r["v"]})))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DISTINCT: %v want %v", got, want)
	}
	expected := map[string]bool{}
	for _, v := range values {
		if v != nil {
			expected[valueText(v)] = true
		}
	}
	count := execSQL(t, db, `SELECT COUNT(DISTINCT v) AS n FROM mixed`)
	if count.Rows[0]["n"] != len(expected) {
		t.Fatalf("COUNT DISTINCT: %v want %d", count, len(expected))
	}
}

func TestCountDistinctRawSQL(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE counts (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO counts VALUES (1,1),(1,1),(1,2),(1,NULL),(2,2),(2,NULL),(3,NULL)`)
	for _, sql := range []string{
		`SELECT COUNT(DISTINCT v) AS n FROM counts`,
		`SELECT COUNT(DISTINCT v) AS n, COUNT(v) AS total, COUNT(*) AS rows, SUM(v) AS s FROM counts`,
		`SELECT g, COUNT(DISTINCT v) AS n, COUNT(DISTINCT g) AS ng FROM counts GROUP BY g ORDER BY g`,
		`SELECT g, COUNT(DISTINCT v) AS n FROM counts GROUP BY g HAVING COUNT(DISTINCT v)>1 ORDER BY g`,
		`SELECT COUNT(DISTINCT v) AS n FROM counts WHERE g=3`,
		`SELECT COUNT(DISTINCT CASE WHEN v=1 THEN NULL ELSE v END) AS n FROM counts`,
		`SELECT COUNT(DISTINCT v+1) AS n FROM counts HAVING COUNT(DISTINCT v+1)>1`,
		`SELECT g, COUNT(DISTINCT v) AS n FROM counts GROUP BY g ORDER BY g LIMIT 1 OFFSET 1`,
	} {
		stmt := mustParse(sql).(*Select)
		_, ok, err := buildSimpleAggregatePlan(ExecEnv{db: db, tenant: "default"}, stmt)
		if !ok || err != nil {
			t.Fatalf("not optimized: %s: %v", sql, err)
		}
		got := execSQL(t, db, sql)
		want := execSQL(t, db, strings.Replace(sql, "FROM counts", "FROM (SELECT * FROM counts) AS counts", 1))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %v want %v", sql, got, want)
		}
		stream, err := ExecuteStream(t.Context(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		var rows []Row
		for stream.Next() {
			rows = append(rows, stream.Row())
		}
		err = stream.Err()
		stream.Close()
		if err != nil || len(rows) != len(got.Rows) || len(rows) > 0 && !reflect.DeepEqual(rows, got.Rows) {
			t.Fatalf("stream %s: %v %v", sql, rows, err)
		}
	}
	// Empty input still produces the implicit aggregate group, with zero count.
	for _, sql := range []string{
		`SELECT COUNT(DISTINCT v) AS n FROM counts WHERE g=9`,
		`SELECT COUNT(DISTINCT v) AS n FROM counts WHERE g=9 HAVING COUNT(DISTINCT v)=0`,
	} {
		got := execSQL(t, db, sql)
		if len(got.Rows) != 1 || got.Rows[0]["n"] != 0 {
			t.Fatalf("empty aggregate: %v", got)
		}
	}
	if got := execSQL(t, db, `SELECT g, COUNT(DISTINCT v) AS n FROM counts WHERE g=9 GROUP BY g`); len(got.Rows) != 0 {
		t.Fatal(got)
	}
	for _, sql := range []string{
		`SELECT COUNT(v) AS n FROM counts HAVING COUNT(DISTINCT v)=2`,
		`SELECT COUNT(DISTINCT v) AS n FROM counts HAVING COUNT(v)=4`,
	} {
		_, ok, err := buildSimpleAggregatePlan(ExecEnv{db: db, tenant: "default"}, mustParse(sql).(*Select))
		if ok || err != nil {
			t.Fatalf("COUNT and COUNT DISTINCT conflated: %s", sql)
		}
		if len(execSQL(t, db, sql).Rows) != 1 {
			t.Fatal(sql)
		}
	}
	execSQL(t, db, `CREATE INDEX counts_g ON counts(g)`)
	parameterized := mustParse(`SELECT COUNT(DISTINCT v) AS n FROM counts WHERE g=1`).(*Select)
	parameter := parameterized.Where.(*Binary).Right.(*Literal)
	parameter.Parameter = true
	for _, tc := range []struct{ group, count int }{{1, 2}, {2, 1}, {3, 0}, {9, 0}, {1, 2}} {
		parameter.Val = tc.group
		rs, err := Execute(t.Context(), db, "default", parameterized)
		if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["n"] != tc.count {
			t.Fatalf("indexed count: %v %v", rs, err)
		}
	}
	concurrent := mustParse(`SELECT g,COUNT(DISTINCT v) AS n FROM counts GROUP BY g ORDER BY g`)
	want, err := Execute(t.Context(), db, "default", concurrent)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 20; j++ {
				got, err := Execute(t.Context(), db, "default", concurrent)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Errorf("concurrent: %v %v", got, err)
					return
				}
			}
		})
	}
	wg.Wait()
	execSQL(t, db, `INSERT INTO counts VALUES (1,3)`)
	got, err := Execute(t.Context(), db, "default", concurrent)
	if err != nil || got.Rows[0]["n"] != 3 {
		t.Fatalf("stale distinct state: %v %v", got, err)
	}
}
