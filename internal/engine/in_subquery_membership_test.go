package engine

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestInSubqueryMembershipMatchesComparisons(t *testing.T) {
	type namedInteger int64
	instant := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	probes := []any{nil, int(0), int(-1), int64(1), int64(9007199254740992), int64(9007199254740993), int64(math.MaxInt64), int64(math.MaxInt64 - 1), int64(math.MinInt64), int64(math.MinInt64 + 1), float64(9007199254740992), math.NaN(), math.Inf(1), math.Inf(-1), 2.5, "", "1", "true", "é漢", true, false, []byte("abc"), []byte(nil), instant, instant.In(time.FixedZone("other", 3600)), namedInteger(1)}
	for _, tc := range []struct {
		name   string
		values []any
	}{
		{"empty", nil},
		{"null", []any{nil}},
		{"integers", []any{0, 1, int64(1), int64(9007199254740992), int64(9007199254740993), int64(math.MaxInt64), int64(math.MinInt64), nil}},
		{"single-wide-integer", []any{int64(9007199254740993)}},
		{"single-wide-real", []any{float64(9007199254740992)}},
		{"reals", []any{0.0, math.Copysign(0, -1), 1.0, 2.5, math.Inf(1), math.Inf(-1), nil}},
		{"nan", []any{math.NaN(), "text", nil}},
		{"text", []any{"", "1", "true", "é漢", nil}},
		{"bool", []any{true, false, nil}},
		{"fallback", []any{[]byte("abc"), []byte(nil), big.NewRat(1, 2), instant, namedInteger(1), nil}},
		{"mixed", []any{1, int64(9007199254740993), 2.5, "1", true, []byte("abc"), big.NewRat(1, 2), instant, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var candidates []any
			for repeat := 0; repeat < 20; repeat++ {
				candidates = append(candidates, tc.values...)
			}
			rs := &ResultSet{Cols: []string{"V"}}
			for _, candidate := range candidates {
				rs.Rows = append(rs.Rows, Row{"v": candidate, "source.v": candidate})
			}
			set, err := buildInSubquerySet(ExecEnv{ctx: t.Context()}, rs, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, probe := range probes {
				// The existing comparison kernel is the independent oracle,
				// including rounded numeric comparisons and NaN behavior.
				want := tvFalse
				for _, candidate := range candidates {
					comparison, err := evalComparisonBinary("=", probe, candidate)
					if err == nil {
						want = triOr(want, toTri(comparison))
					}
				}
				for _, negate := range []bool{false, true} {
					owner := &InExpr{Negate: negate}
					env := ExecEnv{ctx: t.Context(), subqueryCache: &subqueryResultCache{entries: map[Expr]*subqueryCacheEntry{owner: {ready: true, inSet: set}}}}
					got, err := evalInSubquery(env, probe, owner, &SubqueryExpr{})
					expected := want
					if negate {
						expected = triNot(expected)
					}
					if err != nil || !reflect.DeepEqual(got, triToValue(expected)) {
						t.Fatalf("probe %T(%v), negate=%v: got %v, %v; want %v", probe, probe, negate, got, err, triToValue(expected))
					}
				}
			}
		})
	}
}

func TestInSubqueryMembershipCacheOwnsCompactValues(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE candidates (v INT)`)
	for i := 0; i < 64; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO candidates VALUES (%d)`, i))
	}
	selectExpr := &SubqueryExpr{Select: mustParseSelect(t, `SELECT v FROM candidates`)}
	owner := &InExpr{Expr: newVarRef("probe"), Values: []Expr{selectExpr}, Subquery: true}
	env := newSubqueryCacheTestEnv(db)
	for i := 0; i < 100; i++ {
		got, err := evalExpr(env, owner, Row{"probe": i})
		if err != nil || got != (i < 64) {
			t.Fatalf("probe %d: got %v, %v", i, got, err)
		}
	}
	entry := env.subqueryCache.entries[owner]
	if entry == nil || entry.execCount != 1 || entry.rs != nil || entry.inSet == nil || !entry.inSet.indexed || len(entry.inSet.integers) != 64 {
		t.Fatalf("expected one execution and a compact indexed set, got %#v", entry)
	}
	// A shared SubqueryExpr in a manually built AST must retain its scalar
	// cardinality checks, independently of the compact IN-owned cache entry.
	if _, err := evalSubqueryExpr(env, selectExpr); err == nil || !strings.Contains(err.Error(), "scalar subquery returned 64 rows") {
		t.Fatalf("shared scalar subquery: got %v, want cardinality error", err)
	}
	if len(env.subqueryCache.entries) != 2 {
		t.Fatal("scalar and set uses shared a cache entry")
	}
	stmt := mustParse(`SELECT 42 IN (SELECT v FROM candidates WHERE v < 60) AS result`).(*Select)
	parameter := stmt.Projs[0].Expr.(*InExpr).Values[0].(*SubqueryExpr).Select.Where.(*Binary).Right.(*Literal)
	parameter.Parameter = true
	for _, limit := range []int{10, 60, 30, 60} {
		parameter.Val = limit
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || rs.Rows[0]["result"] != (limit > 42) {
			t.Fatalf("rebound limit %d: got %#v, %v", limit, rs, err)
		}
	}
	execSQL(t, db, `DELETE FROM candidates WHERE v = 42`)
	rs, err := Execute(t.Context(), db, "default", stmt)
	if err != nil || rs.Rows[0]["result"] != false {
		t.Fatalf("reused statement after mutation: got %#v, %v", rs, err)
	}

	// Decimal probes use compare's numeric/text conversion instead of an
	// incompatible float64 or native-map equality shortcut.
	for _, probe := range []any{big.NewRat(1, 1), big.NewRat(1, 2)} {
		got, err := evalExpr(env, &InExpr{Expr: &Literal{Val: probe}, Values: owner.Values, Subquery: true}, nil)
		if err != nil || got != (probe.(*big.Rat).Denom().Int64() == 1) {
			t.Fatalf("decimal probe %v: got %v, %v", probe, got, err)
		}
	}
}

func TestInSubqueryMembershipCorrelationBypassesCache(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE candidates (v INT)`)
	execSQL(t, db, `INSERT INTO candidates VALUES (1)`)
	inner := &SubqueryExpr{Select: mustParseSelect(t, `SELECT v FROM candidates WHERE v = outer.id`)}
	owner := &InExpr{Expr: &Literal{Val: 1}, Values: []Expr{inner}, Subquery: true}
	env := newSubqueryCacheTestEnv(db)
	for repeat := 0; repeat < 3; repeat++ {
		if _, err := evalExpr(env, owner, nil); err == nil {
			t.Fatal("expected unresolved correlation error")
		}
	}
	entry := env.subqueryCache.entries[owner]
	if entry == nil || !entry.correlated || entry.execCount != 3 || entry.ready || entry.inSet != nil {
		t.Fatalf("correlated set was reused: %#v", entry)
	}
}

func TestInSubqueryMembershipTriggerBypassesCache(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE parent (id INT)`)
	execSQL(t, db, `CREATE TABLE matched (id INT)`)
	execSQL(t, db, `CREATE TRIGGER selected AFTER INSERT ON parent FOR EACH ROW
		WHEN (2 IN (SELECT CASE WHEN 1 = 1 THEN id ELSE 0 END))
		BEGIN INSERT INTO matched VALUES (id); END`)
	execSQL(t, db, `INSERT INTO parent VALUES (1), (2), (3)`)
	rs := execSQL(t, db, `SELECT id FROM matched`)
	if !reflect.DeepEqual(rs.Rows, []Row{{"id": 2}}) {
		t.Fatalf("trigger cache leaked between firings: %#v", rs.Rows)
	}
}

func TestInSubqueryMembershipBuildCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := buildInSubquerySet(ExecEnv{ctx: ctx}, &ResultSet{Cols: []string{"v"}, Rows: []Row{{"v": 1}}}, true)
	if err != context.Canceled {
		t.Fatalf("got %v, want cancellation", err)
	}
}

func TestInSubqueryMembershipLazyRealIndexCancellation(t *testing.T) {
	set := &inSubquerySet{hasRows: true, indexed: true, integers: make(map[int64]struct{})}
	for i := int64(0); i < 1024; i++ {
		set.integers[i] = struct{}{}
	}
	owner := &InExpr{Subquery: true}
	cache := &subqueryResultCache{entries: map[Expr]*subqueryCacheEntry{owner: {ready: true, inSet: set}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := evalInSubquery(ExecEnv{ctx: ctx, subqueryCache: cache}, float64(42), owner, &SubqueryExpr{}); err != context.Canceled {
		t.Fatalf("got %v, want lazy-build cancellation", err)
	}
	if set.integerReals != nil {
		t.Fatal("aborted lazy build published an incomplete index")
	}
	got, err := evalInSubquery(ExecEnv{ctx: t.Context(), subqueryCache: cache}, float64(42), owner, &SubqueryExpr{})
	if err != nil || got != true || len(set.integerReals) != len(set.integers) {
		t.Fatalf("retry after cancellation: got %v, %v, index size %d", got, err, len(set.integerReals))
	}
}

func TestInSubqueryMembershipConcurrentExecutions(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("candidates", []storage.Column{{Name: "v", Type: storage.IntType}}, false)
	for i := 0; i < 64; i++ {
		table.Rows = append(table.Rows, []any{i})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	// A REAL probe also exercises lazy construction of rounded integer keys.
	stmt := mustParse(`SELECT 42.0 IN (SELECT v FROM candidates) AS result`)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for repeat := 0; repeat < 10; repeat++ {
				rs, err := Execute(t.Context(), db, "default", stmt)
				if err != nil || rs.Rows[0]["result"] != true {
					t.Errorf("shared AST: got %#v, %v", rs, err)
					return
				}
			}
		})
	}
	wg.Wait()
}
