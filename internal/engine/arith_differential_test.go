package engine

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// typed renders a value with its Go type so tests compare representation,
// not only numeric equality: 42 (int) and 42 (float64) must not be equal.
func typed(v any) string {
	if f, ok := v.(float64); ok {
		return "float64:" + strconv.FormatFloat(f, 'g', -1, 64)
	}
	return fmt.Sprintf("%T:%v", v, v)
}

// arithFixture stores raw Go values directly so int, int64 and float64 can
// be mixed exactly as loaded tables and imports may hold them.
func arithFixture(t *testing.T) (*storage.DB, [][]any) {
	t.Helper()
	rows := [][]any{
		{0, 3, 2, 0.5},
		{1, -7, 3, -1.25},
		{2, math.MaxInt64, 1, 2.0},
		{3, math.MinInt64, -1, nil},
		{4, int64(9007199254740993), int64(-9007199254740993), 1e300},
		{5, 0, 7, -0.0},
		{6, nil, 4, 3.5},
		{7, 17, -5, 1.0},
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("nums", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "a", Type: storage.IntType},
		{Name: "b", Type: storage.IntType},
		{Name: "f", Type: storage.FloatType},
	}, false)
	table.Rows = rows
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	return db, rows
}

var arithExpressions = []string{
	"a + b", "a - b", "a * b", "a % b", "a / b", "-a", "+a", "-(-a)", "a + 1", "1 - a",
	"a * 2", "a + f", "f * 2", "a % 3", "-b + a", "(a + b) * (a - b)", "a + b + f", "-9223372036854775807 - 1",
}

// TestArithmeticPathsAgree runs each expression through the generic Row-map
// evaluator (the oracle) and through every optimized path: raw projection,
// raw filters, columnar results, the join fast path, raw aggregates and the
// general GROUP BY path. Values and Go types must match exactly.
func TestArithmeticPathsAgree(t *testing.T) {
	ctx := context.Background()
	db, rows := arithFixture(t)
	env := ExecEnv{ctx: ctx, db: db, tenant: "default"}
	oracle := func(expr Expr, raw []any) (any, error) {
		return evalExpr(env, expr, Row{"id": raw[0], "a": raw[1], "b": raw[2], "f": raw[3]})
	}
	for _, text := range arithExpressions {
		t.Run(text, func(t *testing.T) {
			expr := mustParse("SELECT " + text + " AS v FROM nums").(*Select).Projs[0].Expr
			want := make([]string, len(rows))
			for i, raw := range rows {
				v, err := oracle(expr, raw)
				if err != nil {
					want[i] = "error"
					continue
				}
				want[i] = typed(v)
			}

			// Raw projection.
			if rs, err := Execute(ctx, db, "default", mustParse("SELECT id, "+text+" AS v FROM nums ORDER BY id")); err == nil {
				for i, row := range rs.Rows {
					if got := typed(row["v"]); got != want[i] {
						t.Errorf("projection row %d: got %s, want %s", i, got, want[i])
					}
				}
			} else if !strings.Contains(strings.Join(want, ","), "error") {
				t.Fatalf("projection: %v", err)
			}

			// Columnar results.
			if cs, err := ExecuteColumnar(ctx, db, "default", mustParse("SELECT "+text+" AS v FROM nums")); err == nil {
				for i, v := range cs.Values[0] {
					if got := typed(v); got != want[i] {
						t.Errorf("columnar row %d: got %s, want %s", i, got, want[i])
					}
				}
			}

			// Join fast path: each row joins itself.
			joined := strings.NewReplacer("a", "l.a", "b", "r.b", "f", "l.f").Replace(text)
			if rs, err := Execute(ctx, db, "default", mustParse("SELECT l.id AS id, "+joined+" AS v FROM nums l JOIN nums r ON l.id = r.id ORDER BY l.id")); err == nil {
				for _, row := range rs.Rows {
					id := row["id"].(int)
					if got := typed(row["v"]); got != want[id] {
						t.Errorf("join row %d: got %s, want %s", id, got, want[id])
					}
				}
			}

			// Raw filters: rows equal to each distinct oracle value.
			for i, raw := range rows {
				value, err := oracle(expr, raw)
				if err != nil || value == nil {
					continue
				}
				literal := typedLiteral(value)
				where := mustParse("SELECT id FROM nums WHERE " + text + " = " + literal).(*Select).Where
				var wantIDs []int
				for j, other := range rows {
					match, err := evalExpr(env, where, Row{"id": other[0], "a": other[1], "b": other[2], "f": other[3]})
					if err == nil && toTri(match) == tvTrue {
						wantIDs = append(wantIDs, j)
					}
				}
				rs, err := Execute(ctx, db, "default", mustParse("SELECT id FROM nums WHERE "+text+" = "+literal+" ORDER BY id"))
				if err != nil {
					t.Fatalf("filter %s = %s: %v", text, literal, err)
				}
				var gotIDs []int
				for _, row := range rs.Rows {
					gotIDs = append(gotIDs, row["id"].(int))
				}
				if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
					t.Errorf("filter row %d (%s = %s): got ids %v, want %v", i, text, literal, gotIDs, wantIDs)
				}
			}

			// Aggregates: the raw fast path, GROUP BY, and the generic evaluator.
			sumExpr := &FuncCall{Name: "SUM", Args: []Expr{expr}}
			var all []Row
			for _, raw := range rows {
				all = append(all, Row{"id": raw[0], "a": raw[1], "b": raw[2], "f": raw[3]})
			}
			wantSum, sumErr := evalAggregate(env, sumExpr, all)
			if sumErr != nil {
				return
			}
			for _, sql := range []string{
				"SELECT SUM(" + text + ") AS v FROM nums",
				"SELECT id - id AS g, SUM(" + text + ") AS v FROM nums GROUP BY id - id",
				"SELECT COUNT(*) AS n, SUM(" + text + ") AS v FROM nums HAVING COUNT(*) > 0",
			} {
				rs, err := Execute(ctx, db, "default", mustParse(sql))
				if err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
				if got := typed(rs.Rows[0]["v"]); got != typed(wantSum) {
					t.Errorf("%s: got %s, want %s", sql, got, typed(wantSum))
				}
			}
		})
	}
}

// typedLiteral renders an oracle value as SQL text that parses back to the
// same value and type.
func typedLiteral(v any) string {
	switch x := v.(type) {
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return "NULL"
		}
		s := strconv.FormatFloat(math.Abs(x), 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		if x < 0 || (x == 0 && math.Signbit(x)) {
			return "-" + s
		}
		return s
	case int:
		if x < 0 {
			return fmt.Sprintf("(%d - 1 + 1)", x) // keeps -2^63 representable
		}
		return strconv.Itoa(x)
	case int64:
		return typedLiteral(int(x))
	}
	return fmt.Sprint(v)
}

// The columnar batch aggregator engages at 2048+ rows and sums direct
// columns in bulk. It must match the scalar accumulator exactly, including
// the integer-to-REAL switch on overflow and mixed int/REAL columns.
func TestBatchSumMatchesScalarAccumulator(t *testing.T) {
	ctx := context.Background()
	const n = 5000
	rows := make([][]any, n)
	for i := range rows {
		var mixed any = i
		if i%7 == 0 {
			mixed = float64(i) + 0.25
		}
		var overflow any = int64(math.MaxInt64 / 3000)
		if i%11 == 0 {
			overflow = nil
		}
		rows[i] = []any{i, i - n/2, mixed, overflow, float64(i) / 8}
	}
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("big", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "small", Type: storage.IntType},
		{Name: "mixed", Type: storage.IntType},
		{Name: "huge", Type: storage.IntType},
		{Name: "real", Type: storage.FloatType},
	}, false)
	table.Rows = rows
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	env := ExecEnv{ctx: ctx, db: db, tenant: "default"}
	all := make([]Row, n)
	for i, raw := range rows {
		all[i] = Row{"id": raw[0], "small": raw[1], "mixed": raw[2], "huge": raw[3], "real": raw[4]}
	}
	for _, column := range []string{"small", "mixed", "huge", "real"} {
		for _, fn := range []string{"SUM", "AVG"} {
			want, err := evalAggregate(env, &FuncCall{Name: fn, Args: []Expr{&VarRef{Name: column}}}, all)
			if err != nil {
				t.Fatal(err)
			}
			for _, sql := range []string{
				fmt.Sprintf("SELECT %s(%s) AS v FROM big", fn, column),
				fmt.Sprintf("SELECT %s(%s) AS v, COUNT(%s) AS c FROM big WHERE id >= 0", fn, column, column),
				fmt.Sprintf("SELECT id %% 1 AS g, %s(%s) AS v FROM big GROUP BY id %% 1", fn, column),
			} {
				rs, err := Execute(ctx, db, "default", mustParse(sql))
				if err != nil {
					t.Fatal(err)
				}
				if got := typed(rs.Rows[0]["v"]); got != typed(want) {
					t.Errorf("%s: got %s, want %s", sql, got, typed(want))
				}
			}
		}
	}
	// The integer column overflows int64 part-way and must become REAL.
	rs, err := Execute(ctx, db, "default", mustParse("SELECT SUM(huge) AS v, SUM(small) AS s FROM big"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rs.Rows[0]["v"].(float64); !ok {
		t.Fatalf("overflowing SUM should be REAL, got %T", rs.Rows[0]["v"])
	}
	if want := (n - 1) * n / 2; rs.Rows[0]["s"] != want-n*(n/2) {
		t.Fatalf("SUM(small) = %#v", rs.Rows[0]["s"])
	}
}

func TestIntegerArithmeticSemantics(t *testing.T) {
	db := storage.NewDB()
	for _, tc := range []struct {
		expr string
		want any
	}{
		{"1 + 41", 42}, {"7 * 6", 42}, {"50 - 8", 42}, {"-5", -5}, {"+5", 5}, {"-(-5)", 5},
		{"7 % 3", 1}, {"-7 % 3", -1}, {"84 / 2", 42.0}, {"7 / 2", 3.5},
		{"9223372036854775807 + 0", math.MaxInt64},
		{"9223372036854775807 + 1", 9223372036854775808.0},
		{"-9223372036854775807 - 1", math.MinInt64},
		{"-9223372036854775807 - 2", -9223372036854775809.0},
		{"-(-9223372036854775807 - 1)", 9223372036854775808.0},
		{"4611686018427387904 * 2", 9223372036854775808.0},
		{"(-9223372036854775807 - 1) % -1", 0},
		{"1.5 + 1", 2.5}, {"2 * 0.5", 1.0},
	} {
		if got := queryScalar(t, db, tc.expr); typed(got) != typed(tc.want) {
			t.Errorf("%s = %s, want %s", tc.expr, typed(got), typed(tc.want))
		}
	}
	execSQL(t, db, "CREATE TABLE t (a INT, f FLOAT)")
	execSQL(t, db, "INSERT INTO t VALUES (2, 0.5), (4, NULL), (NULL, 1.5)")
	for sql, want := range map[string]any{
		"SELECT SUM(a) AS v FROM t":     6,
		"SELECT SUM(f) AS v FROM t":     2.0,
		"SELECT SUM(a + f) AS v FROM t": 2.5,
		"SELECT AVG(a) AS v FROM t":     3.0,
		"SELECT SUM(a) + 1 AS v FROM t": 7,
		"SELECT -SUM(a) AS v FROM t":    -6,
	} {
		rs := execSQL(t, db, sql)
		if got := rs.Rows[0]["v"]; typed(got) != typed(want) {
			t.Errorf("%s = %s, want %s", sql, typed(got), typed(want))
		}
	}
	// A computed INT expression used as an index seek key stays exact.
	execSQL(t, db, "CREATE TABLE k (id INT PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO k VALUES (9007199254740992), (9007199254740993)")
	if rs := execSQL(t, db, "SELECT id FROM k WHERE id = 9007199254740992 + 1"); len(rs.Rows) != 1 || rs.Rows[0]["id"] != 9007199254740993 {
		t.Fatalf("exact seek: %#v", rs.Rows)
	}
}
