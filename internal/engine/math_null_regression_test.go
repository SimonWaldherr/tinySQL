package engine

import (
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestMathFunctionsPropagateNull(t *testing.T) {
	for _, expr := range []string{
		"MOD(NULL, 2)", "MOD(2, NULL)",
		"POWER(NULL, 2)", "POWER(2, NULL)",
		"SQRT(NULL)", "LOG(NULL)", "LOG(NULL, 2)", "LOG(2, NULL)",
		"LN(NULL)", "LOG10(NULL)", "LOG2(NULL)", "EXP(NULL)", "SIGN(NULL)",
		"TRUNCATE(NULL, 2)", "TRUNCATE(2, NULL)",
		"SIN(NULL)", "COS(NULL)", "TAN(NULL)", "ASIN(NULL)", "ACOS(NULL)", "ATAN(NULL)",
		"ATAN2(NULL, 1)", "ATAN2(1, NULL)", "DEGREES(NULL)", "RADIANS(NULL)",
	} {
		t.Run(expr, func(t *testing.T) {
			if got := queryScalar(t, storage.NewDB(), expr); got != nil {
				t.Fatalf("%s = %#v, want NULL", expr, got)
			}
		})
	}
}

func TestDecimalScalingPreservesRounding(t *testing.T) {
	// Keep the general-exponent reference to check that the optimized powers
	// preserve rounding, including at decimal boundaries and for subnormals.
	values := []float64{0, math.Copysign(0, -1), 1.005, -1.005, 1.235, 1234.56789,
		math.SmallestNonzeroFloat64, 1.1e-310, 1e308, math.MaxFloat64}
	for precision := -308; precision <= 323; precision++ {
		mult := math.Pow(10, float64(min(precision, 308)))
		extra := math.Pow(10, float64(max(precision-308, 0)))
		for _, op := range []func(float64) float64{math.Round, math.Trunc} {
			for _, value := range values {
				scaled := value * mult * extra
				want := value
				if !math.IsInf(scaled, 0) {
					want = (op(scaled) / extra) / mult
				}
				if got := scaleDecimal(value, precision, op); math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("scaleDecimal(%g, %d) = %g, want %g", value, precision, got, want)
				}
			}
		}
	}
}

func TestRoundAndTruncateExtremeScale(t *testing.T) {
	for _, fn := range []struct {
		name string
		eval func(ExecEnv, []Expr, Row) (any, error)
	}{{"ROUND", evalRound}, {"TRUNCATE", evalTruncate}} {
		for _, tc := range []struct {
			value     float64
			precision int
			want      float64
		}{
			{1.25, 400, 1.25},
			{1.25, -400, 0},
			{1e308, 2, 1e308},
			{1e-310, 309, 0},
			{1e-310, 324, 1e-310},
			{-1.25, -400, math.Copysign(0, -1)},
		} {
			got, err := fn.eval(ExecEnv{}, []Expr{&Literal{Val: tc.value}, &Literal{Val: tc.precision}}, nil)
			if err != nil || got != tc.want || math.Signbit(got.(float64)) != math.Signbit(tc.want) {
				t.Errorf("%s(%v, %d) = %v, %v; want %v", fn.name, tc.value, tc.precision, got, err, tc.want)
			}
		}
		wantSubnormal := float64(0)
		if fn.name == "ROUND" {
			wantSubnormal = 1e-309
		}
		got, err := fn.eval(ExecEnv{}, []Expr{&Literal{Val: 9e-310}, &Literal{Val: 309}}, nil)
		if err != nil || got != wantSubnormal {
			t.Errorf("%s(9e-310, 309) = %v, %v; want %v", fn.name, got, err, wantSubnormal)
		}
		value := 1e-310
		if fn.name == "TRUNCATE" {
			value = 1.1e-310
		}
		got, err = fn.eval(ExecEnv{}, []Expr{&Literal{Val: value}, &Literal{Val: 310}}, nil)
		if err != nil || got != float64(1e-310) {
			t.Errorf("%s(%v, 310) = %v, %v; want 1e-310", fn.name, value, got, err)
		}
	}
	db := storage.NewDB()
	for _, name := range []string{"ROUND", "TRUNCATE"} {
		for expr, want := range map[string]float64{
			name + "(1.25, 400)":            1.25,
			name + "(1.25, -400)":           0,
			name + "(POWER(10, 308), 2)":    math.Pow(10, 308),
			name + "(1.25, POWER(10, 100))": 1.25,
		} {
			if got := queryScalar(t, db, expr); got != want {
				t.Errorf("%s = %v, want %v", expr, got, want)
			}
		}
	}
}

func TestMathFunctionsNullableColumns(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE nullable_math (id INT, value FLOAT)`)
	execSQL(t, db, `INSERT INTO nullable_math VALUES (1, NULL), (2, 4)`)
	stmt := mustParse(`SELECT id, SQRT(value) AS root, MOD(value, 3) AS remainder, POWER(2, value) AS power FROM nullable_math ORDER BY id`)

	result, err := Execute(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows", len(result.Rows))
	}
	for _, col := range []string{"root", "remainder", "power"} {
		if got := result.Rows[0][col]; got != nil {
			t.Errorf("NULL row %s = %#v", col, got)
		}
	}
	for col, want := range map[string]float64{"root": 2, "remainder": 1, "power": 16} {
		if got := result.Rows[1][col]; got != want {
			t.Errorf("numeric row %s = %#v, want %v", col, got, want)
		}
	}

	stream, err := ExecuteStream(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	rows := drainStream(t, stream)
	if len(rows) != 2 || rows[0]["root"] != nil || rows[1]["root"] != float64(2) {
		t.Fatalf("streamed rows: %#v", rows)
	}
}

func TestMathFunctionsStillRejectInvalidInputs(t *testing.T) {
	for _, expr := range []string{"SQRT('bad')", "MOD(1, 0)", "POWER('bad', 2)"} {
		stmt := mustParse("SELECT " + expr)
		if _, err := Execute(t.Context(), storage.NewDB(), "default", stmt); err == nil {
			t.Errorf("%s must reject its input", expr)
		}
	}
}
