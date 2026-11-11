package engine

import (
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func distinctFloatTestValues() []any {
	return []any{
		nil, 0.0, math.Copysign(0, -1), 1.0, -1.0, 1.5,
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
		math.MaxFloat64, -math.MaxFloat64, math.Nextafter(1, 2),
		math.Inf(1), math.Inf(-1), math.NaN(),
		math.Float64frombits(0x7ff0000000000001),
		math.Float64frombits(0xfff8000000000042),
		float64(9007199254740992),
	}
}

func TestDistinctFloatCountMatchesText(t *testing.T) {
	floats := distinctFloatTestValues()
	random := rand.New(rand.NewPCG(17, 29))
	for range 200 {
		floats = append(floats, math.Float64frombits(random.Uint64()))
	}
	mixed := append(distinctFloatTestValues(),
		int(0), int64(0), int64(math.MinInt64), int64(math.MaxInt64),
		int64(9007199254740992), int64(9007199254740993),
		"0", "-0", "1", "1.5", "NaN", "+Inf", "-Inf", "9007199254740993",
		false, true, "false", "true", big.NewRat(1, 2), "1/2", []byte("1"), float32(1.5),
		"1.00", "0.50", "1e0", "nan", "Infinity", "1e1000", "1e-1000")
	floatText := append([]any(nil), floats...)
	for _, value := range floats {
		if value, ok := value.(float64); ok {
			floatText = append(floatText, strconv.FormatFloat(value, 'g', -1, 64))
		}
	}
	for _, tc := range []struct {
		name   string
		values []any
	}{{"Float", floats}, {"Mixed", mixed}, {"FloatText", floatText}} {
		t.Run(tc.name, func(t *testing.T) {
			// Every rotation exercises a different first type and migration
			// point; compare each insertion, not just the final count.
			for shift := range tc.values {
				var set distinctCountSet
				seen := make(map[string]bool)
				for range 2 {
					for offset := range tc.values {
						value := tc.values[(offset+shift)%len(tc.values)]
						want := false
						if value != nil {
							key := valueText(value)
							want = !seen[key]
							seen[key] = true
						}
						if got := set.add(value); got != want {
							t.Fatalf("shift %d, %T(%v): inserted %v, want %v", shift, value, value, got, want)
						}
					}
				}
			}
		})
	}
}

func TestDistinctFloatSelectMatchesGeneral(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		db := storage.NewDB()
		t.Cleanup(func() { _ = db.Close() })
		table := storage.NewTable("float_distinct", []storage.Column{
			{Name: "id", Type: storage.IntType}, {Name: "v", Type: storage.InterfaceType},
		}, false)
		values := distinctFloatTestValues()
		if mixed {
			values = append(values, int(1), int64(1), "1", "NaN", big.NewRat(1, 2), []byte("1"))
		}
		for round := range 3 {
			for i, value := range values {
				table.Rows = append(table.Rows, []any{round*len(values) + i, value})
			}
		}
		if err := db.Put("default", table); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{"", " WHERE id >= 4", " LIMIT 5 OFFSET 2", " ORDER BY value DESC LIMIT 9 OFFSET 1"} {
			query := "SELECT DISTINCT v AS value FROM "
			fast := execSQL(t, db, query+"float_distinct"+suffix)
			general := execSQL(t, db, query+"(SELECT * FROM float_distinct) AS source"+suffix)
			if !reflect.DeepEqual(fast.Cols, general.Cols) || len(fast.Rows) != len(general.Rows) {
				t.Fatalf("mixed=%v %s: different output shape", mixed, suffix)
			}
			for i, row := range fast.Rows {
				got, want := row["value"], general.Rows[i]["value"]
				if real, ok := got.(float64); ok {
					other, ok := want.(float64)
					if !ok || math.Float64bits(real) != math.Float64bits(other) {
						t.Fatalf("mixed=%v %s row %d: float bits changed: %v / %v", mixed, suffix, i, got, want)
					}
				} else if !reflect.DeepEqual(got, want) {
					t.Fatalf("mixed=%v %s row %d: %T(%v), want %T(%v)", mixed, suffix, i, got, got, want, want)
				}
			}
		}
	}
}

func TestDistinctFloatCountQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("float_counts", []storage.Column{
		{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.InterfaceType},
	}, false)
	values := distinctFloatTestValues()
	seen := make(map[string]bool)
	for range 2 {
		for _, value := range values {
			table.Rows = append(table.Rows, []any{1, value})
			if value != nil {
				seen[valueText(value)] = true
			}
		}
	}
	table.Rows = append(table.Rows, []any{2, nil})
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	want := []Row{{"g": 1, "n": len(seen)}, {"g": 2, "n": 0}}
	for _, source := range []string{"float_counts", "(SELECT * FROM float_counts) AS source"} {
		got := execSQL(t, db, "SELECT g, COUNT(DISTINCT v) AS n FROM "+source+" GROUP BY g ORDER BY g")
		if !reflect.DeepEqual(got.Rows, want) {
			t.Fatalf("%s: got %v, want %v", source, got.Rows, want)
		}
		got = execSQL(t, db, "SELECT COUNT(DISTINCT v) AS n FROM "+source)
		if !reflect.DeepEqual(got.Rows, []Row{{"n": len(seen)}}) {
			t.Fatalf("%s: unexpected overall count %v", source, got.Rows)
		}
	}
	// Exercise migration after a float-only execution of the same statement.
	statement := mustParse("SELECT COUNT(DISTINCT v) AS n FROM float_counts")
	if _, err := Execute(t.Context(), db, "default", statement); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, "INSERT INTO float_counts VALUES (1, 'NaN'), (1, 'new')")
	got, err := Execute(t.Context(), db, "default", statement)
	if err != nil || !reflect.DeepEqual(got.Rows, []Row{{"n": len(seen) + 1}}) {
		t.Fatalf("updated input: %v / %v", got, err)
	}
}
