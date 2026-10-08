package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A box predicate on an index over (lat, lon) prunes by longitude while it walks
// the latitude band. Whatever it prunes must be exactly what the predicate would
// reject, so the answer has to equal the unindexed scan's -- including NULLs,
// signed zeros, integers mixed into float columns, and bounds written as
// integers or floats.
func TestRangeIndexNextColumnPruningMatchesScan(t *testing.T) {
	type spec struct {
		name  string
		latFn func(rng *rand.Rand, i int) any
		lonFn func(rng *rand.Rand, i int) any
	}
	floats := func(rng *rand.Rand, i int) any { return (rng.Float64() - 0.5) * 20 }
	specs := []spec{
		{"float/float", floats, floats},
		{"float/nullable", floats, func(rng *rand.Rand, i int) any {
			if i%7 == 0 {
				return nil
			}
			return (rng.Float64() - 0.5) * 20
		}},
		{"float/signed-zero", floats, func(rng *rand.Rand, i int) any {
			switch i % 9 {
			case 0:
				return math.Copysign(0, -1)
			case 1:
				return float64(0)
			}
			return float64(rng.Intn(11) - 5)
		}},
		{"float/integers-in-lon", floats, func(rng *rand.Rand, i int) any { return rng.Intn(21) - 10 }},
		{"float/mixed-lon", floats, func(rng *rand.Rand, i int) any {
			if i%2 == 0 {
				return rng.Intn(21) - 10
			}
			return (rng.Float64() - 0.5) * 20
		}},
		{"int/int", func(rng *rand.Rand, i int) any { return rng.Intn(41) - 20 }, func(rng *rand.Rand, i int) any { return rng.Intn(41) - 20 }},
	}
	boundTexts := []string{"-3", "-3.5", "0", "0.0", "2", "2.5", "7", "-12", "11"}

	for _, sp := range specs {
		t.Run(sp.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(99))
			ctx := context.Background()
			db := storage.NewDB()
			run := func(q string) *ResultSet {
				t.Helper()
				rs, err := Execute(ctx, db, "default", mustParse(q))
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				return rs
			}
			run(`CREATE TABLE g (id INT, lat FLOAT, lon FLOAT)`)
			if sp.name == "int/int" {
				run(`DROP TABLE g`)
				run(`CREATE TABLE g (id INT, lat INT, lon INT)`)
			}
			table, _ := db.Get("default", "g")
			for i := 0; i < 600; i++ {
				table.Rows = append(table.Rows, []any{i, sp.latFn(rng, i), sp.lonFn(rng, i)})
			}
			table.Version++
			scanIDs := func(q string) []int {
				rs := run(q)
				ids := make([]int, 0, len(rs.Rows))
				for _, r := range rs.Rows {
					ids = append(ids, r["id"].(int))
				}
				sort.Ints(ids)
				return ids
			}
			type query struct{ q string }
			var queries []query
			for n := 0; n < 120; n++ {
				a, b := boundTexts[rng.Intn(len(boundTexts))], boundTexts[rng.Intn(len(boundTexts))]
				c, d := boundTexts[rng.Intn(len(boundTexts))], boundTexts[rng.Intn(len(boundTexts))]
				switch rng.Intn(4) {
				case 0:
					queries = append(queries, query{fmt.Sprintf(`SELECT id FROM g WHERE lat BETWEEN %s AND %s AND lon BETWEEN %s AND %s`, a, b, c, d)})
				case 1:
					queries = append(queries, query{fmt.Sprintf(`SELECT id FROM g WHERE lat >= %s AND lat < %s AND lon > %s AND lon <= %s`, a, b, c, d)})
				case 2:
					queries = append(queries, query{fmt.Sprintf(`SELECT id FROM g WHERE lat BETWEEN %s AND %s AND lon >= %s`, a, b, c)})
				default:
					queries = append(queries, query{fmt.Sprintf(`SELECT id FROM g WHERE lat > %s AND lon < %s`, a, c)})
				}
			}
			want := make([][]int, len(queries))
			for i, q := range queries {
				want[i] = scanIDs(q.q) // no index yet: a table scan is the reference
			}
			run(`CREATE INDEX g_latlon ON g (lat, lon)`)
			for i, q := range queries {
				got := scanIDs(q.q)
				if !reflect.DeepEqual(got, want[i]) {
					t.Fatalf("%s\n indexed %v\n scan    %v", q.q, got, want[i])
				}
			}
		})
	}
}

// The benefit itself: the walk must not hand the executor the whole latitude
// band when the longitude bound excludes almost all of it.
func TestRangeIndexNextColumnNarrowsCandidates(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, q := range []string{`CREATE TABLE p (id INT, lat FLOAT, lon FLOAT)`} {
		if _, err := Execute(ctx, db, "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	table, _ := db.Get("default", "p")
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 20000; i++ {
		table.Rows = append(table.Rows, []any{i, 45 + rng.Float64()*10, 5 + rng.Float64()*10})
	}
	table.Version++
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE INDEX p_latlon ON p (lat, lon)`)); err != nil {
		t.Fatal(err)
	}
	idx := table.Indexes["p_latlon"]
	lo, hi := storage.IndexRangeBound{Value: 50.0, Inclusive: true}, storage.IndexRangeBound{Value: 50.5, Inclusive: true}
	band, err := table.LookupSecondaryIndexRange(idx, nil, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	next := &storage.IndexNextBounds{Lo: storage.IndexRangeBound{Value: 10.0, Inclusive: true}, Hi: storage.IndexRangeBound{Value: 10.5, Inclusive: true}}
	box, err := table.LookupSecondaryIndexRangeNext(idx, nil, lo, hi, next)
	if err != nil {
		t.Fatal(err)
	}
	if len(band) < 500 || len(box) > len(band)/10 {
		t.Fatalf("band has %d candidates, box walk %d: the next-column bounds did not narrow the walk", len(band), len(box))
	}
	inBox := map[int]bool{}
	for _, id := range box {
		inBox[id] = true
	}
	for _, id := range band {
		lon := table.Rows[id][2].(float64)
		if lon >= 10 && lon <= 10.5 && !inBox[id] {
			t.Fatalf("row %d with lon %v inside the box was pruned", id, lon)
		}
	}
}
