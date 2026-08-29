package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// SELECT * across a join used to disqualify the raw join fast path, so it fell
// back to the general path: both inputs materialized as dual-key Row maps by
// rowsFromTable, plus a merged map per output row. buildSimpleJoinStarProjections
// expands the star into direct column projections instead.
//
// The observable contract that must not change:
//   - output columns are the unqualified names, de-duplicated on first
//     occurrence, left table first;
//   - each column is reachable under both its unqualified and qualified name;
//   - where two tables share an unqualified column name, the RIGHT table's
//     value wins (mergeRows(l, r) overwrites l with r).

func starJoinDB(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE la (id INT, av TEXT, shared TEXT)`)
	execSQL(t, db, `CREATE TABLE rb (id INT, bv TEXT, shared TEXT)`)
	for i := 0; i < 5; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO la VALUES (%d, 'a%d', 'LEFT%d')`, i, i, i))
		execSQL(t, db, fmt.Sprintf(`INSERT INTO rb VALUES (%d, 'b%d', 'RIGHT%d')`, i, i, i))
	}
	return db
}

func TestStarJoinFastPathColumnContract(t *testing.T) {
	db := starJoinDB(t)
	rs, err := Execute(context.Background(), db, "default",
		mustParse(`SELECT * FROM la JOIN rb ON la.id = rb.id`))
	if err != nil {
		t.Fatal(err)
	}
	// Unqualified names, de-duplicated, left side first.
	want := []string{"id", "av", "shared", "bv"}
	if len(rs.Cols) != len(want) {
		t.Fatalf("Cols = %v, want %v", rs.Cols, want)
	}
	for i := range want {
		if !strings.EqualFold(rs.Cols[i], want[i]) {
			t.Fatalf("Cols = %v, want %v", rs.Cols, want)
		}
	}
	if len(rs.Rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(rs.Rows))
	}
	for _, r := range rs.Rows {
		// Both qualified forms must resolve.
		for _, k := range []string{"la.id", "la.av", "la.shared", "rb.id", "rb.bv", "rb.shared"} {
			if _, ok := ragValue(r, k); !ok {
				t.Errorf("qualified key %q missing from star-join row %v", k, r)
			}
		}
		// The colliding unqualified name takes the RIGHT table's value.
		shared, _ := ragValue(r, "shared")
		rightShared, _ := ragValue(r, "rb.shared")
		if fmt.Sprintf("%v", shared) != fmt.Sprintf("%v", rightShared) {
			t.Errorf("unqualified 'shared' = %v, want right table's %v", shared, rightShared)
		}
		if s := fmt.Sprintf("%v", shared); !strings.HasPrefix(s, "RIGHT") {
			t.Errorf("unqualified 'shared' = %q, want the RIGHT table's value", s)
		}
		// The left value stays reachable under its qualified name.
		leftShared, _ := ragValue(r, "la.shared")
		if s := fmt.Sprintf("%v", leftShared); !strings.HasPrefix(s, "LEFT") {
			t.Errorf("la.shared = %q, want the LEFT table's value", s)
		}
	}
}

// TestStarJoinFastPathMatchesGeneralPath compares against the general path.
// A join whose ON predicate is not a bare equi-join (here an added OR term)
// declines the raw fast path, so the same data can be produced both ways.
func TestStarJoinFastPathMatchesGeneralPath(t *testing.T) {
	db := starJoinDB(t)
	render := func(sql string) []string {
		rs, err := Execute(context.Background(), db, "default", mustParse(sql))
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		out := make([]string, 0, len(rs.Rows))
		for _, r := range rs.Rows {
			keys := keysOfRow(r)
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%v", k, r[k]))
			}
			out = append(out, strings.Join(parts, ","))
		}
		sort.Strings(out)
		return out
	}
	fast := render(`SELECT * FROM la JOIN rb ON la.id = rb.id`)
	general := render(`SELECT * FROM la JOIN rb ON la.id = rb.id AND (la.id = rb.id OR la.av = rb.bv)`)
	if len(fast) != len(general) {
		t.Fatalf("row count differs: fast=%d general=%d\n fast=%v\n general=%v",
			len(fast), len(general), fast, general)
	}
	for i := range fast {
		if fast[i] != general[i] {
			t.Errorf("row %d differs:\n  fast    = %s\n  general = %s", i, fast[i], general[i])
		}
	}
}

// TestStarJoinFastPathWithWhere keeps the pushed-down WHERE filters honest for
// star projections, which resolve columns by index rather than by name.
func TestStarJoinFastPathWithWhere(t *testing.T) {
	db := starJoinDB(t)
	rs, err := Execute(context.Background(), db, "default",
		mustParse(`SELECT * FROM la JOIN rb ON la.id = rb.id WHERE la.id > 2`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rs.Rows))
	}
	for _, r := range rs.Rows {
		v, _ := ragValue(r, "la.id")
		n, err := toInt(v)
		if err != nil || n <= 2 {
			t.Errorf("row leaked past WHERE la.id > 2: %v", r)
		}
	}
}

// BenchmarkStarJoin measures the shape the fast path now covers.
func BenchmarkStarJoin(b *testing.B) {
	db := setupJoinBenchTables(b, 2000)
	runBench(b, db, `SELECT * FROM jl JOIN jr ON jl.id = jr.id`)
}

// BenchmarkStarJoinProjected is the explicit-column control: it was already on
// the fast path, so it should be roughly unchanged.
func BenchmarkStarJoinProjected(b *testing.B) {
	db := setupJoinBenchTables(b, 2000)
	runBench(b, db, `SELECT jl.id, jl.lv, jr.rv FROM jl JOIN jr ON jl.id = jr.id`)
}

func setupJoinBenchTables(b *testing.B, rows int) *storage.DB {
	b.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE TABLE jl (id INT, lv TEXT, lx TEXT)`,
		`CREATE TABLE jr (id INT, rv TEXT, rx TEXT)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(ddl)); err != nil {
			b.Fatal(err)
		}
	}
	for _, spec := range []struct {
		name   string
		prefix string
	}{{"jl", "l"}, {"jr", "r"}} {
		table, err := db.Get("default", spec.name)
		if err != nil {
			b.Fatal(err)
		}
		table.Rows = make([][]any, rows)
		for i := 0; i < rows; i++ {
			table.Rows[i] = []any{
				float64(i),
				fmt.Sprintf("%sv-%d", spec.prefix, i),
				fmt.Sprintf("%sx-%d filler text", spec.prefix, i),
			}
		}
		table.Version++
	}
	return db
}
