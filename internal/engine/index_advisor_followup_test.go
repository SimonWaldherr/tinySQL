package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestIndexAdvisorStablePatternNames(t *testing.T) {
	db := advisorFixture(t, 64)
	a, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinTableRows: 32})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"SELECT lookup FROM events WHERE lookup=1",
		"SELECT lookup FROM events e WHERE 2=e.lookup",
		"SELECT lookup FROM events WHERE lookup=3 AND lookup=3",
	} {
		if _, err := a.Execute(t.Context(), "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	r := a.Recommendations()
	if len(r) != 1 || r[0].Executions != 3 || r[0].Name != "auto_idx_c31e78877ad7400d40767f8b" {
		t.Fatal(r)
	}
	// Bound NULLs are reevaluated each time; observation never caches literals.
	stmt := mustParse("SELECT lookup FROM events WHERE lookup=4").(*Select)
	if _, err := a.Execute(t.Context(), "default", stmt); err != nil {
		t.Fatal(err)
	}
	stmt.Where.(*Binary).Right.(*Literal).Val = nil
	if _, err := a.Execute(t.Context(), "default", stmt); err != nil {
		t.Fatal(err)
	}
	if a.Recommendations()[0].Executions != 4 {
		t.Fatal("cached NULL eligibility")
	}
}

func TestIndexAdvisorWideConjunctionScratch(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	cols := make([]storage.Column, 8)
	predicates := make([]string, 8)
	for i := range cols {
		cols[i] = storage.Column{Name: fmt.Sprintf("c%d", i), Type: storage.IntType}
		predicates[i] = fmt.Sprintf("c%d=17", i)
	}
	table := storage.NewTable("wide", cols, false)
	for i := 0; i < 64; i++ {
		row := make([]any, len(cols))
		for j := range row {
			row[j] = i
		}
		table.Rows = append(table.Rows, row)
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	a, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinExecutions: 2, MinTableRows: 32, MaxCandidates: 6})
	if err != nil {
		t.Fatal(err)
	}
	sql := "SELECT c0 FROM wide WHERE " + strings.Join(predicates, " AND ") + " AND c0=17"
	for i := 0; i < 2; i++ {
		if _, err := a.Execute(t.Context(), "default", mustParse(sql)); err != nil {
			t.Fatal(err)
		}
	}
	recs := a.Recommendations()
	if len(recs) != 6 {
		t.Fatal(recs)
	}
	for _, r := range recs {
		if r.Executions != 2 || r.Status != "ready" || r.Column > "c5" {
			t.Fatal(r)
		}
	}
	if err := a.Apply(t.Context(), "default", recs[5].Name); err != nil {
		t.Fatal(err)
	}
	if table.Indexes[recs[5].Name] == nil {
		t.Fatal("name lookup applied wrong pattern")
	}
}

func BenchmarkIndexAdvisorSmallQuery(b *testing.B) {
	for _, compound := range []bool{false, true} {
		for _, observe := range []bool{false, true} {
			b.Run(fmt.Sprintf("compound%t/observe%t", compound, observe), func(b *testing.B) {
				db := advisorFixture(b, 64)
				a, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinExecutions: 1, MinTableRows: 32})
				if err != nil {
					b.Fatal(err)
				}
				sql := "SELECT lookup FROM events WHERE lookup=17"
				if compound {
					sql += " AND lookup=17"
				}
				stmt := mustParse(sql)
				if _, err := a.Execute(b.Context(), "default", stmt); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					var rs *ResultSet
					var err error
					if observe {
						rs, err = a.Execute(b.Context(), "default", stmt)
					} else {
						rs, err = Execute(b.Context(), db, "default", stmt)
					}
					if err != nil || len(rs.Rows) != 1 {
						b.Fatal(rs, err)
					}
				}
			})
		}
	}
}
