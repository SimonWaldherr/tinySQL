package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// DELETE may evaluate its predicate on the candidate rows of a secondary index
// instead of on every row (deleteCandidateRows). That must be indistinguishable
// from the scan: the same rows removed, in the same order, the same RETURNING
// rows, the same errors, and an index that still agrees with the table. Every
// case runs on two databases that hold the same rows, one with indexes (idx) and
// one without (ref), over data made to expose every difference between SQL
// equality and index keys: NULLs, empty strings, case variants, a value that is a
// prefix of another, trailing spaces and non-ASCII text.

var dcGroups = []any{"g1", "G1", "g10", "g1 ", "", nil, "é", "g2", "g", "g1\x00"}
var dcSubs = []any{"a", "b", "A", "", nil, "ab"}

func dcLit(v any) string {
	if v == nil {
		return "NULL"
	}
	return sqlQuote(v.(string))
}

func dcPair(t *testing.T, seed int64, indexes bool) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	rmMust(t, db, `CREATE TABLE r (id INT, grp TEXT, sub TEXT, n INT, f FLOAT, note TEXT)`)
	r := rand.New(rand.NewSource(seed))
	var sb strings.Builder
	sb.WriteString(`INSERT INTO r VALUES `)
	for i := 0; i < 90; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		n, f := "NULL", "NULL"
		if r.Intn(8) != 0 {
			n = fmt.Sprint(r.Intn(5))
		}
		switch r.Intn(4) {
		case 0:
			f = "1.5"
		case 1:
			f = "2.0"
		}
		fmt.Fprintf(&sb, "(%d, %s, %s, %s, %s, %s)", i, dcLit(dcGroups[r.Intn(len(dcGroups))]), dcLit(dcSubs[r.Intn(len(dcSubs))]), n, f, dcLit(dcSubs[r.Intn(len(dcSubs))]))
	}
	rmMust(t, db, sb.String())
	if indexes {
		for _, stmt := range []string{
			`CREATE INDEX i_grp_sub ON r(grp, sub)`,
			`CREATE INDEX i_sub_grp ON r(sub, grp)`,
			`CREATE INDEX i_note ON r(note)`,
			`CREATE INDEX i_n ON r(n)`,
		} {
			rmMust(t, db, stmt)
		}
	}
	return db
}

type dcCase struct {
	where   string
	indexed bool // deleteCandidateRows must offer candidates
}

func dcCases(a, b any, n int) []dcCase {
	A, B := dcLit(a), dcLit(b)
	return []dcCase{
		{`grp = ` + A, a != nil},
		{A + ` = grp`, a != nil},
		{`grp = ` + A + ` AND sub = ` + B, a != nil && b != nil},
		{`grp = ` + A + ` AND sub <> ` + B, a != nil && b != nil},
		{`grp = ` + A + ` AND sub > ` + B, false}, // ordering on text is left to the scan
		{`sub = ` + B + ` AND grp = ` + A, a != nil && b != nil},
		{`grp IN (` + A + `, ` + B + `)`, a != nil && b != nil},
		{`grp IN (` + A + `, ` + A + `)`, a != nil},
		{`grp IN (` + A + `) AND sub = ` + B, a != nil && b != nil},
		{`grp = ` + A + ` AND sub IN (` + B + `, 'a')`, a != nil && b != nil},
		{`grp = ` + A + ` AND NOT sub = ` + B, a != nil && b != nil},
		{`grp = ` + A + ` AND sub IS NULL`, a != nil},
		{`grp = ` + A + ` AND sub IS NOT NULL`, a != nil},
		{`grp = ` + A + ` AND n > ` + fmt.Sprint(n), false}, // INT columns carry INTEGER affinity
		{`grp = ` + A + ` AND n = ` + fmt.Sprint(n), false},
		{`grp = ` + A + ` AND (n = 1 OR n = 2)`, false},
		{`grp = ` + A + ` AND grp = ` + B, a != nil && b != nil},
		{`note = ` + B, b != nil},
		{`note = ` + B + ` AND grp <> ` + A, a != nil && b != nil},
		{`NOT grp = ` + A, false},
		{`grp = ` + A + ` OR sub = ` + B, false},
		{`grp LIKE 'g%'`, false},
		{`grp = ` + A + ` AND f = 1.5`, false},
		{`grp = ` + A + ` AND f > ` + fmt.Sprint(n), false},
		// The numeric forms that SQL equality and index keys disagree on: scans.
		{`n = ` + fmt.Sprint(n), false},
		{`n = ` + fmt.Sprint(n) + `.0`, false},
		{`f = 2`, false},
		// Literals of another kind than the column: scans, with the scan's answer
		// or error.
		{`grp = ` + fmt.Sprint(n), false},
		{`grp = ` + A + ` AND n = '` + fmt.Sprint(n) + `'`, false},
		{`grp = ` + A + ` AND sub = ` + fmt.Sprint(n), false},
		{`grp IN (` + A + `, ` + fmt.Sprint(n) + `)`, false},
		{`grp = ` + A + ` AND 10 / (n - ` + fmt.Sprint(n) + `) > 1`, false},
		{`grp = ` + A + ` AND ABS(n) > ` + fmt.Sprint(n), false},
		{`grp = NULL`, false},
		{`grp = ` + A + ` AND sub = NULL`, false},
		{`grp = ` + A + ` AND id IN (SELECT id FROM r WHERE n > 1)`, false},
		{`1 = 1`, false},
		{`id < 0`, false},
	}
}

func dcRun(db *storage.DB, sql string) (rows []Row, err error) {
	rs, err := rmRun(db, sql)
	return rmRows(rs), err
}

func TestDeleteWithIndexCandidatesMatchesTheScan(t *testing.T) {
	cases := 0
	indexedRuns := 0
	r := rand.New(rand.NewSource(5))
	for round := 0; round < 12; round++ {
		a := dcGroups[r.Intn(len(dcGroups))]
		b := dcSubs[r.Intn(len(dcSubs))]
		if round%3 == 0 {
			b = dcGroups[r.Intn(len(dcGroups))] // a "sub" value that is also a group
		}
		n := r.Intn(5)
		for _, tc := range dcCases(a, b, n) {
			for _, returning := range []bool{false, true} {
				sql := `DELETE FROM r WHERE ` + tc.where
				if returning {
					sql += ` RETURNING id, grp, sub`
				}
				label := sql
				seed := int64(round*7 + 1)
				idxDB, refDB := dcPair(t, seed, true), dcPair(t, seed, false)
				idxTable, refTable := w0(t, idxDB), w0(t, refDB)

				// What the planner offers, and whether it is complete.
				stmt := mustParse(`DELETE FROM r WHERE ` + tc.where).(*Delete)
				candidates, indexed := deleteCandidateRows(idxTable, simpleColumnIndex(idxTable, "r"), stmt.Where)
				if indexed != tc.indexed {
					t.Fatalf("%s: candidates offered = %v, want %v", label, indexed, tc.indexed)
				}
				want, wantErr := dcRun(refDB, `SELECT id FROM r WHERE `+tc.where)
				if indexed {
					indexedRuns++
					if wantErr != nil {
						t.Fatalf("%s: a predicate that fails on a scan was given candidates", label)
					}
					for _, row := range want {
						pos := slices.IndexFunc(refTable.Rows, func(x []any) bool { return x[0] == row["id"] })
						if !slices.Contains(candidates, pos) {
							t.Fatalf("%s: row %d (id %v) satisfies the predicate but is not a candidate %v", label, pos, row["id"], candidates)
						}
					}
					if !slices.IsSorted(candidates) {
						t.Fatalf("%s: candidates not ascending: %v", label, candidates)
					}
				}

				got, gotErr := dcRun(idxDB, sql)
				ref, refErr := dcRun(refDB, sql)
				if (gotErr == nil) != (refErr == nil) || (gotErr != nil && gotErr.Error() != refErr.Error()) {
					t.Fatalf("%s: error %v, the scan gave %v", label, gotErr, refErr)
				}
				if !reflect.DeepEqual(got, ref) {
					t.Fatalf("%s:\n idx %v\n ref %v", label, got, ref)
				}
				if !reflect.DeepEqual(idxTable.Rows, refTable.Rows) {
					t.Fatalf("%s: the tables differ afterwards", label)
				}
				rmCheckIndexes(t, idxTable, label)
				cases++
			}
		}
	}
	if indexedRuns < 100 {
		t.Fatalf("only %d of %d statements used candidates", indexedRuns, cases)
	}
	t.Logf("%d statements, %d with index candidates", cases, indexedRuns)
}

func w0(t *testing.T, db *storage.DB) *storage.Table {
	t.Helper()
	table, err := db.Get("default", "r")
	if err != nil {
		t.Fatal(err)
	}
	return table
}

// The candidates are looked up for the value of each statement, also when the
// same table is deleted from again and again with changing values. (Rebinding the
// literals of one AST is TestDeleteWithIndexCandidatesBoundParameters.)
func TestDeleteWithIndexCandidatesFollowsBoundValues(t *testing.T) {
	db := dcPair(t, 3, true)
	ref := dcPair(t, 3, false)
	for _, group := range []string{"g1", "g2", "g1", "zz", "G1"} {
		sql := `DELETE FROM r WHERE grp = ` + sqlQuote(group) + ` AND sub <> 'b'`
		got, err1 := dcRun(db, sql)
		want, err2 := dcRun(ref, sql)
		if err1 != nil || err2 != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %v %v / %v %v", sql, got, err1, want, err2)
		}
		if !reflect.DeepEqual(w0(t, db).Rows, w0(t, ref).Rows) {
			t.Fatalf("%s: tables differ", sql)
		}
		rmCheckIndexes(t, w0(t, db), sql)
	}
}

// dcMarkers returns the literals of e whose value is one of the marker strings
// (p1, p2, ...), in the order of the markers.
func dcMarkers(e Expr, out map[string]*Literal) {
	switch ex := e.(type) {
	case *Binary:
		dcMarkers(ex.Left, out)
		dcMarkers(ex.Right, out)
	case *Unary:
		dcMarkers(ex.Expr, out)
	case *InExpr:
		dcMarkers(ex.Expr, out)
		for _, v := range ex.Values {
			dcMarkers(v, out)
		}
	case *Literal:
		if s, ok := ex.Val.(string); ok && len(s) == 2 && s[0] == 'p' {
			out[s] = ex
		}
	}
}

// A prepared statement runs the same AST again with other bound values (the
// driver rebinds Literal.Val and marks it Parameter). The candidates depend on
// the value of each execution, including its type: a string seeks the index, an
// integer, a NULL or a blob scans, and the answer is the scan's either way.
func TestDeleteWithIndexCandidatesBoundParameters(t *testing.T) {
	shapes := []struct {
		sql     string
		markers int
	}{
		{`DELETE FROM r WHERE grp = 'p1' AND sub <> 'b'`, 1},
		{`DELETE FROM r WHERE grp IN ('p1', 'p2') AND sub <> 'b'`, 2},
		{`DELETE FROM r WHERE grp = 'p1' AND sub = 'p2' RETURNING id, grp`, 2},
		{`DELETE FROM r WHERE 'p1' = grp AND n IS NOT NULL`, 1},
	}
	values := []any{"g1", "g2", int64(5), nil, "G1", "g1", []byte("g1"), "", 2.5, "zz", "g10", true}
	for _, shape := range shapes {
		idxDB, refDB := dcPair(t, 4, true), dcPair(t, 4, false)
		idxStmt, refStmt := mustParse(shape.sql).(*Delete), mustParse(shape.sql).(*Delete)
		bind := func(stmt *Delete, vals []any) map[string]*Literal {
			found := map[string]*Literal{}
			dcMarkers(stmt.Where, found)
			if len(found) != shape.markers {
				t.Fatalf("%s: found %d markers, want %d", shape.sql, len(found), shape.markers)
			}
			for i := 1; i <= shape.markers; i++ {
				lit := found[fmt.Sprintf("p%d", i)]
				lit.Parameter = true
				lit.Val = vals[i-1]
			}
			return found
		}
		// The literals are found once (while they still hold their marker) and
		// rebound for every execution.
		idxLits, refLits := bind(idxStmt, []any{"x", "y"}), bind(refStmt, []any{"x", "y"})
		indexedRuns := 0
		for i, v := range values {
			vals := []any{v, values[(i+3)%len(values)]}
			for k := 1; k <= shape.markers; k++ {
				idxLits[fmt.Sprintf("p%d", k)].Val = vals[k-1]
				refLits[fmt.Sprintf("p%d", k)].Val = vals[k-1]
			}
			label := fmt.Sprintf("%s with %#v", shape.sql, vals[:shape.markers])
			table := w0(t, idxDB)
			_, indexed := deleteCandidateRows(table, simpleColumnIndex(table, "r"), idxStmt.Where)
			want := true
			for _, v := range vals[:shape.markers] {
				if _, isString := v.(string); !isString {
					want = false
				}
			}
			if indexed != want {
				t.Fatalf("%s: candidates offered = %v, want %v", label, indexed, want)
			}
			if indexed {
				indexedRuns++
			}
			got, err1 := Execute(t.Context(), idxDB, "default", idxStmt)
			ref, err2 := Execute(t.Context(), refDB, "default", refStmt)
			if (err1 == nil) != (err2 == nil) || (err1 != nil && err1.Error() != err2.Error()) {
				t.Fatalf("%s: error %v, the scan gave %v", label, err1, err2)
			}
			if !reflect.DeepEqual(rmRows(got), rmRows(ref)) {
				t.Fatalf("%s:\n idx %v\n ref %v", label, rmRows(got), rmRows(ref))
			}
			if !reflect.DeepEqual(table.Rows, w0(t, refDB).Rows) {
				t.Fatalf("%s: the tables differ afterwards", label)
			}
			rmCheckIndexes(t, table, label)
		}
		if indexedRuns == 0 {
			t.Fatalf("%s: no execution used candidates", shape.sql)
		}
	}
}

// Long index keys (the scratch buffer of a point lookup is 128 bytes), a UNIQUE
// index over a nullable column with a NULL key, and a composite index whose
// leading column holds long URLs: the statement an importer issues for a source
// named by a URL.
func TestDeleteWithIndexCandidatesLongKeysAndNullableUnique(t *testing.T) {
	build := func(indexes bool) *storage.DB {
		db := storage.NewDB()
		t.Cleanup(func() { _ = db.Close() })
		rmMust(t, db, `CREATE TABLE r (id INT, url TEXT, u TEXT, load_id TEXT)`)
		var sb strings.Builder
		sb.WriteString(`INSERT INTO r VALUES `)
		for i := 0; i < 120; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Twelve sources; some URLs share a 300-byte prefix and differ at the end.
			url := fmt.Sprintf("https://example.org/%s/%02d", strings.Repeat("segment/", 40), i%12)
			u := sqlQuote(fmt.Sprintf("u%d", i))
			if i == 7 {
				u = "NULL"
			}
			fmt.Fprintf(&sb, "(%d, %s, %s, 'L%d')", i, sqlQuote(url), u, i%3)
		}
		rmMust(t, db, sb.String())
		if indexes {
			rmMust(t, db, `CREATE INDEX i_url_load ON r(url, load_id)`)
			rmMust(t, db, `CREATE UNIQUE INDEX i_u ON r(u)`)
			rmMust(t, db, `CREATE INDEX i_url ON r(url)`)
		}
		return db
	}
	idxDB, refDB := build(true), build(false)
	urlOf := func(n int) string {
		return fmt.Sprintf("https://example.org/%s/%02d", strings.Repeat("segment/", 40), n)
	}
	statements := []string{
		`DELETE FROM r WHERE url = ` + sqlQuote(urlOf(3)) + ` AND load_id <> 'L0'`,
		`DELETE FROM r WHERE url = ` + sqlQuote(urlOf(3)),
		`DELETE FROM r WHERE url = ` + sqlQuote(urlOf(3)[:200]),
		`DELETE FROM r WHERE url IN (` + sqlQuote(urlOf(5)) + `, ` + sqlQuote(urlOf(9)) + `) AND load_id = 'L1'`,
		`DELETE FROM r WHERE u IS NULL`,
		`DELETE FROM r WHERE u = 'u7'`,
		`DELETE FROM r WHERE u = 'u8' RETURNING id, u`,
		`DELETE FROM r WHERE u IN ('u9', 'u10', 'nothing')`,
		`DELETE FROM r WHERE url = ` + sqlQuote(urlOf(11)) + ` AND u IS NOT NULL RETURNING id`,
		`DELETE FROM r WHERE url = ` + sqlQuote(urlOf(0)+"x"),
		`DELETE FROM r WHERE load_id = 'L2'`,
	}
	indexedRuns := 0
	for _, sql := range statements {
		table := w0(t, idxDB)
		stmt := mustParse(sql).(*Delete)
		if _, indexed := deleteCandidateRows(table, simpleColumnIndex(table, "r"), stmt.Where); indexed {
			indexedRuns++
		}
		got, err1 := dcRun(idxDB, sql)
		want, err2 := dcRun(refDB, sql)
		if err1 != nil || err2 != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%.120s: %v %v / %v %v", sql, got, err1, want, err2)
		}
		if !reflect.DeepEqual(table.Rows, w0(t, refDB).Rows) {
			t.Fatalf("%.120s: tables differ", sql)
		}
		rmCheckIndexes(t, table, sql)
	}
	if indexedRuns < 8 {
		t.Fatalf("only %d statements used candidates", indexedRuns)
	}
}

// A replaced source is INSERTed and then DELETEd; the compacted row slice keeps
// room, so that the next INSERT appends in place instead of copying every row
// header (see deleteSpareRows).
func TestDeleteLeavesRoomForTheNextInsert(t *testing.T) {
	for _, tc := range []struct {
		name, where string
		indexes     bool
	}{
		{"index candidates", `grp = 'g1' AND grp <> 'zz'`, true},
		{"scan with an index", `id % 7 = 0`, true},
		{"scan without an index", `id % 7 = 0`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			rmMust(t, db, `CREATE TABLE r (id INT, grp TEXT)`)
			var sb strings.Builder
			sb.WriteString(`INSERT INTO r VALUES `)
			for i := 0; i < 2000; i++ {
				if i > 0 {
					sb.WriteString(", ")
				}
				fmt.Fprintf(&sb, "(%d, 'g%d')", i, i%3)
			}
			rmMust(t, db, sb.String())
			if tc.indexes {
				rmMust(t, db, `CREATE INDEX i_grp ON r(grp)`)
			}
			table := w0(t, db)
			before := len(table.Rows)
			stmt := mustParse(`DELETE FROM r WHERE ` + tc.where).(*Delete)
			if _, indexed := deleteCandidateRows(table, simpleColumnIndex(table, "r"), stmt.Where); indexed != (tc.name == "index candidates") {
				t.Fatalf("candidates offered = %v for %s", indexed, tc.name)
			}
			rmMust(t, db, `DELETE FROM r WHERE `+tc.where)
			if len(table.Rows) >= before {
				t.Fatalf("nothing was deleted")
			}
			if spare := cap(table.Rows) - len(table.Rows); spare < 64 {
				t.Fatalf("only %d spare slots after the DELETE", spare)
			}
			first := &table.Rows[0]
			rmMust(t, db, `INSERT INTO r VALUES (5000, 'g1'), (5001, 'g2')`)
			if &table.Rows[0] != first {
				t.Fatal("the INSERT copied the row slice although the DELETE left room")
			}
			rmCheckIndexes(t, table, tc.name)
		})
	}
}
