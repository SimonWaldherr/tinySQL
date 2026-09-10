package engine

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func starChoicesFixture(tb testing.TB, n int, indexed bool) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	table := storage.NewTable("items", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "tag", Type: storage.TextType}, {Name: "value", Type: storage.IntType}, {Name: "payload", Type: storage.BlobType}}, false)
	for i := 0; i < n; i++ {
		var tag any = fmt.Sprintf("tag%d", i%17)
		if i%11 == 0 {
			tag = nil
		}
		table.Rows = append(table.Rows, []any{i, tag, i % 100, []byte{byte(i), byte(i >> 8)}})
	}
	if indexed {
		for _, col := range []string{"id", "tag"} {
			if err := table.CreateSecondaryIndex("idx_"+col, []string{col}, false); err != nil {
				tb.Fatal(err)
			}
		}
	}
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	return db
}

func TestSelectStarChoicesMatchScan(t *testing.T) {
	indexed := starChoicesFixture(t, 1000, true)
	scanned := starChoicesFixture(t, 1000, false)
	queries := []string{
		`SELECT * FROM items WHERE id IN (999,3,40,3,NULL)`,
		`SELECT * FROM items WHERE id IN (1001,1002,NULL)`,
		`SELECT * FROM items WHERE id IN (NULL)`,
		`SELECT * FROM items WHERE id NOT IN (3,NULL)`,
		`SELECT * FROM items WHERE NOT (id IN (3,NULL))`,
		`SELECT * FROM items WHERE id=3 OR id=999 OR 40=id OR id=3`,
		`SELECT * FROM items WHERE id=3 OR tag='tag1'`,
		`SELECT * FROM items WHERE id=3 OR value>90`,
		`SELECT * FROM items WHERE value>2 AND id IN (3,7,999) LIMIT 2 OFFSET 1`,
		`SELECT * FROM items i WHERE i.id IN (3,7,999) ORDER BY i.id DESC LIMIT 2`,
		`SELECT * FROM items WHERE id IN (3.0,7.0,999.0)`,
		`SELECT * FROM items WHERE tag IN ('tag1','tag2',NULL)`,
		`SELECT * FROM items WHERE tag IN ('tag1','tag2','tag3','tag4','tag5','tag6','tag7','tag8')`,
	}
	for _, q := range queries {
		got := rangeExec(t, indexed, q)
		want := rangeExec(t, scanned, q)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: indexed and scan results differ", q)
		}
	}
	// Oversized lists retain bounded planning work by falling back to a scan.
	values := make([]string, 100)
	for i := range values {
		values[i] = fmt.Sprint(i)
	}
	q := `SELECT * FROM items WHERE id IN (` + strings.Join(values, ",") + `)`
	if !reflect.DeepEqual(rangeExec(t, indexed, q), rangeExec(t, scanned, q)) {
		t.Fatal("large list mismatch")
	}
}

func BenchmarkSelectStarChoices(b *testing.B) {
	db := starChoicesFixture(b, 50000, true)
	for _, query := range []struct{ name, sql string }{
		{"in", `SELECT * FROM items WHERE id IN (3,200,49000)`},
		{"or", `SELECT * FROM items WHERE id=3 OR id=200 OR id=49000`},
		{"equality", `SELECT * FROM items WHERE id=200`},
		{"scan", `SELECT * FROM items WHERE value>95 LIMIT 20`},
	} {
		b.Run(query.name, func(b *testing.B) {
			stmt := mustParse(query.sql)
			if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSelectStarChoicesPlansAndParameters(t *testing.T) {
	db := starChoicesFixture(t, 1000, true)
	env := ExecEnv{db: db, tenant: "default", ctx: context.Background()}
	for _, sql := range []string{`SELECT * FROM items WHERE id IN (1,2)`, `SELECT * FROM items WHERE id=1 OR id=2`} {
		stmt := mustParse(sql).(*Select)
		var choices []*Binary
		if !collectIndexChoices(stmt.Where, &choices) {
			t.Fatal("missing choices")
		}
		for _, choice := range choices {
			choice.Right.(*Literal).Parameter = true
		}
		plan, ok, err := buildSimpleSelectPlan(env, stmt)
		if err != nil || !ok || plan.scanType != "INDEX MULTI SEEK" || len(plan.rowIDs) != 2 {
			t.Fatalf("plan: %+v %v", plan, err)
		}
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 2 {
			t.Fatal(rs, err)
		}
		choices[0].Right.(*Literal).Val = 2000
		rs, err = Execute(context.Background(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["id"] != 2 {
			t.Fatal("stale parameters", rs, err)
		}
		rangeExec(t, db, `INSERT INTO items VALUES (2000,'new',10,NULL)`)
		rs, err = Execute(context.Background(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 2 {
			t.Fatal("stale index", rs, err)
		}
		rangeExec(t, db, `DELETE FROM items WHERE id=2000`)
	}
	for _, q := range []string{`SELECT * FROM items WHERE absent IN (NULL)`, `SELECT * FROM items WHERE absent=NULL OR id=1`} {
		if _, err := Execute(context.Background(), db, "default", mustParse(q)); err == nil {
			t.Fatalf("lost unknown-column error: %s", q)
		}
	}
	fallback := mustParse(`SELECT * FROM items WHERE id=1 OR value>90`).(*Select)
	plan, ok, err := buildSimpleSelectPlan(env, fallback)
	if err != nil || !ok || plan.rowIDs != nil {
		t.Fatalf("unsafe partial OR seek: %+v %v", plan, err)
	}
}

func TestSelectStarChoicesMixedNumeric(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("mixed", []storage.Column{{Name: "id", Type: storage.FloatType}, {Name: "label", Type: storage.TextType}}, false)
	for i, v := range []any{1, int64(1), 1.0, 0, 0.0, math.Copysign(0, -1), nil, "1", 2} {
		table.Rows = append(table.Rows, []any{v, fmt.Sprint(i)})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	queries := []string{`SELECT * FROM mixed WHERE id IN (1,0,NULL)`, `SELECT * FROM mixed WHERE id=1.0 OR id=0.0`, `SELECT * FROM mixed WHERE id IN (1,1,1.0)`}
	expected := make([]*ResultSet, len(queries))
	for i, q := range queries {
		expected[i] = rangeExec(t, db, q)
	}
	rangeExec(t, db, `CREATE INDEX mixed_id ON mixed(id)`)
	for i, q := range queries {
		if got := rangeExec(t, db, q); !reflect.DeepEqual(got, expected[i]) {
			t.Fatalf("mixed numeric mismatch for %s", q)
		}
	}
}

func TestSelectStarChoicesConstraintIndex(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	rangeExec(t, db, `CREATE TABLE keyed (id INT PRIMARY KEY, value TEXT)`)
	rangeExec(t, db, `INSERT INTO keyed VALUES (5,'five'),(3,'three'),(1,'one')`)
	stmt := mustParse(`SELECT * FROM keyed WHERE id IN (1,5.0,1)`).(*Select)
	plan, ok, err := buildSimpleSelectPlan(ExecEnv{db: db, tenant: "default", ctx: context.Background()}, stmt)
	if err != nil || !ok || plan.scanType != "INDEX MULTI SEEK" {
		t.Fatalf("constraint path not used: %+v %v", plan, err)
	}
	rs, err := Execute(context.Background(), db, "default", stmt)
	if err != nil || len(rs.Rows) != 2 || rs.Rows[0]["id"] != 5 || rs.Rows[1]["id"] != 1 {
		t.Fatalf("constraint union order/duplicates: %+v %v", rs, err)
	}
}
