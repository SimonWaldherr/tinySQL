package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestStringSplitLiteralDelimiters(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	for _, tc := range []struct {
		sql    string
		fields []string
	}{
		{`SELECT value,ordinal FROM STRING_SPLIT('a<>b<><>','<>',1) ORDER BY ordinal`, []string{"a", "b", "", ""}},
		{`SELECT value,ordinal FROM STRING_SPLIT('ä🙂漢🙂','🙂',1) ORDER BY ordinal`, []string{"ä", "漢", ""}},
		{`SELECT value,ordinal FROM STRING_SPLIT('a::b','::',1) ORDER BY ordinal`, []string{"a", "b"}},
		{`SELECT value,ordinal FROM STRING_SPLIT('x',':',1) ORDER BY ordinal`, []string{"x"}},
	} {
		rs := execSQL(t, db, tc.sql)
		if len(rs.Rows) != len(tc.fields) {
			t.Fatalf("%s: %v", tc.sql, rs.Rows)
		}
		for i, field := range tc.fields {
			if rs.Rows[i]["value"] != field || rs.Rows[i]["ordinal"] != i+1 {
				t.Fatalf("%s row %d: %v", tc.sql, i, rs.Rows[i])
			}
		}
	}
	for _, sql := range []string{`SELECT * FROM STRING_SPLIT(NULL,',',1)`, `SELECT * FROM STRING_SPLIT('a',NULL,1)`, `SELECT * FROM STRING_SPLIT('',',',1)`} {
		rs := execSQL(t, db, sql)
		if len(rs.Rows) != 0 || !reflect.DeepEqual(rs.Cols, []string{"ordinal", "value"}) {
			t.Fatalf("%s: %v", sql, rs)
		}
	}
	stmt := mustParse(`SELECT * FROM STRING_SPLIT('a::b','::',1)`).(*Select)
	flag := stmt.From.TableFunc.Args[2].(*Literal)
	flag.Parameter = true
	for _, ordinal := range []int{0, 1, 0} {
		flag.Val = ordinal
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || len(rs.Cols) != 1+ordinal {
			t.Fatalf("ordinal %d: %v %v", ordinal, rs, err)
		}
	}
	for _, sql := range []string{
		`SELECT * FROM STRING_SPLIT('a')`, `SELECT * FROM STRING_SPLIT('a','',1)`,
		`SELECT * FROM STRING_SPLIT('a',',',2)`, `SELECT * FROM STRING_SPLIT('a',',',NULL)`,
		`SELECT * FROM STRING_SPLIT('a',',',1.5)`, `SELECT STRING_SPLIT('a',',')`,
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err == nil {
			t.Fatalf("accepted %s", sql)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (&stringSplitFunc{}).Execute(ctx, []Expr{&Literal{Val: "a"}, &Literal{Val: ","}}, ExecEnv{}, nil); err == nil {
		t.Fatal("ignored cancellation")
	}
}
