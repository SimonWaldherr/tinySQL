package engine

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestTextColumnsSQL(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	for _, tc := range []struct {
		sql  string
		cols []string
		rows []Row
	}{
		{`SELECT * FROM TEXT_TO_COLUMNS('Anna;Berlin;42',';')`, []string{"column1", "column2", "column3"}, []Row{{"column1": "Anna", "column2": "Berlin", "column3": "42"}}},
		{`SELECT column1 AS name, column2 AS city FROM TEXT_TO_COLUMNS('Anna;Berlin',';')`, []string{"name", "city"}, []Row{{"name": "Anna", "city": "Berlin"}}},
		{`SELECT COLUMNS_TO_TEXT(';',column1,column2,column3) AS output_text FROM TEXT_TO_COLUMNS('a;;',';')`, []string{"output_text"}, []Row{{"output_text": "a;;"}}},
		{`SELECT * FROM TEXT_TO_COLUMNS('ä🙂','')`, []string{"column1", "column2"}, []Row{{"column1": "ä", "column2": "🙂"}}},
		{`SELECT * FROM TEXT_TO_COLUMNS('a<>b<>','<>')`, []string{"column1", "column2", "column3"}, []Row{{"column1": "a", "column2": "b", "column3": ""}}},
		{`SELECT * FROM TEXT_TO_COLUMNS('',';')`, []string{"column1"}, []Row{{"column1": ""}}},
		{`SELECT COLUMNS_TO_TEXT(';','a',NULL,'',42) AS output_text`, []string{"output_text"}, []Row{{"output_text": "a;;42"}}},
		{`SELECT COLUMNS_TO_TEXT(NULL,'a','b') AS output_text`, []string{"output_text"}, []Row{{"output_text": nil}}},
	} {
		rs := execSQL(t, db, tc.sql)
		visible := make([]Row, len(rs.Rows))
		for i, row := range rs.Rows {
			visible[i] = make(Row)
			for _, col := range rs.Cols {
				visible[i][col] = row[col]
			}
		}
		if !reflect.DeepEqual(rs.Cols, tc.cols) || !reflect.DeepEqual(visible, tc.rows) {
			t.Fatalf("%s: %v", tc.sql, rs)
		}
		stream, err := ExecuteStream(t.Context(), db, "default", mustParse(tc.sql))
		if err != nil {
			t.Fatal(err)
		}
		var streamed []Row
		for stream.Next() {
			streamed = append(streamed, stream.Row())
		}
		err = stream.Err()
		stream.Close()
		if err != nil || !reflect.DeepEqual(streamed, rs.Rows) {
			t.Fatalf("stream: %v %v", streamed, err)
		}
	}
	for _, sql := range []string{`SELECT * FROM TEXT_TO_COLUMNS(NULL,';')`, `SELECT * FROM TEXT_TO_COLUMNS('a',NULL)`} {
		if rs := execSQL(t, db, sql); len(rs.Rows) != 0 {
			t.Fatal(rs)
		}
	}
	for _, sql := range []string{`SELECT * FROM TEXT_TO_COLUMNS('a')`, `SELECT * FROM TEXT_TO_COLUMNS('a',';',1)`, `SELECT COLUMNS_TO_TEXT(';')`, `SELECT TEXT_TO_COLUMNS('a',';')`} {
		if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err == nil {
			t.Fatal("accepted invalid call:", sql)
		}
	}
	stmt := mustParse(`SELECT * FROM TEXT_TO_COLUMNS('a;b',';')`).(*Select)
	param := stmt.From.TableFunc.Args[0].(*Literal)
	param.Parameter = true
	for _, value := range []string{"a;b", "x;y;z"} {
		param.Val = value
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil || len(rs.Cols) != len(strings.Split(value, ";")) {
			t.Fatalf("stale schema: %v %v", rs, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (&textToColumnsFunc{}).Execute(ctx, []Expr{&Literal{Val: "a"}, &Literal{Val: ";"}}, ExecEnv{}, nil); err == nil {
		t.Fatal("ignored cancellation")
	}
}
