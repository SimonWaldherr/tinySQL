package engine

import (
	"reflect"
	"testing"
)

func TestParseCachedInserts(t *testing.T) {
	stmt, err := NewParser(`insert /* batch */ CaChEd into "Content" (id) values (1), (2)`).ParseStatement()
	insert, ok := stmt.(*Insert)
	if err != nil || !ok || !insert.Cached || insert.Table != "Content" || len(insert.Rows) != 2 || !reflect.DeepEqual(insert.Cols, []string{"id"}) {
		t.Fatalf("cached VALUES = %#v, %v", stmt, err)
	}

	stmt, err = NewParser(`WITH source AS (SELECT 1 AS id) INSERT CACHED INTO content SELECT id FROM source`).ParseStatement()
	insert, ok = stmt.(*Insert)
	if err != nil || !ok || !insert.Cached || insert.Select == nil || len(insert.Select.CTEs) != 1 {
		t.Fatalf("cached SELECT with CTE = %#v, %v", stmt, err)
	}

	for _, sql := range []string{
		`INSERT CACHED DELAYED INTO content VALUES (1)`,
		`INSERT DELAYED CACHED INTO content VALUES (1)`,
		`INSERT "CACHED" INTO content VALUES (1)`,
		`FLUSH INSERTS`,
		`FLUSH INSERTS INTO`,
		`DISCARD INSERTS FROM`,
		`DISCARD INSERTS content junk`,
		`FLUSH "INSERTS" content`,
	} {
		if stmt, err := NewParser(sql).ParseStatement(); err == nil {
			t.Errorf("parsed %q as %#v", sql, stmt)
		}
	}
}

func TestParseInsertBufferCommands(t *testing.T) {
	for _, test := range []struct {
		sql  string
		want Statement
	}{
		{`flush inserts content`, &FlushInserts{Table: "content"}},
		{`FLUSH /* batch */ INSERTS INTO "Content";`, &FlushInserts{Table: "Content"}},
		{`discard inserts content`, &DiscardInserts{Table: "content"}},
		{`DISCARD INSERTS FROM "Content"`, &DiscardInserts{Table: "Content"}},
	} {
		stmt, err := NewParser(test.sql).ParseStatement()
		if err != nil || !reflect.DeepEqual(stmt, test.want) {
			t.Errorf("parse %q = %#v, %v; want %#v", test.sql, stmt, err, test.want)
		}
	}
}

func TestCachedInsertWordsRemainIdentifiers(t *testing.T) {
	for _, name := range []string{"cached", "flush", "discard", "inserts"} {
		stmt, err := NewParser(name).ParseStatement()
		selectStmt, ok := stmt.(*Select)
		if err != nil || !ok || selectStmt.From.Table != name {
			t.Errorf("bare table %q = %#v, %v", name, stmt, err)
		}
	}

	stmt, err := NewParser(`INSERT INTO cached (flush, discard, inserts) VALUES (1, 2, 3)`).ParseStatement()
	insert, ok := stmt.(*Insert)
	if err != nil || !ok || insert.Cached || insert.Table != "cached" || !reflect.DeepEqual(insert.Cols, []string{"flush", "discard", "inserts"}) {
		t.Fatalf("ordinary INSERT = %#v, %v", stmt, err)
	}
}
