package tinysql_test

import (
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestPublicColumnarAPI(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	result, err := tsql.ExecSQLColumnar(t.Context(), db, "default", `SELECT 42 AS answer,NULL AS missing`)
	if err != nil {
		t.Fatal(err)
	}
	var typed *tsql.ColumnarResultSet = result
	if typed.RowCount != 1 || len(typed.Values) != 2 || typed.Values[1][0] != nil {
		t.Fatal(typed)
	}
	stmt, err := tsql.ParseSQL(`SELECT 'ok' AS status`)
	if err != nil {
		t.Fatal(err)
	}
	result, err = tsql.ExecuteColumnar(t.Context(), db, "default", stmt)
	if err != nil || result.Values[0][0] != "ok" {
		t.Fatal(result, err)
	}
	if _, err := tsql.ExecSQLColumnar(t.Context(), db, "default", `SELECT (`); err == nil {
		t.Fatal("parse error lost")
	}
}
