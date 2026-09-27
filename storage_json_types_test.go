package tinysql_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// SQL-declared INT/INTEGER columns carry SQLite integer affinity. ModeJSON
// used to reload their values as float64, changing the type and rounding
// integers above 2^53.
func TestModeJSONReloadKeepsIntegerColumns(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires 64-bit INT")
	}
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *tsql.DB {
		db, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeJSON, Path: dir})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	if _, err := tsql.ExecScript(ctx, db, "default", `
		CREATE TABLE t (id INT, big INTEGER, score FLOAT, loose INTEGER);
		INSERT INTO t VALUES (42, 9007199254740993, 1.0, 1.5);`); err != nil {
		t.Fatal(err)
	}
	before, err := tsql.ExecSQL(ctx, db, "default", "SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	after, err := tsql.ExecSQL(ctx, db, "default", "SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"id", "big", "score", "loose"} {
		want, got := before.Rows[0][column], after.Rows[0][column]
		if fmt.Sprintf("%T %v", want, want) != fmt.Sprintf("%T %v", got, got) {
			t.Errorf("%s: before reload %T %v, after %T %v", column, want, want, got, got)
		}
	}
	if after.Rows[0]["big"] != 9007199254740993 {
		t.Fatalf("big integer lost precision: %v", after.Rows[0]["big"])
	}
}
