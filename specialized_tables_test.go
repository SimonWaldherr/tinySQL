package tinysql

import "testing"

func TestCreateSpecializedTable(t *testing.T) {
	db := NewDB()
	defer db.Close()
	for _, kind := range []TableKind{KeyValueTable, DocumentTable, TimeSeriesTable} {
		if err := CreateSpecializedTable(t.Context(), db, "default", string(kind), kind); err != nil {
			t.Fatal(err)
		}
	}
	if err := CreateSpecializedTable(t.Context(), db, "other", "keyvalue", KeyValueTable, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecSQL(t.Context(), db, "other", `INSERT INTO keyvalue VALUES ('a', X'0102')`); err != nil {
		t.Fatal(err)
	}
	rs, err := ExecSQL(t.Context(), db, "default", `SELECT * FROM keyvalue`)
	if err != nil || len(rs.Rows) != 0 {
		t.Fatalf("tenant isolation: %v %v", rs, err)
	}
	if err := CreateSpecializedTable(t.Context(), nil, "default", "x", KeyValueTable); err == nil {
		t.Fatal("nil DB accepted")
	}
	if err := CreateSpecializedTable(t.Context(), db, "default", "x", TableKind("bad")); err == nil {
		t.Fatal("unknown kind accepted")
	}
	if err := CreateSpecializedTable(t.Context(), db, "default", "x", KeyValueTable, "one"); err == nil {
		t.Fatal("wrong columns accepted")
	}
}
