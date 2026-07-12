package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSQLiteDeclaredTypesAndAffinities(t *testing.T) {
	stmt, err := NewParser(`CREATE TABLE poi (
		id INTEGER,
		name VARCHAR(255),
		score DOUBLE PRECISION,
		rank NUMERIC(12,2),
		note CLOB,
		payload,
		metadata ANY
	)`).ParseStatement()
	if err != nil {
		t.Fatalf("parse SQLite declarations: %v", err)
	}
	create := stmt.(*CreateTable)
	got := create.Cols
	if len(got) != 7 {
		t.Fatalf("columns = %d, want 7", len(got))
	}
	wantAffinity := []storage.SQLiteAffinity{
		storage.AffinityInteger,
		storage.AffinityText,
		storage.AffinityReal,
		storage.AffinityNumeric,
		storage.AffinityText,
		storage.AffinityBlob,
		storage.AffinityBlob,
	}
	for i, want := range wantAffinity {
		if got[i].Affinity != want {
			t.Errorf("%s affinity = %s, want %s", got[i].Name, got[i].Affinity, want)
		}
	}
	if got[1].DeclaredType != "VARCHAR(255)" || got[3].DeclaredType != "NUMERIC(12,2)" {
		t.Fatalf("type decorations not retained: %#v", got)
	}
	if got[5].Type != storage.InterfaceType || got[6].Type != storage.InterfaceType {
		t.Fatalf("typeless and ANY columns must remain dynamically typed: %#v", got)
	}
}

func TestSQLiteAffinityUsesLosslessCoercion(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execAffinitySQL(t, ctx, db, `CREATE TABLE values_t (
		integer_value INTEGER,
		real_value REAL,
		text_value VARCHAR(12),
		numeric_value NUMERIC,
		untyped_value
	)`)
	execAffinitySQL(t, ctx, db, `INSERT INTO values_t VALUES ('42', '3.5', 7, '2.0', 'unchanged')`)
	execAffinitySQL(t, ctx, db, `INSERT INTO values_t VALUES ('not-an-int', 'not-a-real', X'0102', 'not-a-number', X'0102')`)

	table, err := db.Get("default", "values_t")
	if err != nil {
		t.Fatalf("values_t not found: %v", err)
	}
	if got := table.Rows[0]; got[0] != 42 || got[1] != 3.5 || got[2] != "7" || got[3] != 2 || got[4] != "unchanged" {
		t.Fatalf("first row = %#v", got)
	}
	if got := table.Rows[1]; got[0] != "not-an-int" || got[1] != "not-a-real" || got[3] != "not-a-number" {
		t.Fatalf("lossy values must retain their storage class: %#v", got)
	}
	if got, ok := table.Rows[1][2].([]byte); !ok || string(got) != "\x01\x02" {
		t.Fatalf("TEXT affinity must not coerce BLOB: %#v", table.Rows[1][2])
	}
	if got, ok := table.Rows[1][4].([]byte); !ok || string(got) != "\x01\x02" {
		t.Fatalf("typeless BLOB = %#v, want []byte{1,2}", table.Rows[1][4])
	}
}

func execAffinitySQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if _, err := Execute(ctx, db, "default", stmt); err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
}
