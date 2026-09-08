package sqlpackages_test

import (
	"database/sql"
	"errors"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/testutil"
	"github.com/jmoiron/sqlx"
)

type item struct {
	ID   int64          `db:"id"`
	Name string         `db:"name"`
	Note sql.NullString `db:"note"`
}

func TestIntegrationSQLX(t *testing.T) {
	db := sqlx.NewDb(testutil.Open(t, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, note TEXT)"), driver.DriverName)
	ctx := t.Context()
	for _, v := range []item{{ID: 1, Name: "O'Reilly", Note: sql.NullString{String: "present", Valid: true}}, {ID: 2, Name: "second"}} {
		if _, err := db.NamedExecContext(ctx, "INSERT INTO items (id, name, note) VALUES (:id, :name, :note)", v); err != nil {
			t.Fatal(err)
		}
	}
	var got item
	if err := db.GetContext(ctx, &got, "SELECT id, name, note FROM items WHERE id = ?", 1); err != nil {
		t.Fatal(err)
	}
	if got.Name != "O'Reilly" || !got.Note.Valid || got.Note.String != "present" {
		t.Fatalf("StructScan: %#v", got)
	}
	q, args, err := sqlx.In("SELECT id, name, note FROM items WHERE id IN (?) ORDER BY id", []int64{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	var selected []item
	if err := db.SelectContext(ctx, &selected, db.Rebind(q), args...); err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[1].Note.Valid {
		t.Fatalf("IN/NULL: %#v", selected)
	}
	named, err := db.PrepareNamedContext(ctx, "SELECT id, name, note FROM items WHERE id = :id")
	if err != nil {
		t.Fatal(err)
	}
	defer named.Close()
	if err := named.GetContext(ctx, &got, map[string]any{"id": 2}); err != nil || got.ID != 2 {
		t.Fatalf("named prepare: %#v err=%v", got, err)
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.NamedExecContext(ctx, "UPDATE items SET name = :name WHERE id = :id", map[string]any{"id": 1, "name": "temporary"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.GetContext(ctx, &got, "SELECT id, name, note FROM items WHERE id = ?", 1); err != nil || got.Name != "O'Reilly" {
		t.Fatalf("rollback: %#v err=%v", got, err)
	}
	if err := db.GetContext(ctx, &got, "SELECT id FROM items WHERE id = ?", 99); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing row: %v", err)
	}
}

func TestIntegrationSquirrel(t *testing.T) {
	for _, format := range []struct {
		name  string
		value sq.PlaceholderFormat
	}{{"question", sq.Question}, {"dollar", sq.Dollar}} {
		t.Run(format.name, func(t *testing.T) {
			db := testutil.Open(t, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
			builder := sq.StatementBuilder.PlaceholderFormat(format.value).RunWith(db)
			if r, err := builder.Insert("items").Columns("id", "name").Values(1, "O'Reilly ?").Values(2, "second").ExecContext(t.Context()); err != nil {
				t.Fatal(err)
			} else if n, _ := r.RowsAffected(); n != 2 {
				t.Fatalf("insert rows=%d", n)
			}
			var name string
			if err := builder.Select("name").From("items").Where(sq.Eq{"id": 1}).QueryRowContext(t.Context()).Scan(&name); err != nil || name != "O'Reilly ?" {
				t.Fatalf("select %q err=%v", name, err)
			}
			if _, err := builder.Update("items").Set("name", "updated").Where(sq.Eq{"id": 2}).ExecContext(t.Context()); err != nil {
				t.Fatal(err)
			}
			rows, err := builder.Select("id").From("items").Where(sq.Eq{"id": []int{1, 2}}).OrderBy("id").QueryContext(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var ids []int
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			err = rows.Err()
			rows.Close()
			if err != nil || len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
				t.Fatalf("IN: %v err=%v", ids, err)
			}
			cache := sq.NewStmtCache(db)
			defer cache.Clear()
			if err := builder.RunWith(cache).Select("name").From("items").Where(sq.Eq{"id": 2}).QueryRowContext(t.Context()).Scan(&name); err != nil || name != "updated" {
				t.Fatalf("statement cache: %q err=%v", name, err)
			}
			if _, err := builder.Delete("items").Where(sq.Eq{"id": 1}).ExecContext(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
