package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestInsertReturningStarAndExpression(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE items (id INT, name TEXT, qty INT)")); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	rs, err := Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (1, 'apple', 3), (2, 'pear', 5) RETURNING *"))
	if err != nil {
		t.Fatalf("INSERT RETURNING * failed: %v", err)
	}
	if got, want := rs.Cols, []string{"id", "name", "qty"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RETURNING * cols = %v, want %v", got, want)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("returned rows = %d, want 2", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 1 || rs.Rows[0]["name"] != "apple" || rs.Rows[1]["qty"] != 5 {
		t.Fatalf("unexpected returned rows: %#v", rs.Rows)
	}

	rs, err = Execute(ctx, db, "default", mustParse("INSERT INTO items (id, name, qty) VALUES (3, 'plum', 8) RETURNING id, qty + 2 AS next_qty"))
	if err != nil {
		t.Fatalf("INSERT RETURNING expressions failed: %v", err)
	}
	if got, want := rs.Cols, []string{"id", "next_qty"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RETURNING expression cols = %v, want %v", got, want)
	}
	if rs.Rows[0]["id"] != 3 || rs.Rows[0]["next_qty"] != float64(10) {
		t.Fatalf("unexpected expression row: %#v", rs.Rows[0])
	}
}

func TestUpdateReturningUpdatedRows(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE items (id INT, name TEXT, qty INT)")); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (1, 'apple', 3), (2, 'pear', 5), (3, 'plum', 8)")); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rs, err := Execute(ctx, db, "default", mustParse("UPDATE items SET name = 'updated' WHERE qty >= 5 RETURNING id, name"))
	if err != nil {
		t.Fatalf("UPDATE RETURNING failed: %v", err)
	}
	if got, want := rs.Cols, []string{"id", "name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RETURNING cols = %v, want %v", got, want)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("returned rows = %d, want 2", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 2 || rs.Rows[0]["name"] != "updated" || rs.Rows[1]["id"] != 3 {
		t.Fatalf("unexpected update returning rows: %#v", rs.Rows)
	}
}

func TestDeleteReturningDeletedRows(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE items (id INT, name TEXT, qty INT)")); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (1, 'apple', 3), (2, 'pear', 5), (3, 'plum', 8)")); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rs, err := Execute(ctx, db, "default", mustParse("DELETE FROM items WHERE id <> 2 RETURNING id, name"))
	if err != nil {
		t.Fatalf("DELETE RETURNING failed: %v", err)
	}
	if got, want := rs.Cols, []string{"id", "name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RETURNING cols = %v, want %v", got, want)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("returned rows = %d, want 2", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 1 || rs.Rows[0]["name"] != "apple" || rs.Rows[1]["id"] != 3 {
		t.Fatalf("unexpected delete returning rows: %#v", rs.Rows)
	}

	remaining, err := Execute(ctx, db, "default", mustParse("SELECT id FROM items"))
	if err != nil {
		t.Fatalf("SELECT remaining failed: %v", err)
	}
	if len(remaining.Rows) != 1 || remaining.Rows[0]["id"] != 2 {
		t.Fatalf("unexpected remaining rows: %#v", remaining.Rows)
	}
}
