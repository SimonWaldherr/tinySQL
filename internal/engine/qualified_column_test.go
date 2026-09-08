package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestQuotedQualifiedColumns(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	ctx := context.Background()
	for _, q := range []string{"CREATE TABLE products (id INT, price INT)", "INSERT INTO products VALUES (1, 10), (2, 20), (3, 30)"} {
		st, err := NewParser(q).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Execute(ctx, db, "default", st); err != nil {
			t.Fatal(err)
		}
	}
	pairs := [][2]string{
		{`SELECT * FROM "products" WHERE "products"."id" > 1 ORDER BY "products"."id" DESC LIMIT 1`, `SELECT * FROM products WHERE products.id > 1 ORDER BY products.id DESC LIMIT 1`},
		{`SELECT "p"."id" AS id FROM products p WHERE "p"."price" >= 10 ORDER BY "p"."id" DESC`, `SELECT p.id AS id FROM products p WHERE p.price >= 10 ORDER BY p.id DESC`},
		{`SELECT "p"."id" AS id FROM products p JOIN products q ON "p"."id" = "q"."id" ORDER BY id`, `SELECT p.id AS id FROM products p JOIN products q ON p.id = q.id ORDER BY id`},
	}
	for _, pair := range pairs {
		var results [2]*ResultSet
		for i, q := range pair {
			st, err := NewParser(q).ParseStatement()
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			results[i], err = Execute(ctx, db, "default", st)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		if !reflect.DeepEqual(results[0], results[1]) {
			t.Fatalf("quoted differs: %#v vs %#v", results[0], results[1])
		}
	}
	for _, q := range []string{`SELECT "p". FROM products p`, `SELECT id FROM products ORDER BY "products".`, `SELECT id FROM products ORDER BY "products".."id"`} {
		if _, err := NewParser(q).ParseStatement(); err == nil {
			t.Fatalf("accepted malformed column: %s", q)
		}
	}
}
