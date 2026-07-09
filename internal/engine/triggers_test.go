package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestTriggerWhenCondition(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	for _, sql := range []string{
		`CREATE TABLE orders (id INT, amount INT)`,
		`CREATE TABLE audit_log (id INT, note TEXT)`,
		`CREATE TRIGGER audit_large_order AFTER INSERT ON orders
			FOR EACH ROW WHEN (NEW.amount > 100)
			BEGIN
				INSERT INTO audit_log VALUES (NEW.id, 'large');
			END`,
		`INSERT INTO orders VALUES (1, 50)`,
		`INSERT INTO orders VALUES (2, 150)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	rs, err := Execute(ctx, db, "default", mustParse(`SELECT id, note FROM audit_log`))
	if err != nil {
		t.Fatalf("select audit_log: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected one trigger row, got %#v", rs.Rows)
	}
	if rs.Rows[0]["id"] != 2 || rs.Rows[0]["note"] != "large" {
		t.Fatalf("unexpected trigger row: %#v", rs.Rows[0])
	}
}
