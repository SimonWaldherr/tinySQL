package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSimpleJoinFastPathPushesSingleSideWhereTerms(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE users (id INT, enabled BOOL)`)
	execSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, state TEXT)`)
	for _, sql := range []string{
		`INSERT INTO users VALUES (1, true)`,
		`INSERT INTO users VALUES (2, false)`,
		`INSERT INTO orders VALUES (10, 1, 'open')`,
		`INSERT INTO orders VALUES (11, 1, 'closed')`,
		`INSERT INTO orders VALUES (12, 2, 'open')`,
	} {
		execSQL(t, db, sql)
	}

	stmt := mustParse(`
		SELECT u.id AS user_id, o.id AS order_id
		FROM users u
		JOIN orders o ON u.id = o.user_id
		WHERE u.enabled = true AND o.state = 'open'
	`).(*Select)
	plan, ok, err := buildSimpleJoinPlan(ExecEnv{ctx: context.Background(), tenant: "default", db: db}, stmt)
	if err != nil || !ok {
		t.Fatalf("simple join plan = %#v, ok=%v, err=%v", plan, ok, err)
	}
	if plan.leftFilter == nil || plan.rightFilter == nil || plan.where != nil {
		t.Fatalf("expected two pushed filters and no residual, got left=%v right=%v residual=%#v", plan.leftFilter != nil, plan.rightFilter != nil, plan.where)
	}

	rs, err := Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("joined rows = %#v, want one", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["user_id"], 1, "user id")
	expectInt(t, rs.Rows[0]["order_id"], 10, "order id")
}
