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

func TestSimpleJoinFastPathCachesPlanForRepeatedStatement(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE left_rows (id INT, name TEXT)`)
	execSQL(t, db, `CREATE TABLE right_rows (id INT, value TEXT)`)
	execSQL(t, db, `INSERT INTO left_rows VALUES (1, 'one')`)
	execSQL(t, db, `INSERT INTO right_rows VALUES (1, 'value')`)

	stmt := mustParse(`SELECT l.name, r.value FROM left_rows l JOIN right_rows r ON l.id = r.id`).(*Select)
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}
	first, ok, err := buildSimpleJoinPlan(env, stmt)
	if err != nil || !ok {
		t.Fatalf("first join plan = %#v, ok=%v, err=%v", first, ok, err)
	}
	second, ok, err := buildSimpleJoinPlan(env, stmt)
	if err != nil || !ok {
		t.Fatalf("second join plan = %#v, ok=%v, err=%v", second, ok, err)
	}
	if first != second {
		t.Fatal("repeated statement did not reuse its simple join plan")
	}

	rs, err := Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["l.name"] != "one" || rs.Rows[0]["r.value"] != "value" {
		t.Fatalf("cached join result = %#v", rs)
	}
}
