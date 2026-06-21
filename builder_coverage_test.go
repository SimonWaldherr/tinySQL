package tinysql

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func TestBuilderSelectVariantsToSQL(t *testing.T) {
	stmt := Select(Col("u.id"), Upper(Col("u.name"))).
		FromAs("users", "u").
		JoinAs("departments", "d", Eq(Col("u.dept_id"), Col("d.id"))).
		LeftJoinAs("profiles", "p", Eq(Col("u.id"), Col("p.user_id"))).
		Where(And(Gt(Col("u.age"), Val(18)), IsNotNull(Col("u.email")))).
		GroupBy("u.id").
		GroupByExpr(Lower(Col("d.name"))).
		Having(Ge(CountStar(), Val(1))).
		OrderBy("u.id").
		OrderByDesc("u.created_at").
		Limit(10).
		Offset(5).
		Build()

	want := "SELECT u.id, UPPER(u.name) FROM users AS u INNER JOIN departments AS d ON (u.dept_id = d.id) LEFT JOIN profiles AS p ON (u.id = p.user_id) WHERE ((u.age > 18) AND (u.email IS NOT NULL)) GROUP BY u.id, LOWER(d.name) HAVING (COUNT(*) >= 1) ORDER BY u.id, u.created_at DESC LIMIT 10 OFFSET 5"
	if got := ToSQL(stmt); got != want {
		t.Fatalf("ToSQL mismatch:\ngot:  %s\nwant: %s", got, want)
	}

	distinct := SelectDistinct(Col("name")).From("users").Build()
	if got := ToSQL(distinct); got != "SELECT DISTINCT name FROM users" {
		t.Fatalf("distinct SQL = %q", got)
	}

	star := SelectStar().From("users").LeftJoin("teams", Eq(Col("users.team_id"), Col("teams.id"))).Build()
	if got := ToSQL(star); got != "SELECT * FROM users LEFT JOIN teams ON (users.team_id = teams.id)" {
		t.Fatalf("star SQL = %q", got)
	}

	inner := Select(Col("users.id")).From("users").Join("teams", Eq(Col("users.team_id"), Col("teams.id"))).Build()
	if got := ToSQL(inner); got != "SELECT users.id FROM users INNER JOIN teams ON (users.team_id = teams.id)" {
		t.Fatalf("inner join SQL = %q", got)
	}
}

func TestBuilderCTEAndStatementRendering(t *testing.T) {
	cte := With(WithQuery{
		Name:  "ranked",
		Query: Select(Col("id"), Col("score")).From("scores").Where(Gt(Col("score"), Val(100))),
	})
	cte.projs = []engine.SelectItem{{Expr: Col("id").Build()}}
	cte.from = &engine.FromItem{Table: "ranked"}

	if got, want := ToSQL(cte.Build()), "WITH ranked AS (SELECT id, score FROM scores WHERE (score > 100)) SELECT id FROM ranked"; got != want {
		t.Fatalf("CTE SQL = %q, want %q", got, want)
	}

	insert := InsertInto("users").
		Columns("id", "name", "active").
		Values(Val(1), Val("Ada"), Val(true)).
		Values(Val(2), Null(), Val(false)).
		Build()
	if got, want := ToSQL(insert), "INSERT INTO users (id, name, active) VALUES (1, Ada, true), (2, <nil>, false)"; got != want {
		t.Fatalf("insert SQL = %q, want %q", got, want)
	}

	update := Update("users").Set("name", Trim(Col("name"))).Where(Ne(Col("id"), Val(0))).Build()
	if got, want := ToSQL(update), "UPDATE users SET name = TRIM(name) WHERE (id <> 0)"; got != want {
		t.Fatalf("update SQL = %q, want %q", got, want)
	}

	deleteStmt := DeleteFrom("users").Where(Le(Col("age"), Val(12))).Build()
	if got, want := ToSQL(deleteStmt), "DELETE FROM users WHERE (age <= 12)"; got != want {
		t.Fatalf("delete SQL = %q, want %q", got, want)
	}

	create := NewTableBuilder("events").
		Temp().
		Int("id").
		Text("name").
		Bool("active").
		Float("score").
		Timestamp("created_at").
		JSON("payload").
		Build()
	if got, want := ToSQL(create), "CREATE TEMP TABLE events (id INT, name TEXT, active BOOL, score FLOAT64, created_at TIMESTAMP, payload JSON)"; got != want {
		t.Fatalf("create SQL = %q, want %q", got, want)
	}

	if got := ToSQL(&engine.DropTable{Name: "events"}); got != "DROP TABLE events" {
		t.Fatalf("drop SQL = %q", got)
	}
	if got := ToSQL(nil); got != "UNKNOWN STATEMENT" {
		t.Fatalf("unknown SQL = %q", got)
	}
}

func TestBuilderExpressionsToSQL(t *testing.T) {
	tests := []struct {
		name string
		expr ExprBuilder
		want string
	}{
		{"and empty", And(), "true"},
		{"and single", And(Eq(Col("a"), Val(1))), "(a = 1)"},
		{"or empty", Or(), "false"},
		{"or single", Or(Eq(Col("a"), Val(1))), "(a = 1)"},
		{"or multi", Or(Eq(Col("a"), Val(1)), Lt(Col("b"), Val(2)), Eq(Col("c"), Val(3))), "(((a = 1) OR (b < 2)) OR (c = 3))"},
		{"not", Not(Eq(Col("a"), Val(1))), "NOT ((a = 1))"},
		{"is null", IsNull(Col("a")), "(a IS NULL)"},
		{"arithmetic", Div(Mul(Sub(Add(Col("a"), Val(2)), Val(3)), Val(4)), Val(5)), "((((a + 2) - 3) * 4) / 5)"},
		{"aggregates", Coalesce(Count(Col("id")), Sum(Col("score")), Avg(Col("score")), Min(Col("score")), Max(Col("score"))), "COALESCE(COUNT(id), SUM(score), AVG(score), MIN(score), MAX(score))"},
		{"strings", Concat(Lower(Col("first")), Val(" "), Upper(Col("last"))), "CONCAT(LOWER(first),  , UPPER(last))"},
		{"hashes", Coalesce(MD5(Col("a")), SHA1(Col("b")), SHA256(Col("c")), SHA512(Col("d"))), "COALESCE(MD5(a), SHA1(b), SHA256(c), SHA512(d))"},
		{"literal nil", Null(), "<nil>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := exprToSQL(tc.expr.Build()); got != tc.want {
				t.Fatalf("exprToSQL = %q, want %q", got, tc.want)
			}
		})
	}

	if got := exprToSQL(nil); got != "NULL" {
		t.Fatalf("nil expr = %q", got)
	}
	if got := exprToSQL(struct{ engine.Expr }{}); got != "?" {
		t.Fatalf("unknown expr = %q", got)
	}
}

func TestTableBuilderCreate(t *testing.T) {
	db := NewDB()
	if err := NewTableBuilder("builder_users").Int("id").Text("name").Create(db, "tenant"); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	table, err := db.Get("tenant", "builder_users")
	if err != nil {
		t.Fatalf("created table missing: %v", err)
	}
	if table.Name != "builder_users" || len(table.Cols) != 2 {
		t.Fatalf("unexpected table: %#v", table)
	}
}

func TestBuilderRightJoinRendering(t *testing.T) {
	stmt := &engine.Select{
		Projs: []engine.SelectItem{{Expr: Col("id").Build()}},
		From:  engine.FromItem{Table: "users"},
		Joins: []engine.JoinClause{{
			Type:  engine.JoinRight,
			Right: engine.FromItem{Table: "audits"},
			On:    Eq(Col("users.id"), Col("audits.user_id")).Build(),
		}},
	}

	if got := ToSQL(stmt); !strings.Contains(got, " RIGHT JOIN audits ON ") {
		t.Fatalf("right join not rendered: %q", got)
	}
}
