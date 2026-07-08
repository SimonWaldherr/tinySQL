package tinyorm

import (
	"context"
	"errors"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

type testUser struct {
	ID     int     `db:"id,pk"`
	Name   string  `db:"name"`
	Age    int     `db:"age"`
	Score  float64 `db:"score"`
	Active bool    `db:"active"`
	Skip   string  `db:"-"`
}

func (testUser) TableName() string { return "users" }

func TestAutoMigrateInsertFindSelectDelete(t *testing.T) {
	ctx := context.Background()
	orm := New(tinysql.NewDB(), "default")

	if err := orm.AutoMigrate(ctx, testUser{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	if err := orm.Insert(ctx, testUser{ID: 1, Name: "Alice", Age: 32, Score: 9.5, Active: true, Skip: "ignored"}); err != nil {
		t.Fatalf("Insert Alice: %v", err)
	}
	if err := orm.Insert(ctx, testUser{ID: 2, Name: "Bob", Age: 41, Score: 7.25, Active: false}); err != nil {
		t.Fatalf("Insert Bob: %v", err)
	}

	var got testUser
	if err := orm.FindByPK(ctx, &got, 1); err != nil {
		t.Fatalf("FindByPK: %v", err)
	}
	if got.ID != 1 || got.Name != "Alice" || got.Age != 32 || got.Score != 9.5 || !got.Active {
		t.Fatalf("unexpected user: %#v", got)
	}

	var users []testUser
	if err := orm.Select(ctx, &users, "age > :min_age", map[string]any{"min_age": 35}); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(users) != 1 || users[0].Name != "Bob" {
		t.Fatalf("unexpected selected users: %#v", users)
	}

	if err := orm.DeleteByPK(ctx, testUser{}, 2); err != nil {
		t.Fatalf("DeleteByPK: %v", err)
	}
	if err := orm.FindByPK(ctx, &got, 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindByPK after delete error = %v, want ErrNotFound", err)
	}
}

func TestExecNamedAndBindNamed(t *testing.T) {
	ctx := context.Background()
	orm := New(tinysql.NewDB(), "")
	if err := orm.AutoMigrate(ctx, testUser{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	if _, err := orm.Exec(ctx,
		"INSERT INTO users (id, name, age, score, active) VALUES (:id, :name, :age, :score, @active)",
		testUser{ID: 7, Name: "O'Hara", Age: 28, Score: 3.5, Active: true},
	); err != nil {
		t.Fatalf("Exec named: %v", err)
	}

	var got testUser
	if err := orm.FindByPK(ctx, &got, 7); err != nil {
		t.Fatalf("FindByPK: %v", err)
	}
	if got.Name != "O'Hara" {
		t.Fatalf("name = %q, want O'Hara", got.Name)
	}

	bound, err := BindNamed("SELECT ':kept', :name", map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("BindNamed: %v", err)
	}
	if !strings.Contains(bound, "':kept'") || !strings.Contains(bound, "'A'") {
		t.Fatalf("bound SQL = %s", bound)
	}
}

func TestColumns(t *testing.T) {
	cols, err := Columns(testUser{})
	if err != nil {
		t.Fatalf("Columns: %v", err)
	}
	got := strings.Join(cols, ",")
	want := "active,age,id,name,score"
	if got != want {
		t.Fatalf("columns = %s, want %s", got, want)
	}
}
