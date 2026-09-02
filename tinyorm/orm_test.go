package tinyorm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
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

// taggedUser intentionally uses GORM-style Go struct tags. tinyorm supports
// these alongside its compact db tags so models can stay declarative.
type taggedUser struct {
	ID     int    `gorm:"column:id;primaryKey"`
	Email  string `gorm:"column:email;unique;not null;type:VARCHAR(120)"`
	State  string `gorm:"column:state;default:'active'"`
	Hidden string `gorm:"-"`
}

func (taggedUser) TableName() string { return "tagged_users" }

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

func TestSelectPointerSlice(t *testing.T) {
	ctx := context.Background()
	orm := New(tinysql.NewDB(), "default")
	if err := orm.AutoMigrate(ctx, testUser{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	for _, user := range []testUser{
		{ID: 1, Name: "Ada", Age: 36, Score: 9.5, Active: true},
		{ID: 2, Name: "Linus", Age: 55, Score: 8.0, Active: false},
	} {
		if err := orm.Insert(ctx, user); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	var users []*testUser
	if err := orm.Select(ctx, &users, "age >= :min_age", map[string]any{"min_age": 0}); err != nil {
		t.Fatalf("Select pointer slice: %v", err)
	}
	if len(users) != 2 || users[0] == nil || users[1] == nil {
		t.Fatalf("Select pointer slice = %#v, want two non-nil users", users)
	}
	if users[0].Name != "Ada" || users[1].Name != "Linus" {
		t.Fatalf("unexpected users: %#v", users)
	}
}

func TestMetadataCachesAreConcurrentSafe(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			meta, err := describeModel(testUser{})
			if err != nil || meta.primaryField() == nil || meta.selectList() == "" {
				t.Errorf("describeModel() = %#v, %v", meta, err)
			}
			fields := namedFieldsFor(reflect.TypeOf(testUser{}))
			if len(fields) == 0 {
				t.Error("namedFieldsFor returned no fields")
			}
		}()
	}
	wg.Wait()
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

	bound, err = BindNamed("SELECT :account_id, @AccountID", struct {
		AccountID int `db:"account_id"`
	}{AccountID: 42})
	if err != nil {
		t.Fatalf("BindNamed tagged struct: %v", err)
	}
	if bound != "SELECT 42, 42" {
		t.Fatalf("tagged bound SQL = %q", bound)
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

func TestGORMStyleTagsDefaultsAndSave(t *testing.T) {
	ctx := context.Background()
	orm := New(tinysql.NewDB(), "")
	if err := orm.AutoMigrate(ctx, taggedUser{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}

	// Omit state deliberately: its default comes from the Go struct tag and
	// is applied by the generated table schema.
	if _, err := orm.Exec(ctx, "INSERT INTO tagged_users (id, email) VALUES (:id, :email)", map[string]any{
		"id": 1, "email": "ada@example.test",
	}); err != nil {
		t.Fatalf("insert with default: %v", err)
	}

	var got taggedUser
	if err := orm.First(ctx, &got, "email = :email", struct {
		Email string `gorm:"column:email"`
	}{Email: "ada@example.test"}); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.State != "active" || got.Hidden != "" {
		t.Fatalf("scanned tagged row = %#v", got)
	}
	got.State = "disabled"
	if err := orm.Save(ctx, &got); err != nil {
		t.Fatalf("Save: %v", err)
	}
	var byPK taggedUser
	if err := orm.FindByPK(ctx, &byPK, 1); err != nil {
		t.Fatalf("FindByPK after Save: %v", err)
	}
	if byPK.State != "disabled" {
		t.Fatalf("saved state = %q, want disabled", byPK.State)
	}
}

func TestOtherORMTagStyles(t *testing.T) {
	tests := []struct {
		name  string
		model any
		want  fieldMeta
	}{
		{
			name: "bun",
			model: struct {
				ID    int    `bun:"id,pk"`
				State string `bun:"state,notnull,default:'queued'"`
			}{},
			want: fieldMeta{column: "state", notNull: true, defaultSQL: "'queued'", sqlType: "TEXT"},
		},
		{
			name: "go-pg",
			model: struct {
				ID    int    `pg:"id,pk"`
				Email string `pg:"email,unique"`
			}{},
			want: fieldMeta{column: "email", unique: true, sqlType: "TEXT"},
		},
		{
			name: "xorm",
			model: struct {
				ID   int    `xorm:"pk 'user_id'"`
				Name string `xorm:"varchar(120) notnull 'display_name'"`
			}{},
			want: fieldMeta{column: "display_name", notNull: true, sqlType: "varchar(120)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, err := describeModel(tt.model)
			if err != nil {
				t.Fatalf("describeModel: %v", err)
			}
			var got *fieldMeta
			for i := range meta.fields {
				if meta.fields[i].column == tt.want.column {
					got = &meta.fields[i]
					break
				}
			}
			if got == nil || got.unique != tt.want.unique || got.notNull != tt.want.notNull || got.defaultSQL != tt.want.defaultSQL || got.sqlType != tt.want.sqlType {
				t.Fatalf("mapped field = %#v, want %#v", got, tt.want)
			}
		})
	}
}
