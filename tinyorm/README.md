# tinyorm

`tinyorm` maps Go structs to tinySQL tables and keeps the common CRUD path
small and explicit.

```go
type User struct {
	ID    int    `gorm:"column:id;primaryKey"`
	Email string `gorm:"column:email;unique;not null;type:VARCHAR(120)"`
	State string `gorm:"column:state;default:'active'"`
	Secret string `gorm:"-"`
}

func (User) TableName() string { return "users" }

orm := tinyorm.New(db, "default")
if err := orm.AutoMigrate(ctx, User{}); err != nil { /* handle */ }
if err := orm.Create(ctx, &User{ID: 1, Email: "ada@example.test"}); err != nil { /* handle */ }

var user User
if err := orm.First(ctx, &user, "email = :email", map[string]any{"email": "ada@example.test"}); err != nil { /* handle */ }
user.State = "disabled"
if err := orm.Save(ctx, &user); err != nil { /* handle */ }
```

Besides GORM, tinyorm understands the tag formats used by sqlx (`db`), Bun,
go-pg, and XORM. The compact `db` and `tinyorm` forms are supported as well:

```go
ID    int    `db:"id,pk"`
Email string `tinyorm:"email,unique,notnull,type=VARCHAR(120)"`
State string `db:"state,default=active"`
```

```go
// Bun and go-pg use the same comma-separated convention.
ID    int    `bun:"id,pk"`
Email string `pg:"email,unique,notnull"`

// XORM uses space-separated options and a quoted column name.
Name string `xorm:"varchar(120) notnull 'display_name'"`
```

Supported options are `pk`/`primaryKey`, `unique`, `notnull`/`not null`,
`type`, `default`, and `-`. The accepted GORM-style equivalents are
`primaryKey`, `unique`, `not null`, `type:…`, `default:…`, and `-`. XORM
also recognizes its common quoted column name, `pk`, `unique`, `notnull`,
`default`, and SQL type modifiers.

`AutoMigrate` is additive: it creates missing tables but never drops or
rewrites existing schema. `Create`/`Insert`, `First`, `FindByPK`, `Select`,
`Save`/`Update`, and `DeleteByPK` cover the basic model lifecycle.
