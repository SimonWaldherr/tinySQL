# GORM integration tests

These tests use GORM v1.31.1 and its PostgreSQL dialector v1.6.0 with a tinySQL
`*sql.DB`. No PostgreSQL server or SQLite engine executes the queries. The
separate module keeps ORM dependencies out of tinySQL's root module.

From the repository root:

```sh
make test-gorm
make test-gorm GO_TEST_FLAGS='-race -count=1'
```

`make test-integration` and `make test-ci` include this module. Root-level
`go test ./...` does not traverse nested Go modules.

The tested subset covers:

- `Migrator().CreateTable` and `AddColumn`, including reading/writing the new column;
- create/read/update/delete, affected rows and missing-record errors;
- explicit integer primary keys, text escaping, booleans, zero values and BLOBs;
- generated `$n` placeholders, `IN` expansion and quoted qualified identifiers;
- explicit transaction commit and rollback, plus GORM's default write transactions;
- execution with `PrepareStmt` both disabled and enabled.

Models use `gorm:"primaryKey;autoIncrement:false"` and supply their IDs. BLOB
fields explicitly use `gorm:"type:blob"`. Full PostgreSQL catalog introspection,
`AutoMigrate`, automatic IDs, associations, nested transactions/savepoints and
all PostgreSQL types are outside this tested compatibility subset. Use explicit
migrations; these tests do not claim that tinySQL is a PostgreSQL replacement.

Connection pattern:

```go
pool := testutil.Open(t)
db, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{})
if err != nil {
    t.Fatal(err)
}
```

References: [GORM existing-connection integration](https://gorm.io/docs/connecting_to_the_database.html),
[GORM migration API](https://gorm.io/docs/migration.html).
