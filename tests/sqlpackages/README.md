# database/sql package integration tests

The tests run sqlx v1.4.0 and Squirrel v1.5.4 against actual, isolated tinySQL
in-memory databases. No mock driver or external database server is involved.
Dependencies are confined to this nested test module.

```sh
# Repository root; includes the sibling GORM module:
make test-sql-compat
make test-sql-compat GO_TEST_FLAGS='-race -count=1'

# Only sqlx and Squirrel:
make test-sqlpackages
```

Both suites are included in `make test-integration` and the normal/race parts of
`make test-ci`. Root `go test ./...` does not traverse these nested modules.
See the [compatibility matrix](../../docs/database-sql-compatibility.md) for
versions, tested operations, configuration and limitations.
