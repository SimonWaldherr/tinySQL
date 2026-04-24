# tinySQL SQL Toolkit (`sqltools`)

A multi-purpose SQL utility that bundles a formatter, validator, query explainer,
template library, and interactive REPL into a single binary.

## Build

```bash
go build -o sqltools ./cmd/sqltools
```

## Usage

```
sqltools <subcommand> [options] [args...]
```

## Subcommands

### `beautify` — Format a SQL statement

```bash
./sqltools beautify "select id,name from users where id=1"
```

Output:

```sql
SELECT
  id,
  name
FROM
  users
WHERE
  id = 1
```

Options:

| Flag | Description | Default |
|------|-------------|---------|
| `-upper` | Convert keywords to uppercase | `true` |

### `validate` — Check SQL syntax

```bash
./sqltools validate "SELECT * FROM users WHERE"
# exit code 1 + error message on invalid SQL

./sqltools validate "SELECT id FROM users"
# exit code 0 + "OK"
```

### `explain` — Show a query execution plan

Parses the SQL and prints a human-readable description of the execution steps
tinySQL would use (table scan, join order, filter pushdown, etc.).

```bash
./sqltools explain "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name"
```

### `templates` — List built-in query templates

Prints a catalogue of common SQL patterns (CREATE TABLE, SELECT with JOIN, CTE,
window function, etc.) that can be used as starting points.

```bash
./sqltools templates
```

### `repl` — Interactive SQL tools shell

An enhanced REPL with schema browsing, query history, and access to all
subcommands as slash-commands.

```bash
./sqltools repl
./sqltools repl -tenant mydb
```

Options:

| Flag | Description | Default |
|------|-------------|---------|
| `-tenant` | Tenant name for the in-memory database | `default` |

Inside the REPL:

| Command | Description |
|---------|-------------|
| `/beautify <sql>` | Format a statement |
| `/validate <sql>` | Validate syntax |
| `/explain <sql>` | Show execution plan |
| `/templates` | List templates |
| `.tables` | List tables |
| `.schema <table>` | Show table schema |
| `.help` | Show help |
| `.quit` | Exit |

## Examples

```bash
# Format and validate in one pipeline
./sqltools beautify "select*from t" | ./sqltools validate

# Quick explain from a file
cat query.sql | xargs ./sqltools explain

# Start interactive session against an in-memory tenant
./sqltools repl -tenant analytics
```
