# tinySQL SQL Toolkit (`sqltools`)

Formatter, validator, linter, normalizer, differ, query explainer, template
library, and interactive REPL in one binary.

## Build

```bash
go build -o sqltools ./cmd/sqltools
```

## Usage

```
sqltools <subcommand> [options] [args...]
```

Every SQL-taking subcommand accepts the statement inline, as `@file.sql`, or as
a single `-` argument to read stdin.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `beautify [-upper=true] <sql>` | Format a statement (`-upper` uppercases keywords) |
| `validate <sql>` | Check syntax; exit 1 on invalid SQL |
| `explain <sql>` | Print the execution plan |
| `lint <sql>` | Multi-rule analysis; exit 1 if any issue is `error` severity |
| `normalize [-placeholders] <sql>` | Canonicalize SQL for comparison |
| `diff <fileA.sql> <fileB.sql>` | Compare two SQL files |
| `templates` | List built-in query templates with SQL and parameters |
| `repl [-tenant=default]` | Interactive shell against an in-memory tenant |

`validate` prints `✓ Valid SELECT statement` plus any `⚠ Warning` lines, or
`✗ Invalid SQL: <error>` and exits 1. `explain` prints the execution steps
tinySQL would use (table scan, join order, filter pushdown, etc.).

## repl

```bash
./sqltools repl
./sqltools repl -tenant analytics
```

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.quit` / `.exit` | Exit |
| `.tables` | List tables |
| `.schema [table]` | Show schema (all tables or one) |
| `.history [n]` | Show last n queries (default 10) |
| `.beautify <sql>` | Format SQL |
| `.validate <sql>` | Check syntax |
| `.explain <sql>` | Show query plan |
| `.export <csv\|json\|ndjson\|sql> <file> <sql>` | Export results to a file |
| `.template <name>` | Show one template |
| `.templates` | List all templates |

Anything else is executed as SQL. `.export ndjson out.ndjson SELECT ...` gives
a JSON Lines export.

## Examples

```bash
# Format, then validate the formatted output
./sqltools beautify "select*from t" | ./sqltools validate -

# Compare two revisions of a query
./sqltools diff old.sql new.sql

# Normalize with literals replaced by placeholders
./sqltools normalize -placeholders "SELECT * FROM t WHERE id = 42"
```
