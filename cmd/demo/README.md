# tinySQL SQL Playground (`demo`)

Part of [tinySQL](../../README.md). Use the root guide for the current feature,
GIS, storage, and import overview.

Seeds sample data into an in-memory or file-backed tinySQL database and lets you
explore it via a guided feature tour, a SQL script, or an interactive REPL.

## Build

Run from the repository root.

```bash
go build -o bin/demo ./cmd/demo
```

## Usage

```
demo [OPTIONS]

Options:
  -dsn string       Storage DSN (default: mem://?tenant=default)
                      file:/tmp/mydb.db?tenant=main&autosave=1
  -seed             Populate sample tables (default: true)
  -script FILE      Execute SQL statements from FILE; use - for stdin
  -interactive      Start interactive SQL shell after setup
  -timer            Print execution time for every statement
  -quiet            Suppress DDL/DML confirmation output; only SELECT results
  -output string    Output format: table (default), csv, json, ndjson
```

Without `-script` or `-interactive`, the default run is a feature tour covering
joins and APPLY, PIVOT and window functions, JSON, date and regex functions,
triggers, full-text and vector search, CTEs, geospatial distance and bearing,
temp tables, and UPDATE/DELETE.

## Examples

```bash
bin/demo                                   # feature tour on freshly seeded data
bin/demo -script my_queries.sql            # run a script against seeded tables
bin/demo -interactive                      # seed, then open a REPL
bin/demo -timer -script heavy_queries.sql  # time every statement
printf '%s\n' 'SELECT id, name FROM users ORDER BY id;' |
  bin/demo -script - -output ndjson -quiet | jq -c .

# Persist to a file, then reuse it without reseeding
bin/demo -dsn "file:/tmp/mydb.db?tenant=main" -interactive
bin/demo -dsn "file:/tmp/mydb.db?tenant=main" -seed=false -script report.sql
```

Scripts accept multiple statements on a line, multiline triggers, and a final
statement without a semicolon. In script and tour modes, CSV, JSON, and NDJSON
write results to stdout and SQL echoes, confirmations, and timing to stderr.
For a single JSON array or CSV header, run a script with one result-producing
statement. NDJSON emits one object per row across all results.

REPL session:

```
tinySQL playground — type SQL ending with ';' to execute, '.quit' to exit.
  .tables   list tables      .help   show this message

sql> SELECT name, email FROM users WHERE active = TRUE;
name   email
-----  -----------------
Alice  alice@example.com
Bob    NULL
(2 row(s))

sql> .quit
Bye.
```

## Sample data

Seeding creates `users`, `orders`, `order_audit`, `sales`, `articles`, and
`docs`. The users and orders tables drive ordinary query examples;
`order_audit` records inserted orders through an AFTER INSERT trigger, and the
remaining tables drive PIVOT/window, full-text, and vector-search steps.
