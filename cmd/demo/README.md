# tinySQL SQL Playground (`demo`)

Seeds sample data into an in-memory or file-backed tinySQL database and lets you
explore it via a guided feature tour, a SQL script, or an interactive REPL.

## Build

```bash
go build -o demo .
```

## Usage

```
demo [OPTIONS]

Options:
  -dsn string       Storage DSN (default: mem://?tenant=default)
                      file:/tmp/mydb.db?tenant=main&autosave=1
  -seed             Populate sample tables (default: true)
  -script FILE      Execute SQL statements from FILE
  -interactive      Start interactive SQL shell after setup
  -timer            Print execution time for every statement
  -quiet            Suppress DDL/DML confirmation output; only SELECT results
  -output string    Output format: table (default), csv, json, ndjson
```

Without `-script` or `-interactive`, the default run is a feature tour covering
joins, PIVOT/window functions, JSON, triggers, full-text search, vector search,
CTEs, temp tables, and UPDATE/DELETE.

## Examples

```bash
./demo                                   # feature tour on freshly seeded data
./demo -script my_queries.sql            # run a script against seeded tables
./demo -interactive                      # seed, then open a REPL
./demo -timer -script heavy_queries.sql  # time every statement
./demo -output ndjson -script report.sql | jq -c .

# Persist to a file, then reuse it without reseeding
./demo -dsn "file:/tmp/mydb.db?tenant=main" -interactive
./demo -dsn "file:/tmp/mydb.db?tenant=main" -seed=false -script report.sql
```

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
`docs`.

**`users`** (id INT, name TEXT, email TEXT, active BOOL)

| id | name  | email             | active |
|----|-------|-------------------|--------|
| 1  | Alice | alice@example.com | true   |
| 2  | Bob   | NULL              | true   |
| 3  | Carol | carol@example.com | NULL   |

**`orders`** (id INT, user_id INT, amount FLOAT, status TEXT, meta JSON)

| id  | user_id | amount | status   |
|-----|---------|--------|----------|
| 101 | 1       | 100.5  | PAID     |
| 102 | 1       | 75.0   | PAID     |
| 103 | 2       | 200.0  | PAID     |
| 104 | 2       | 20.0   | CANCELED |

`order_audit` records inserted order IDs through an `AFTER INSERT` trigger;
`sales`, `articles`, and `docs` power the PIVOT/window, full-text, and vector
search steps of the feature tour.
