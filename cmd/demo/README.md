# tinySQL SQL Playground (`demo`)

A practical SQL playground that seeds sample data into an in-memory (or file-backed) tinySQL database and lets you explore it interactively or via a SQL script.

## Features

- **Seed mode** – Creates `users` and `orders` tables with realistic sample data.
- **Feature tour** – Runs a guided tour of tinySQL capabilities (JOINs, GROUP BY, JSON extraction, CTEs, temp tables, UPDATE/DELETE).
- **Script mode** – Executes any SQL script file (`.sql`) statement by statement.
- **Interactive REPL** – Drops into a live SQL shell after setup.
- **Timer** – Optionally prints per-statement execution time.
- **Persistent storage** – Point at a file-based DSN to persist data across runs.

## Build

```bash
go build -o demo .
```

## Usage

```
demo [OPTIONS]

Options:
  -dsn string       Storage DSN (default: in-memory)
                      mem://?tenant=default
                      file:/tmp/mydb.db?tenant=main&autosave=1
  -seed             Populate sample tables (default: true)
  -script FILE      Execute SQL statements from FILE
  -interactive      Start interactive SQL shell after setup
  -timer            Print execution time for every statement
  -quiet            Suppress DDL/DML confirmation output
```

## Examples

### Built-in feature tour (default)

```bash
./demo
```

Runs the full feature tour against freshly seeded sample data.

### Execute a SQL script

```bash
./demo -script my_queries.sql
```

Executes every statement in `my_queries.sql` against the seeded `users`/`orders` tables.

### Interactive playground

```bash
./demo -interactive
```

Seeds sample data and opens a REPL where you can enter any SQL:

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

### Persistent database

```bash
./demo -dsn "file:/tmp/mydb.db?tenant=main" -interactive
```

The database is saved to `/tmp/mydb.db`; run again without `-seed` to keep existing data:

```bash
./demo -dsn "file:/tmp/mydb.db?tenant=main" -seed=false -interactive
```

### Skip seeding, run a script on an existing database

```bash
./demo -dsn "file:/tmp/mydb.db?tenant=main" -seed=false -script report.sql
```

### Time every statement

```bash
./demo -timer -script heavy_queries.sql
```

## Sample data

After seeding, two tables are available:

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
