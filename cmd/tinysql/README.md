# tinySQL CLI (`tinysql`)

A SQLite-compatible command-line interface for tinySQL. Accepts a database file
path (or `:memory:`) and runs in interactive REPL mode, executes inline SQL, or
dispatches named subcommands for common operations.

## Build

```bash
go build -o tinysql ./cmd/tinysql
```

## Usage

```
tinysql [FLAGS] <database> [SQL]
tinysql [FLAGS] <database> <subcommand> [subcommand-flags]
```

`<database>` is either a file path or `:memory:` for an in-memory database.

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-tenant` | Tenant namespace | `default` |
| `-mode` | Output mode: `column`, `list`, `csv`, `json`, `table` | `column` |
| `-header` | Print column headers | `true` |
| `-echo` | Echo each SQL statement before executing | `false` |
| `-cmd` | Execute this SQL then exit | — |
| `-batch` | Batch mode: suppress prompts, exit on first error | `false` |
| `-output` | Write results to this file instead of stdout | — |

## Interactive REPL

```bash
# In-memory database
./tinysql :memory:

# File-backed database (created if it doesn't exist)
./tinysql mydb.dat
```

Special commands inside the REPL:

| Command | Description |
|---------|-------------|
| `.tables` | List all tables |
| `.schema [table]` | Show CREATE TABLE for one or all tables |
| `.mode <mode>` | Change output mode |
| `.headers on\|off` | Toggle column headers |
| `.output <file>` | Redirect output to a file |
| `.quit` / `.exit` | Exit |
| `.help` | Show available commands |

## Inline SQL

```bash
./tinysql mydb.dat "SELECT * FROM users LIMIT 5"
```

## Subcommands

### `tables` — List tables

```bash
./tinysql mydb.dat tables
```

### `schema` — Show table schema

```bash
./tinysql mydb.dat schema users
```

### `insert` — Bulk insert from a file

```bash
./tinysql mydb.dat insert -file data.csv -table users
```

### `query` — Execute a query and write to a file

```bash
./tinysql mydb.dat query -sql "SELECT * FROM users" -output results.csv -format csv
```

### `export` — Export a table

```bash
./tinysql mydb.dat export -table users -output users.json -format json
```

## Examples

```bash
# One-shot query with JSON output
./tinysql :memory: -mode json "SELECT 1 AS n, 'hello' AS greeting"

# Batch mode for scripting (exit on error)
./tinysql mydb.dat -batch -cmd "INSERT INTO log VALUES (NOW(), 'ping')"

# Pipe a SQL script
cat setup.sql | ./tinysql mydb.dat -batch

# Redirect results to a file
./tinysql mydb.dat -output report.txt "SELECT * FROM sales ORDER BY amount DESC"
```
