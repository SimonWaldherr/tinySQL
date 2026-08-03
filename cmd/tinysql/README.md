# tinySQL CLI (`tinysql`)

Part of [TinySQL](../../README.md). See the root guide for SQL, GIS, storage,
and import capabilities.

SQLite-style CLI for tinySQL: REPL, inline SQL, piped scripts,
`sqlite-utils`-style subcommands.

## Build

```bash
go build -o tinysql ./cmd/tinysql
```

## Usage

```
tinysql [FLAGS] [<database>] [SQL...]
tinysql <subcommand> [subcommand-flags] <database> [args...]
```

`<database>` is a file path or `:memory:` (default). A file-backed database is
created if missing and saved on exit and after each dirty statement.
Subcommands are matched on the first argument, so they must precede any flags.

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-tenant` | Tenant/schema name | `default` |
| `-mode` | Output mode: `column`, `list`, `csv`, `json`, `table` | `column` |
| `-header` | Include column headers | `true` |
| `-echo` | Echo SQL before execution | `false` |
| `-cmd` | Run this SQL and exit | — |
| `-batch` | Force batch mode (error if no SQL) | `false` |
| `-output` | Write output to this file, not stdout | — |

SQL source precedence: `-cmd`, then trailing positional SQL, then stdin when
piped. With none of those and no `-batch`, the REPL starts.

## REPL

Prompt is `tinysql> ` (`   ...> ` while a statement is incomplete). Ctrl+C
discards a partial statement; on an empty buffer it exits.

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.exit` / `.quit` | Exit |
| `.tables` | List table names |
| `.schema ?TABLE?` | Show CREATE statements |
| `.mode MODE` | Set output mode (no arg: print current) |
| `.headers on\|off` | Toggle header display |
| `.timer on\|off` | Toggle SQL timer (no arg: print state) |
| `.nullvalue STRING` | Print STRING for NULL values |
| `.read FILENAME` | Execute SQL in FILENAME |
| `.save FILENAME` | Write in-memory database to FILENAME |
| `.dump [TABLE...]` | Dump tables as INSERT statements |
| `.import FILE [TABLE]` | Import a CSV/JSON file into a table |
| `.count [TABLE...]` | Show row counts |
| `.stats` | Database statistics |

## Subcommands

Subcommand flags must precede the database argument.

```bash
./tinysql tables mydb.dat  # -tenant NAME, -json
./tinysql schema mydb.dat  # CREATE statements; -tenant NAME
./tinysql query -mode csv mydb.dat "SELECT * FROM users"  # -tenant, -mode (default table)
./tinysql insert mydb.dat users '{"id":1,"name":"Alice"}' # JSON rows; -tenant NAME
```

## Examples

```bash
cat setup.sql | ./tinysql mydb.dat
./tinysql mydb.dat -cmd "INSERT INTO log VALUES (NOW(), 'ping')"
./tinysql mydb.dat -output report.txt -mode json "SELECT * FROM sales"
```
