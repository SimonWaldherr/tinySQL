# fsql — Filesystem Query Language

Query your filesystem with SQL. `fsql` treats directories as relational tables,
exposing file metadata, file contents (line by line), CSV rows, and JSON rows
through table-valued functions backed by the tinySQL engine.

> **Note:** `fsql` lives in its own Go module (`cmd/fsql/go.mod`) and must be
> built from its directory.

## Build

```bash
cd cmd/fsql
go build -o fsql .
```

## Usage

```
fsql [FLAGS] <command> [args...]

Flags:
  --mount <path>    Ad-hoc root path for the query (overrides named mounts)
  --output <fmt>    Output format: table | csv | json  (default: table)
  --scope <name>    Named scope to use as the default mount root
  --version         Print version and exit
```

## Named mounts

Mounts are named filesystem roots stored in
`~/.config/fsql/mounts.json`. They let you refer to directories by a
short alias in queries.

```bash
# Register a named mount
fsql mount logs /var/log

# List registered mounts
fsql mounts

# Remove a mount
fsql umount logs
```

## Querying

```bash
# Ad-hoc query — no mount needed
fsql --mount /var/log "SELECT path, size FROM files('root', true) WHERE ext = 'log'"

# Use a named scope as the root
fsql --scope logs "SELECT name, size FROM files('logs') ORDER BY size DESC LIMIT 10"

# Named query subcommand
fsql query --scope logs "SELECT * FROM files('logs', true) WHERE size > 1048576"
```

## Table-valued functions

| Function | Columns | Description |
|----------|---------|-------------|
| `files(path [, recursive])` | `path`, `name`, `ext`, `size`, `mod_time`, `is_dir` | Filesystem metadata |
| `lines(file)` | `line_number`, `line` | Lines of a text file |
| `csv_rows(file [, header])` | One column per CSV field | Rows from a CSV file |
| `json_rows(file [, path])` | One column per JSON key | Objects from a JSON file |

## Query examples

```sql
-- Find the 10 largest log files
SELECT path, size
FROM files('/var/log', true)
WHERE ext = 'log'
ORDER BY size DESC
LIMIT 10;

-- Search for a pattern across all .go source files
SELECT path, line_number, line
FROM files('/home/user/project', true) AS f,
     lines(f.path) AS l
WHERE f.ext = 'go'
  AND l.line LIKE '%TODO%';

-- Aggregate CSV data
SELECT city, COUNT(*) AS residents
FROM csv_rows('/data/people.csv', true) AS p
GROUP BY city
ORDER BY residents DESC;

-- Explore a JSON data file
SELECT name, age
FROM json_rows('/data/users.json')
WHERE age > 30;
```

## Output formats

```bash
# Default: aligned table
fsql --mount /tmp "SELECT name, size FROM files('root')"

# JSON
fsql --output json --mount /tmp "SELECT name, size FROM files('root')"

# CSV (pipe-friendly)
fsql --output csv --mount /tmp "SELECT name, size FROM files('root')" > files.csv
```

## Configuration

Mounts are persisted in `~/.config/fsql/mounts.json`:

```json
{
  "logs":    "/var/log",
  "project": "/home/user/myproject"
}
```

## Architecture

`fsql` is a standalone module that registers four table-valued functions
(`files`, `lines`, `csv_rows`, `json_rows`) via
`tinysql.RegisterExternalTableFunc` and uses the tinySQL engine for all SQL
evaluation. See [`internal/adapter/`](internal/adapter/) for the TVF
implementations and [`internal/scope/`](internal/scope/) for mount management.
