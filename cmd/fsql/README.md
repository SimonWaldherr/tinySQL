# fsql — Filesystem Query Language

Query the filesystem with SQL. `fsql` exposes file metadata, text file lines,
CSV rows, and JSON rows as table-valued functions backed by the tinySQL engine.
It lives in its own Go module (`cmd/fsql/go.mod`) and must be built from its
directory.

## Build

```bash
cd cmd/fsql
go build -o fsql .
```

## Usage

```
fsql [FLAGS] mount <name> <path>      Register a named filesystem mount
fsql [FLAGS] umount <name>            Remove a named mount (alias: unmount)
fsql [FLAGS] mounts                   List registered mounts
fsql [FLAGS] query <sql>              Execute a SQL query
fsql [FLAGS] index build <scope>      Build an index for a scope (stub)
fsql [FLAGS] <sql>                    Shorthand query execution

Flags:
  --mount <path>    Ad-hoc root path for the query (overrides named mounts)
  --output <fmt>    Output format: table | csv | json  (default: table)
  --scope <name>    Named scope to use as the default mount root
  --version         Print version and exit
```

Flags are parsed before the subcommand, so they must come first. `index build`
prints a not-implemented notice.

## Named mounts

```bash
fsql mount logs /var/log
fsql mounts
fsql umount logs
```

Mounts are persisted as a JSON array in `fsql/mounts.json` under
`os.UserConfigDir()` (`~/.config/fsql/mounts.json` on Linux,
`%AppData%\fsql\mounts.json` on Windows), falling back to
`~/.fsql/mounts.json`:

```json
[
  {"name": "logs", "path": "/var/log"},
  {"name": "project", "path": "/home/user/myproject"}
]
```

## Querying

With `--mount`, the ad-hoc root is addressed as `'/'` (also `'.'` or `''`).
With `--scope`, the root is addressable as `'/'` or by the scope name.

```bash
# Ad-hoc query — no mount needed
fsql --mount /var/log "SELECT path, size FROM files('/', true) WHERE ext = 'log'"

# Use a named scope as the root
fsql --scope logs "SELECT name, size FROM files('logs') ORDER BY size DESC LIMIT 10"

# Output formats
fsql --output json --mount /tmp "SELECT name, size FROM files('/')"
fsql --output csv --mount /tmp "SELECT name, size FROM files('/')" > files.csv
```

## Table-valued functions

| Function | Columns |
|----------|---------|
| `files(path [, recursive])` | `path`, `name`, `size`, `ext`, `mod_time`, `is_dir` |
| `lines(file)` | `line_number`, `line` |
| `csv_rows(file [, header])` | one column per CSV field |
| `json_rows(file [, path])` | one column per JSON key |

`ext` has no leading dot. `mod_time` is RFC 3339. Without `recursive`, `files()`
does not descend into subdirectories. Unreadable entries are skipped.

```sql
-- Search for a pattern across all .go source files
SELECT path, line_number, line
FROM files('/home/user/project', true) AS f,
     lines(f.path) AS l
WHERE f.ext = 'go' AND l.line LIKE '%TODO%';

-- Aggregate CSV data
SELECT city, COUNT(*) AS residents
FROM csv_rows('/data/people.csv', true) AS p
GROUP BY city ORDER BY residents DESC;
```

## Architecture

A standalone module that registers the four table-valued functions via
`tinysql.RegisterExternalTableFunc` and delegates all SQL evaluation to the
tinySQL engine. TVF implementations live in
[`internal/adapter/`](internal/adapter/), mount management in
[`internal/scope/`](internal/scope/).
