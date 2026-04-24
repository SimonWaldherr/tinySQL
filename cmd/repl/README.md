# tinySQL Interactive REPL (`repl`)

An interactive SQL shell built on top of Go's `database/sql` interface and the
tinySQL driver. Supports multiple output formats, echo mode, optional HTML
output, and in-memory or file-backed databases.

## Build

```bash
go build -o repl ./cmd/repl
```

## Usage

```
repl [FLAGS]

Flags:
  -dsn string
        Storage DSN (default: in-memory)
        Examples:
          mem://?tenant=default
          file:/tmp/mydb.db?tenant=main&autosave=1
  -format string
        Output format: table | csv | tsv | json | yaml | markdown (default: "table")
  -echo
        Echo each SQL statement before executing it
  -beautiful
        Enable enhanced table borders (box-drawing characters)
  -html
        Emit results as HTML tables instead of text
  -errors-only
        Suppress successful results; only print errors
```

## Quick start

### In-memory database (default)

```bash
./repl
```

### File-backed database

```bash
./repl -dsn "file:/tmp/demo.db?tenant=main&autosave=1"
```

### JSON output format

```bash
./repl -format json
```

### Markdown output (great for copy-paste into docs)

```bash
./repl -format markdown
```

## Interactive commands

Inside the REPL, type any SQL statement ending with `;` to execute it.
Special dot-commands:

| Command | Description |
|---------|-------------|
| `.quit` / `.exit` | Exit the REPL |
| `.tables` | List all tables |
| `.schema <table>` | Show CREATE TABLE statement |
| `.help` | Show available commands |

## Output formats

| Format | Description |
|--------|-------------|
| `table` | ASCII-aligned columns (default) |
| `csv` | Comma-separated values |
| `tsv` | Tab-separated values |
| `json` | JSON array of objects |
| `yaml` | YAML sequence |
| `markdown` | GitHub-flavoured Markdown table |

## Example session

```
tinySQL repl — type SQL ending with ';' or a dot-command.

sql> CREATE TABLE users (id INT, name TEXT, active BOOL);
(ok)

sql> INSERT INTO users VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE);
(2 row(s) affected)

sql> SELECT * FROM users WHERE active = TRUE;
 id | name
----+------
  1 | Alice
(1 row(s))

sql> .tables
users

sql> .quit
Bye.
```
