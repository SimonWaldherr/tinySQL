# tinySQL Interactive REPL (`repl`)

Part of [tinySQL](../../README.md). See the root guide for supported SQL,
storage modes, and limitations.

An interactive SQL shell over `database/sql` and the tinySQL driver.

## Build

```bash
go build -o repl ./cmd/repl
```

## Usage

```
repl [FLAGS]

Flags:
  -dsn string
        Storage DSN (default "mem://?tenant=default")
        e.g. file:/tmp/mydb.db?tenant=main&autosave=1
  -format string
        Output format: table | csv | tsv | json | yaml | markdown (default "table")
  -echo
        Echo each SQL statement before executing it
  -beautiful
        Pretty-print SQL blocks and results (groups statements until the next SELECT)
  -html
        Emit a single HTML page of SQL blocks and results (for redirected input)
  -errors-only
        Only print queries/results that produce errors
```

```bash
./repl
./repl -dsn "file:/tmp/demo.db?tenant=main&autosave=1"
./repl -format json
./repl -format markdown
```

`markdown` emits GitHub-flavoured tables; `json` an array of objects; `yaml` a
sequence. `md` is accepted as an alias for `markdown`.

## Interactive commands

Type SQL ending with `;` to execute. Dot-commands:

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.quit` / `.exit` | Exit |
| `.tables` | List all tables |
| `.schema [TABLE]` | Show CREATE TABLE (all tables, or one) |
| `.count [TABLE...]` | Show row counts |
| `.dump [TABLE...]` | Dump table(s) as INSERT statements |
| `.read FILE` | Execute SQL from a file |
| `.clear` | Clear the screen |

`.help` also advertises `.output FORMAT` and `.timer on|off`, but neither is
implemented — they fall through and are treated as SQL. Set the format with
`-format` instead.

## Example session

```
sql> CREATE TABLE users (id INT, name TEXT, active BOOL);
(ok)

sql> INSERT INTO users VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE);
(2 row(s) affected)

sql> SELECT * FROM users WHERE active = TRUE;
 id | name
----+------
  1 | Alice
(1 row(s))

sql> .quit
```
