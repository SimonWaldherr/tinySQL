# tinySQL SQL Diagnostic Tool (`debug`)

Part of [TinySQL](../../README.md). See the root guide for supported SQL,
functions, and known limits.

Parses and executes SQL against a fresh in-memory tinySQL database, reporting
statement type, result, and per-statement timing.

## Build

```bash
go build -o debug ./cmd/debug
```

## Usage

```
debug [FLAGS] [SQL...]

Flags:
  -sql      SQL statement(s) to execute (semicolon-separated)
  -timing   Print execution time for each statement (default: true)
  -verbose  Print statement type (default: false)
```

SQL is taken from the first available source: `-sql`, then positional
arguments (joined with a space), then stdin (when stdin is not a TTY).
Semicolon-separated statements are accepted in all three modes.

## Examples

```bash
./debug -sql "SELECT 1 + 1 AS result"
./debug "SELECT UPPER('hello') AS greeting"
./debug "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (1, 'Alice'); SELECT * FROM t"
echo "SELECT 42 AS answer" | ./debug
./debug -verbose -sql "SELECT COUNT(*) FROM (VALUES (1),(2),(3)) AS x(n)"
./debug -timing=false -sql "SELECT NOW()"
```

## Sample output

```
[1] SQL> SELECT 1 + 1 AS result
    result
    ------
    2
    (1 row(s))
    elapsed: 48µs

[2] SQL> CREATE TABLE t (id INT, name TEXT)
    (ok)
    elapsed: 12µs

[3] SQL> INSERT INTO t VALUES (1, 'Alice')
    (0 rows)
    elapsed: 9µs
```

## Notes

- Each run starts with a fresh in-memory database; data does not persist
  between invocations.
- A development aid, not an end-user application. For a persistent
  interactive shell see [`tinysql`](../tinysql/) or [`repl`](../repl/).
