# tinySQL SQL Diagnostic Tool (`debug`)

A lightweight command-line tool for parsing and executing SQL statements against
an in-memory tinySQL database. It reports the statement type, execution result,
and per-statement timing — useful for quickly testing SQL compatibility and
debugging query behaviour without a full REPL.

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
  -verbose  Print statement type and extra diagnostics (default: false)
```

SQL can be provided in three ways, evaluated in this order:

1. `-sql` flag
2. Positional arguments (joined with a space)
3. Standard input (when stdin is not a TTY)

Multiple semicolon-separated statements are accepted in all three modes.

## Examples

### Via `-sql` flag

```bash
./debug -sql "SELECT 1 + 1 AS result"
```

### Positional argument

```bash
./debug "SELECT UPPER('hello') AS greeting"
```

### Multiple statements

```bash
./debug "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (1, 'Alice'); SELECT * FROM t"
```

### Pipe from stdin

```bash
echo "SELECT 42 AS answer" | ./debug
```

### With verbose output

```bash
./debug -verbose -sql "SELECT COUNT(*) FROM (VALUES (1),(2),(3)) AS x(n)"
```

### Suppress timing

```bash
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

[4] SQL> SELECT * FROM t
    id  name
    --  -----
    1   Alice
    (1 row(s))
    elapsed: 6µs
```

## Notes

- Each run starts with a **fresh in-memory database**; data does not persist
  between invocations.
- The tool is intended as a development aid, not an end-user application. For
  a persistent interactive shell see [`tinysql`](../tinysql/) or
  [`repl`](../repl/).
