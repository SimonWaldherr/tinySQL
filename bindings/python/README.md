# tinySQL for Python

A DB-API 2.0 (PEP 249) package for the embedded tinySQL engine. It loads the
shared library built from the repository's [C ABI](../c/include/tinysql.h)
with `ctypes`; no compiler is needed at import time and there are no Python
dependencies.

## Build and install

Requirements: Python 3.8+, Go (version from the repository `go.mod`) and a C
toolchain for cgo.

```sh
# Build tinysql/libtinysql.so (.dylib on macOS) and run the tests:
make -C bindings/python test

# Or install into the active environment; this builds the library with Go:
pip install ./bindings/python
```

The library is searched in this order: the `library=` argument of `connect`,
the `TINYSQL_LIBRARY` environment variable, the package directory, then the
system library path. On Windows build `tinysql.dll` with
`go build -buildmode=c-shared -o bindings/python/tinysql/tinysql.dll ./bindings/c`.

## Quick start

```python
import tinysql

with tinysql.connect() as conn:                      # in-memory database
    conn.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, avatar BLOB)")
    conn.executemany(
        "INSERT INTO users VALUES (?, ?, ?)",
        [(1, "Ada", None), (2, "Grace", b"\x89PNG")],
    )
    row = conn.execute("SELECT id, name FROM users WHERE name = ?", ("Grace",)).fetchone()
    print(row, row["name"])                          # (2, 'Grace') Grace
```

[examples/quickstart.py](examples/quickstart.py) shows a durable database,
batches, transactions and `dict_row`.

## Opening databases

| Call | Result |
| --- | --- |
| `connect()` / `connect(":memory:")` | New in-memory database |
| `connect("app.tinysql")` | Load a snapshot; changes stay in memory until `conn.save(path)` |
| `connect("app.tinysql", read_only=True)` | Load a snapshot and reject writes |
| `connect("./data", mode="wal")` | Durable in-memory tables with a write-ahead log |
| `connect("./data", mode="disk")` / `"json"` | Durable per-table files (GOB or readable JSON) |
| `connect("./data", mode="hybrid", max_memory_bytes=256 << 20)` | Disk tables with a bounded cache |

Durable modes persist every acknowledged write. Tuning keywords:
`max_memory_bytes`, `sync_on_mutate`, `compress_files`, `checkpoint_every`,
`checkpoint_interval_ms`, `checkpoint_max_bytes` and `wal_sync`
(`"full"` or `"normal"`). `encryption_key=<32 bytes>` encrypts table files of
the `disk`, `json`, `index` and `hybrid` modes. See the
[storage guide](../../docs/storage-guide.md) for the trade-offs.

## Statements and results

- `conn.execute(sql, params)` and `cursor.execute` run one statement.
  Row-producing statements (SELECT, WITH, PRAGMA, `... RETURNING`) fill the
  cursor; others set `cursor.rowcount`.
- `executemany(sql, rows)` prepares the statement once in the engine and
  sends all parameter rows in a single native call by default. Pass
  `batch_size=1000` to consume an iterable in bounded native batches.
- `executescript(script)` runs several statements; semicolons inside
  strings, comments and `CREATE TRIGGER ... BEGIN ... END` bodies are handled.
- Placeholders: `?` (the declared `paramstyle`), `$1` or `:1`. Named
  parameters are not supported.
- Rows are tuples that also accept case-insensitive column names
  (`row["name"]`). Set `conn.row_factory = tinysql.dict_row` for dictionaries.
- Results are materialized. Page large results with `LIMIT`/`OFFSET` or a
  keyset condition.

| Python | SQL value | Returned as |
| --- | --- | --- |
| `None` | NULL | `None` |
| `bool` | BOOL | `bool` |
| `int` (signed 64-bit) | INT | `int` |
| `float` (finite) | FLOAT | `float` |
| `str` | TEXT | `str` |
| `bytes`, `bytearray`, `memoryview` | BLOB | `bytes` |
| `datetime`, `date`, `time` | ISO 8601 text (aware datetimes in UTC, `Z`) | `str` |
| `decimal.Decimal`, `uuid.UUID` | text | `str` |

## Transactions

Connections autocommit by default; `commit()` and `rollback()` are no-ops
without a transaction, so code written for `sqlite3` keeps working.

```python
with conn.transaction():          # BEGIN ... COMMIT, ROLLBACK on exception
    conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (10, 1))
    conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (10, 2))
```

`connect(autocommit=False)` gives PEP 249 behavior: a transaction starts
before the first INSERT/UPDATE/DELETE and lasts until `commit()`. `BEGIN`,
`COMMIT` and `ROLLBACK` issued as SQL work too; `conn.in_transaction` always
reflects the engine's state. Leaving a `with tinysql.connect(...)` block
commits (or, after an exception, rolls back) and closes the connection.

Transaction scopes attempt rollback after body errors, interrupted execution
and commit failures, preserving the original exception. Do not issue transaction
control inside the scope. SQL text and snapshot paths containing NUL are rejected
before native calls; bind text values containing NUL as parameters. Invalid script
text is rejected before an implicit transaction can be committed.

## Errors and threads

Errors derive from `tinysql.Error` following PEP 249: syntax errors and
unknown tables raise `ProgrammingError`, constraint violations
`IntegrityError`, I/O, conflicts and read-only violations `OperationalError`.

Each connection is an independent database whose native calls are
serialized. The GIL is released during native calls, so separate connections
run queries in parallel. Share the module between threads, not a connection
(`threadsafety = 1`).

## Legacy API

The single-database functions of the former bindings (`TinySQLExec`,
`TinySQLSave`, `TinySQLLoad`, `TinySQLReset`, `TinySQLVersion`,
`TinySQLFree`) are still exported by the library for existing `ctypes`
callers. New code should use this package.

## Large imports

```python
conn.execute("CREATE TABLE events (id INT PRIMARY KEY, body TEXT)")
with conn.transaction():
    conn.executemany("INSERT INTO events VALUES (?, ?)",
                     ((i, "event") for i in range(100_000)), batch_size=1000)
```

The optional positive integer `batch_size` bounds the encoded rows per call.
Omitting it retains whole-input validation and the existing single-call behavior.
Chunked imports validate incrementally: a failing generator, invalid parameter or
SQL error can leave earlier chunks applied outside a transaction. Use the scope
above for all-or-nothing ingestion. Error row numbers refer to the failing chunk.
`rowcount` is the combined affected count on success and stays unknown (-1) on
failure. No transaction is opened for empty input.

Each chunk prepares the statement again; tune the size for row width, memory
and call overhead. This bounds parameter assembly, not query result memory:
results remain materialized, including when fetched using `fetchmany`.
See [bulk integration measurements](../../docs/language-bindings.md#large-data-imports).
