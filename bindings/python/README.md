# tinySQL Python Binding

This directory hosts a minimal cgo bridge that exposes tinySQL to Python. It is useful when you need SQLite-like embeddability but want to execute queries via the tinySQL engine.

## Build

First, compile the Go code as either a shared object (`.so`) or a static archive (`.a`) along with the generated C header:

```bash
# Build shared library (libtinysql.so + libtinysql.h)
go build -buildmode=c-shared  -o libtinysql.so ./bindings/python

# Build static archive (libtinysql.a + libtinysql.h)
go build -buildmode=c-archive -o libtinysql.a  ./bindings/python
```

Both commands emit a `libtinysql.h` header containing the exported function declarations:

```c
const char* TinySQLVersion(void);
const char* TinySQLExec(const char* sql);
const char* TinySQLSave(const char* path);
const char* TinySQLLoad(const char* path);
void        TinySQLReset(void);
void        TinySQLFree(char* ptr);
```

`TinySQLExec` accepts a UTF‑8 SQL string, executes it against an in-memory database (tenant `default`), and returns a JSON payload describing the outcome. `TinySQLSave` and `TinySQLLoad` allow persisting the database to disk. `TinySQLFree` must be called on every pointer returned by the `TinySQL*` functions (except `TinySQLReset`) to avoid a leak. `TinySQLReset` wipes the in-memory state so you can reuse the same process for multiple tests.

## Python Usage

A lightweight `ctypes` wrapper is provided in [example.py](./example.py). You can adapt it to your own application. The gist:

```python
from example import TinySQL

db = TinySQL()
print(db.version())
db.execute("CREATE TABLE users (id INT, name TEXT);")
db.execute("INSERT INTO users VALUES (1, 'Alice');")
result = db.execute("SELECT * FROM users;")
print(result["rows"])
db.save("mydata.db")
```

The returned JSON looks like this:

```json
{
  "status": "ok",
  "columns": ["id", "name"],
  "rows": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ]
}
```

Create tables, insert rows, and run queries just like the Go API:

```python
exec_sql("CREATE TABLE users (id INT, name TEXT);")
exec_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');")
print(exec_sql("SELECT * FROM users ORDER BY id;"))
```

Call `exec_sql("DROP TABLE users;")` or `lib.TinySQLReset()` when you want a clean slate.

## Thread Safety

The Go bridge serializes access through a mutex, so you can call `TinySQLExec` from multiple Python threads without corrupting the in-memory database. Long-running queries will still block other callers, so consider sharding across multiple shared objects if you need maximal parallelism.

## Common Pitfalls

- **Missing lib**: ensure the `.so` is on `LD_LIBRARY_PATH` (Linux) or next to your script. macOS may require `install_name_tool -id` adjustments.
- **Architecture mismatch**: build the Go library with the same architecture/ABI as your Python interpreter (e.g., `GOOS=darwin GOARCH=arm64` for Apple Silicon).
- **Unicode**: pass UTF‑8 strings; `ctypes` handles encoding when you call `.encode("utf-8")`.

Feel free to expand this binding (e.g., wrap it via `cffi` or PyO3) if you need richer ergonomics.
