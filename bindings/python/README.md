# tinySQL Python Binding

A minimal cgo bridge that exposes tinySQL to Python via `ctypes`.

## Build

```bash
# Shared library (libtinysql.so + libtinysql.h)
go build -buildmode=c-shared  -o libtinysql.so ./bindings/python

# Static archive (libtinysql.a + libtinysql.h)
go build -buildmode=c-archive -o libtinysql.a  ./bindings/python
```

Both commands emit `libtinysql.h` with the exported declarations:

```c
const char* TinySQLVersion(void);
const char* TinySQLExec(const char* sql);
const char* TinySQLSave(const char* path);
const char* TinySQLLoad(const char* path);
void        TinySQLReset(void);
void        TinySQLFree(char* ptr);
```

`TinySQLExec` takes a UTF-8 SQL string, runs it against an in-memory database
(tenant `default`), and returns a JSON payload. `TinySQLSave`/`TinySQLLoad`
persist and restore the database. `TinySQLReset` wipes the in-memory state so
one process can serve multiple tests.

`TinySQLFree` must be called on every pointer returned by the `TinySQL*`
functions (all except `TinySQLReset`) or you leak memory.

Payloads are UTF-8 RFC 8259 JSON. Future error objects can carry SQLSTATE
classification from the public tinySQL API; see the
[`standards`](../../standards/) package for the shared standards map.

## Python usage

[example.py](./example.py) contains the `ctypes` wrapper:

```python
from example import TinySQL

db = TinySQL()
print(db.version())
db.execute("CREATE TABLE users (id INT, name TEXT);")
db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');")
result = db.execute("SELECT * FROM users ORDER BY id;")
print(result["rows"])
db.save("mydata.db")
db.reset()  # clean slate
```

A result-producing statement returns:

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

Statements without a result set return `{"status": "ok", "rows": 0}`.

## Thread safety

The bridge serializes access through a mutex, so `TinySQLExec` is safe from
multiple Python threads. Long-running queries still block other callers; shard
across multiple shared objects if you need more parallelism.

## Common pitfalls

- **Missing lib**: put the `.so` on `LD_LIBRARY_PATH` (Linux) or next to your
  script. macOS may need `install_name_tool -id` adjustments.
- **Architecture mismatch**: build with the same architecture/ABI as your
  Python interpreter (e.g. `GOOS=darwin GOARCH=arm64` on Apple Silicon).
- **Unicode**: pass UTF-8; `ctypes` encodes when you call `.encode("utf-8")`.
