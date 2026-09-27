# tinySQL C ABI

`bindings/c` builds tinySQL as a static or shared C library. The Swift, Python
and Rust bindings wrap exactly this ABI; C, C++, Zig, Nim, .NET P/Invoke,
Java FFM, Node-API and other hosts can use it directly. The contract lives in
[`include/tinysql.h`](include/tinysql.h).

## Build

Requirements: Go (version from the repository `go.mod`) and a C toolchain for
cgo.

```sh
make build-c                 # bindings/c/build/{libtinysql.a, libtinysql.so|.dylib, tinysql.h}

# or directly:
go build -buildmode=c-archive -o libtinysql.a  ./bindings/c
go build -buildmode=c-shared  -o libtinysql.so ./bindings/c
```

Use `include/tinysql.h`, not the header cgo writes next to the library. Link
the static archive with the system libraries the Go runtime needs:

| Platform | Link flags |
| --- | --- |
| Linux | `-lpthread -lm -ldl` |
| macOS / iOS | `-framework CoreFoundation -framework Security -lresolv` |
| Windows (MinGW) | `-lws2_32 -luserenv -lbcrypt -lntdll -lwinmm` |

A process can contain only one Go runtime built with `-buildmode=c-archive`
or `c-shared`; link one tinySQL library per process.

## Example

```c
#include <stdio.h>
#include "tinysql.h"

int main(void) {
    char *opened = TinySQLDatabaseOpenWithOptions("{\"mode\":\"wal\",\"path\":\"./data\"}");
    unsigned long long db = 0;
    if (sscanf(opened, "{\"handle\":%llu}", &db) != 1) {   /* use a JSON parser in real code */
        fprintf(stderr, "%s\n", opened);
        TinySQLDatabaseFree(opened);
        return 1;
    }
    TinySQLDatabaseFree(opened);

    char *r = TinySQLDatabaseExecuteScript(db,
        "CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body TEXT)");
    TinySQLDatabaseFree(r);
    r = TinySQLDatabaseExecuteBatch(db, "INSERT INTO notes VALUES (?, ?)",
        "[[1, \"first\"], [2, {\"blob\": \"AAH/\"}]]");
    TinySQLDatabaseFree(r);
    r = TinySQLDatabaseRun(db, "SELECT id, body FROM notes WHERE id >= ?", "[1]");
    puts(r);   /* {"columns":["id","body"],"rows":[[1,"first"],[2,{"blob":"AAH/"}]]} */
    TinySQLDatabaseFree(r);

    TinySQLDatabaseFree(TinySQLDatabaseClose(db));
    return 0;
}
```

[`example/abi_smoke.c`](example/abi_smoke.c) is compiled and run by
`make test-c`.

## Functions

| Function | Returns |
| --- | --- |
| `TinySQLABIVersion()` | `2` (no allocation) |
| `TinySQLInfo()` | `{"version","abi"}` |
| `TinySQLDatabaseOpen(path)` | In-memory database, or a loaded snapshot: `{"handle"}` |
| `TinySQLDatabaseOpenWithOptions(json)` | Any storage mode (see below): `{"handle"}` |
| `TinySQLDatabaseExecute(h, sql, params)` | `{"rowsAffected"}` |
| `TinySQLDatabaseQuery(h, sql, params)` | `{"columns","rows"}` |
| `TinySQLDatabaseRun(h, sql, params)` | Either shape, chosen from the statement |
| `TinySQLDatabaseExecuteBatch(h, sql, [[params], ...])` | `{"rowsAffected","statements"}` |
| `TinySQLDatabaseExecuteScript(h, script)` | `{"rowsAffected","statements"}` |
| `TinySQLDatabaseSave(h, path)` | `{}`; writes a snapshot (gzip for `.gz`) |
| `TinySQLDatabaseSync(h)` | `{}`; flushes table-file storage modes |
| `TinySQLDatabaseClose(h)` | `{}`; invalidates the handle |
| `TinySQLDatabaseFree(buffer)` | Releases any returned buffer |

Statement responses carry `"inTransaction": true` while the handle is inside a
transaction, also next to an `"error"`.

### Open options

```json
{
  "mode": "wal",
  "path": "./data",
  "readOnly": false,
  "maxMemoryBytes": 268435456,
  "syncOnMutate": false,
  "compressFiles": false,
  "checkpointEvery": 1000,
  "checkpointIntervalMs": 30000,
  "checkpointMaxBytes": 67108864,
  "walSync": "normal",
  "encryptionKey": "base64 of 32 bytes"
}
```

Every member is optional. `mode` is `memory`, `snapshot`, `wal`,
`advanced_wal`, `disk`, `json`, `index`, `hybrid` or `paged_index`; it
defaults to `memory` without a path and `snapshot` with one. Unknown members
are rejected, which catches typos. `encryptionKey` applies to the table files
of `disk`, `json`, `index` and `hybrid`.

## Rules

- Inputs are borrowed NUL-terminated UTF-8 strings; `NULL` means empty.
- Every returned `char *` must be released with `TinySQLDatabaseFree`.
- A response is one JSON object; a nonempty `"error"` means failure.
- Parameters: JSON array of `null`, booleans, int64 integers, finite numbers,
  strings, `{"real": n}` or `{"blob": "base64"}`; placeholders `?`, `$1`, `:1`.
- Results: integers as JSON integers, REALs as `{"real": n}`, BLOBs as
  `{"blob": "..."}`, timestamps as RFC 3339 strings.
- Handles are never reused. Calls on one handle are serialized; separate
  handles are independent databases and run in parallel. Each handle pins one
  connection, so `BEGIN`/`COMMIT` span calls.
- The library performs no background writes on snapshots: `TinySQLDatabaseOpen`
  never modifies the file, and nothing is saved implicitly.

`TinySQLVersion`, `TinySQLExec`, `TinySQLSave`, `TinySQLLoad`, `TinySQLReset`
and `TinySQLFree` are the legacy single-database API of the former Python
bridge and remain exported for existing callers.

## Test

```sh
make test-c        # go test -race ./bindings/c and the C smoke program
```
