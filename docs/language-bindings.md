# Language bindings

tinySQL is written in Go and embeds directly into Go programs. Rust, Python,
Swift and any C-compatible host use one shared native library built from
[`bindings/c`](../bindings/c/README.md). Every binding runs the same engine in
the host process: there is no server, socket or runtime download.

| Host | Package | Links as | Guide |
| --- | --- | --- | --- |
| Go | `github.com/SimonWaldherr/tinySQL`, `.../driver` | Go package (pure Go, no cgo) | [developer integration](developer-integration.md) |
| Rust | crate `tinysql` in `bindings/rust` | static archive, built by `build.rs` | [README](../bindings/rust/README.md) |
| Python | package `tinysql` in `bindings/python` | shared library via `ctypes` | [README](../bindings/python/README.md) |
| Swift / Xcode | package `TinySQL` in `bindings/swift` | static XCFramework | [README](../bindings/swift/README.md) |
| C, C++, others | `bindings/c/include/tinysql.h` | static or shared library | [README](../bindings/c/README.md) |

```text
 Go application ──────────────────────────────┐
                                              ▼
 Rust crate ──┐                        tinySQL engine
 Python pkg ──┼─► C ABI v2 (bindings/c) ─► database/sql driver ─► storage
 Swift pkg ───┤    handles, JSON values      (pinned connection)   (memory,
 C / C++ ─────┘                                                     WAL, disk…)
```

## Quick start

Each example creates a table, inserts with bound parameters and reads a row.

```go
// Go: go get github.com/SimonWaldherr/tinySQL (error handling omitted)
db := tinysql.NewDB()
defer db.Close()
tinysql.ExecScript(ctx, db, "default", `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`)
tinysql.ExecSQLArgs(ctx, db, "default", "INSERT INTO users VALUES (?, ?)", 1, "Ada")
rs, err := tinysql.ExecSQLArgs(ctx, db, "default", "SELECT name FROM users WHERE id = ?", 1)
```

```rust
// Rust: tinysql = { path = "bindings/rust" }
let db = tinysql::Database::open_in_memory()?;
db.execute_script("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")?;
db.execute("INSERT INTO users VALUES (?, ?)", tinysql::params![1, "Ada"])?;
let name: Option<String> = db.query_value("SELECT name FROM users WHERE id = ?", tinysql::params![1])?;
```

```python
# Python: pip install ./bindings/python
import tinysql
with tinysql.connect() as conn:
    conn.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users VALUES (?, ?)", (1, "Ada"))
    name = conn.execute("SELECT name FROM users WHERE id = ?", (1,)).fetchone()["name"]
```

```swift
// Swift: add bindings/swift as a local package after `make build-apple`
let db = try await Database.open()
try await db.executeScript("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
try await db.execute("INSERT INTO users VALUES (?, ?)", parameters: [1, "Ada"])
let name = try await db.query("SELECT name FROM users WHERE id = ?", parameters: [1])
    .value(row: 0, column: "name")?.stringValue
```

```c
/* C: make build-c, then link bindings/c/build/libtinysql.a */
char *opened = TinySQLDatabaseOpenWithOptions(NULL);        /* {"handle":1} */
/* ... parse the handle, then: */
char *rows = TinySQLDatabaseRun(handle, "SELECT name FROM users WHERE id = ?", "[1]");
TinySQLDatabaseFree(rows);
```

## Feature matrix

| Capability | Go | Rust | Python | Swift |
| --- | --- | --- | --- | --- |
| In-memory database | `NewDB` | `Database::open_in_memory` | `connect()` | `Database()` |
| Snapshot load/save | `LoadFromFile` / `SaveToFile` | `open_snapshot` / `save` | `connect(path)` / `save` | `Database(snapshot:)` / `save(to:)` |
| Durable storage modes | `OpenDB(StorageConfig)` | `OpenOptions::mode` | `connect(dir, mode=...)` | `Database(directory:storage:)` |
| Bound parameters `?`, `$1`, `:1` | `ExecSQLArgs`, `database/sql` | `params!` | DB-API `qmark` | `[SQLValue]` |
| Prepared batch | `database/sql` `Prepare` | `execute_batch` | `executemany` | `executeBatch` |
| Multi-statement scripts | `ExecScript` | `execute_script` | `executescript` | `executeScript` |
| Transactions | `database/sql` `BeginTx` or SQL `BEGIN` | `transaction(closure)` | `with conn.transaction():` | `BEGIN`/`COMMIT` + `isInTransaction` |
| Encryption at rest | `StorageConfig.EncryptionKey` | `encryption_key` | `encryption_key=` | `StorageOptions.encryptionKey` |
| Streaming results | `ExecSQLStream`, `database/sql` rows | materialized | materialized | materialized |

Go additionally offers streaming, columnar results, the query builder, custom
table-valued functions, stored procedures, file importers and multi-tenancy.
The native bindings expose the SQL surface; everything available through SQL
(imports via table functions, full-text and vector search, GIS, jobs,
triggers, views) works in every language.

## Values

| SQL value | JSON on the C ABI | Go | Rust `Value` | Python | Swift `SQLValue` |
| --- | --- | --- | --- | --- | --- |
| NULL | `null` | `nil` | `Null` | `None` | `.null` |
| INTEGER (64-bit) | `42` | `int` | `Integer(i64)` | `int` | `.integer(Int64)` |
| REAL | `{"real": 1.5}` | `float64` | `Real(f64)` | `float` | `.real(Double)` |
| TEXT | `"text"` | `string` | `Text(String)` | `str` | `.text(String)` |
| BOOL | `true` | `bool` | `Boolean(bool)` | `bool` | `.boolean(Bool)` |
| BLOB | `{"blob": "base64"}` | `[]byte` | `Blob(Vec<u8>)` | `bytes` | `.blob(Data)` |

REALs are tagged so an integral double such as `3.0` keeps its type across
JSON. Integer arithmetic and `SUM` over integer columns return integers;
division and `AVG` return REAL (see [SQL feature gaps](sql-feature-gaps.md#integer-arithmetic)).
Timestamps and other engine types arrive as text (RFC 3339 for timestamps).

## Transactions and concurrency

- Every native database handle pins one engine connection, so `BEGIN`,
  `COMMIT` and `ROLLBACK` span calls on that handle. Responses report
  `inTransaction`, which the bindings expose (`in_transaction`,
  `isInTransaction`).
- Calls on one handle are serialized inside the library. Separate handles are
  independent databases and run in parallel; Python releases the GIL during
  native calls.
- Each handle owns its database. Never open the same durable directory or
  snapshot file for writing from two handles or processes at once; share one
  handle instead. Several `readOnly` handles may share an artifact that no
  writer changes.
- A process can contain only one Go runtime built as a C archive. Link one
  tinySQL library per process and share it between components.

## Choosing persistence

| Need | Mode |
| --- | --- |
| Tests, caches, scratch data | in-memory |
| A document-style file the user opens and saves | snapshot (`save` writes a GOB file, `.gz` compresses) |
| An app database that survives crashes | `wal` (small/medium) or `advanced_wal` |
| Readable, diffable files | `json` |
| Data larger than RAM | `index` or `hybrid` with `maxMemoryBytes` |
| Shipping a read-only artifact | any directory mode with `readOnly` |

Durable modes persist every acknowledged write; closing flushes them. The
[storage guide](storage-guide.md) covers checkpoints, backups and the
encryption scope.

## Performance

- Prefer batches for bulk writes: `executemany` / `execute_batch` /
  `executeBatch` prepare the statement once and cross the language boundary
  once. Wrap large batches in a transaction.
- Scripts run many statements in one call, including DDL with triggers.
- Native results are materialized and encoded as compact JSON. Select only the
  needed columns and page large results with `LIMIT` or a keyset condition
  (`WHERE id > ? ORDER BY id LIMIT 500`).
- Create indexes for equality and range filters (`CREATE INDEX ...`); the
  [automatic index guide](automatic-indexes.md) explains the advisor.
- Reuse one handle per database rather than reopening it for each request.

## The C ABI

`bindings/c/include/tinysql.h` is the contract; its comments are normative.
In short: functions take NUL-terminated UTF-8 strings, return one JSON object
per call that the caller frees with `TinySQLDatabaseFree`, and report failures
in an `"error"` member. Handles are opaque integers that are never reused.
`TinySQLABIVersion()` returns `2`; later versions only add functions and
optional response members. The former single-database functions of the Python
bridge (`TinySQLExec`, ...) remain exported for compatibility.

## Building and distributing

| Language | Build | Distribute |
| --- | --- | --- |
| C | `make build-c` → `bindings/c/build/{libtinysql.a, libtinysql.so, tinysql.h}` | Release archives of native targets contain both libraries and `tinysql.h`. |
| Python | `make -C bindings/python build` or `pip install ./bindings/python` | Build a platform wheel per OS/architecture (`pip wheel ./bindings/python`). |
| Rust | `cargo build` (needs Go and a C toolchain) | Set `TINYSQL_LIB_DIR` to a prebuilt `libtinysql.a` for cross-compilation. |
| Swift | `make build-apple` (macOS with Xcode) | Ship the package with its XCFramework or a checksummed binary target. |

`make test-bindings` runs the C, Python and Rust suites; `make test-swift`
runs the Swift tests on macOS. CI runs all of them.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| Python: `libtinysql was not found` | Run `make -C bindings/python build` or set `TINYSQL_LIBRARY` to the library path. |
| `implements tinySQL ABI 1` | The loaded library is older than the binding; rebuild it from the same checkout. |
| Rust: `failed to run go` | Install Go or set `TINYSQL_LIB_DIR`. |
| Linker errors about `CoreFoundation`/`resolv` (macOS) or `pthread` (Linux) | Link the system libraries listed in [bindings/c](../bindings/c/README.md). |
| Corrupt or diverging data after concurrent use | Two handles or processes wrote the same directory; keep one writer handle. |
| `snapshot must be a regular file` | `connect(path)` without `mode` loads a snapshot; pass a storage `mode` to open a directory. |

## Deutsch

tinySQL lässt sich in Go direkt als Paket einbinden. Rust, Python, Swift und
C/C++ nutzen eine gemeinsame native Bibliothek aus `bindings/c` mit einer
stabilen C-ABI (Version 2): Handles statt Zeiger, JSON-Antworten, gebundene
Parameter (`?`, `$1`, `:1`), typisierte Werte inklusive BLOBs und exakter
64-Bit-Integer. Alle Bindings bieten In-Memory-Datenbanken, Snapshots,
dauerhafte Storage-Modi (`wal`, `disk`, `json`, `hybrid` …) mit optionaler
Verschlüsselung, Skripte, vorbereitete Batches und Transaktionen, deren Status
die Engine mit jeder Antwort meldet. Aufrufe auf einem Handle werden
serialisiert, verschiedene Handles laufen parallel. Für Massendaten Batches
in einer Transaktion verwenden und große Ergebnisse mit `LIMIT` seitenweise
lesen. `make test-bindings` testet C, Python und Rust; Swift läuft unter macOS
mit `make test-swift`.
