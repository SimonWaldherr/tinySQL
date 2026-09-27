# tinySQL developer integration

tinySQL can run directly in a Go process, through `database/sql`, in a
browser/WebAssembly application, or inside Rust, Python, Swift and C programs
through the shared C ABI. Choose the smallest integration surface that fits
the host; the [language bindings guide](language-bindings.md) compares the
non-Go hosts. For storage, DSN options, durability, and read-only serving, use
the [storage guide](storage-guide.md); for retrieval applications, start with the
[RAG guide](rag-guide.md).

| Host | Start with | Use it when |
| --- | --- | --- |
| Go application | `tinysql.NewDB`, `ExecSQLArgs`, `ExecScript` | You need direct control of the database and statements. |
| Existing SQL-oriented Go application | `github.com/SimonWaldherr/tinySQL/driver` | The application already uses `database/sql`. |
| Browser application | `cmd/query_files_wasm` | Data and queries should run locally in WebAssembly. |
| Swift / Xcode application | [Swift package](../bindings/swift/README.md) | Embed the engine in macOS or iOS/iPadOS with actor isolation, bound parameters and durable storage. |
| Rust application | [Rust crate](../bindings/rust/README.md) | A `Send + Sync` database with typed rows, batches and transactions, linked statically. |
| Python application | [Python package](../bindings/python/README.md) | A DB-API 2.0 connection for scripts, notebooks and services. |
| C, C++ or another FFI host | [C ABI](../bindings/c/README.md) | Any language that can call C functions and parse JSON. |

Reference implementations live in `example_test.go`, `import_example_test.go`,
`cmd/demo`, `cmd/ragdemo`, `cmd/query_files_wasm`, `cmd/wasm_browser`, and
`example_showcase.sql`.

## Direct Go API

Use the root module for the public parser, execution, and import APIs. Code
below an `internal/` directory is not available to external modules.

```go
db := tinysql.NewDB()
defer db.Close()

// Schema and seed data: one call, statements split by the engine lexer.
if _, err := tinysql.ExecScript(ctx, db, "default", `
    CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
    CREATE INDEX users_name ON users (name);`); err != nil {
    return err
}

// Bind values instead of formatting them into SQL text.
if _, err := tinysql.ExecSQLArgs(ctx, db, "default",
    "INSERT INTO users VALUES (?, ?)", 1, "O'Hara"); err != nil {
    return err
}
rs, err := tinysql.ExecSQLArgs(ctx, db, "default",
    "SELECT id, name FROM users WHERE name = ?", "O'Hara")
if err != nil {
    return err
}
for _, row := range rs.Rows {
    fmt.Println(row["id"], row["name"])
}
```

`NewDB` creates an in-memory database; `OpenDB` opens a durable one (see the
[storage guide](storage-guide.md)). `default` is the conventional tenant in
examples, but applications may use their own tenant names.

| Function | Use it for |
| --- | --- |
| `ExecSQL(ctx, db, tenant, sql)` | One statement without parameters. |
| `ExecSQLArgs(ctx, db, tenant, sql, args...)` | One statement with `?`, `$1` or `:1` parameters. Strings, `[]byte`, numbers, booleans, `nil`, `time.Time` and JSON-marshalable values are bound as literals; placeholder characters inside quotes or comments are left alone. |
| `ExecScript(ctx, db, tenant, script)` | Several statements; returns the last result. Semicolons in literals, comments and `CREATE TRIGGER ... BEGIN ... END` bodies do not split. Stops at the first failing statement ("statement N: ..."). |
| `ParseSQL` + `Execute` | Parse once, execute many times without parameters. |
| `ExecSQLStream` | Large results; close the stream as soon as the consumer stops. |
| `ExecSQLColumnar` | Column-oriented results for analytics. |
| `sqlutil.SplitStatements` | Split a script without executing it (editors, migration tools). |

Transaction control (`BEGIN`/`COMMIT`) belongs to the `database/sql` driver
below; the direct API executes each statement immediately. Integer arithmetic
and `SUM` over integer columns return `int`; division and `AVG` return
`float64`. The [API stability guide](api-stability.md) covers stream ownership
and backpressure.

`BeautifySQL` and `MinifySQL` format or compact SQL without a database or parser
round trip. They preserve literals, quoted identifiers, and comments, but do not
validate syntax; use `ParseSQL` or `sqltools validate` when validation is needed.

```go
pretty := tinysql.BeautifySQL("select id,name from users where id=42")
compact := tinysql.MinifySQL(pretty)
```

## `database/sql`

Import the public driver package and reuse one `*sql.DB` wherever components
must share data. Each named `sql.Open` owns a separate tinySQL database, even
when the `mem://` DSN text is identical.

```go
import (
    "database/sql"

    tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

db, err := sql.Open(tsqldriver.DriverName, "mem://?tenant=default")
if err != nil {
    return err
}
defer db.Close()

if _, err := db.Exec(`CREATE TABLE users (id INT, name TEXT)`); err != nil {
    return err
}
if _, err := db.Exec(`INSERT INTO users VALUES (?, ?)`, 1, "Alice"); err != nil {
    return err
}
```

Use `QueryContext`/`ExecContext` with a request context, close `sql.Rows` early
when reading a prefix, and configure pool lifetime through `database/sql`.
`ORDER BY`, joins, grouping, and other global query shapes materialize before
their first streamed row so SQL semantics remain exact.

For a named DSN, config fields, storage modes, WAL settings, and their limits,
see the [storage guide](storage-guide.md#databasesql-driver). Typical starting
points are:

```go
cfg := tsqldriver.DefaultOpenConfig()
cfg.Tenant = "default"
db, err := tsqldriver.OpenWithConfig(ctx, cfg)
if err != nil {
    return err
}
defer db.Close()

// Workload-specific persistent profiles:
navCfg := tsqldriver.OfflineNavigationOpenConfig("./nav-artifact")
ragCfg := tsqldriver.RAGOpenConfig("./rag-artifact")
toolCfg := tsqldriver.EmbeddedToolOpenConfig("./tool.db")
_, _, _ = navCfg, ragCfg, toolCfg
```

`OpenWithDB` binds a pool to a caller-supplied `*tinysql.DB`; close the pool
before the native database. A database opened with a durable `StorageMode`
persists every acknowledged write, exactly like the equivalent `mode=` DSN. It does not change a process-wide default. Legacy
empty-DSN callers that intentionally need that behavior must call
`driver.SetDefaultDB` explicitly.

Transactions use snapshots. If another connection changes the same table before
`COMMIT`, retry the complete business operation after checking
`errors.Is(err, driver.ErrTransactionConflict)`. Prepared statements and batch
writes should normally share that transaction.

Keep extensions on public APIs: register custom table-valued functions with
`tinysql.RegisterExternalTableFunc`. Use `ImportCSV`, `ImportJSON`, `ImportFile`,
or `OpenFile` for file ingestion; `import_example_test.go` shows options for
table creation, type inference, and headers.

## Browser and WebAssembly

`cmd/query_files_wasm` is the maintained browser reference. Build and serve it
from its directory:

```sh
./build.sh --build-only
./build.sh --serve
```

Serve WASM over HTTP or HTTPS; `file://` normally lacks the fetch and MIME
handling it needs. The bundled loader in `cmd/query_files_wasm/app.js` first
tries the compressed artifact and falls back to the ordinary WASM file.

The module exposes these functions on its WASM runtime global. The reference
app reaches them through `TinySQLWasmClient`:

```text
importFile       executeQuery       executeMulti       clearDatabase
dropTable        listTables         exportResults      getTableSchema
exportDatabase   importDatabase
```

They return JSON-like success objects or `{success: false, error}`. Query
results include columns, rows, and duration; import/export responses add their
respective metadata. Treat these as the application boundary: wait for WASM
initialization before enabling actions, keep UI state outside the database, and
handle loading, errors, and empty results explicitly.

For React, Vue, Svelte, or vanilla JavaScript, wrap those globals in a small
client module. The reference UI demonstrates uploads, multi-statement execution,
schema panels, result export, and large demo tables. Browser data is in memory
unless the frontend or host persists it. The reference tool limits SQL input to
256 KiB and enables query timeouts; retain equivalent bounds in a custom UI.

## Deutsch

tinySQL laesst sich direkt in Go, ueber `database/sql`, im Browser mit WASM
sowie ueber die gemeinsame C-ABI in Rust, Python, Swift und C einbinden (siehe
[Language-Bindings-Leitfaden](language-bindings.md)). Fuer neue Go-Anwendungen
ist die direkte API mit `NewDB`, `ExecSQLArgs` (gebundene Parameter) und
`ExecScript` (mehrere Anweisungen) passend; bestehende SQL-Anwendungen
verwenden das oeffentliche Paket `github.com/SimonWaldherr/tinySQL/driver`.
Pakete unter `internal/` sind keine externe API.

Eine `*sql.DB` teilt eine tinySQL-Datenbank mit ihren Verbindungen. Ein zweites
`sql.Open` erzeugt dagegen auch bei gleichem `mem://`-DSN eine neue Datenbank.
DSN-Optionen, Speicher-Modi, Haltbarkeit und Read-only-Betrieb stehen in der
[Storage-Anleitung](storage-guide.md). Bei Transaktionskonflikten die gesamte
fachliche Operation nach `driver.ErrTransactionConflict` erneut ausfuehren.

Fuer Browser dient `cmd/query_files_wasm` als Vorlage: WASM ueber HTTP(S)
ausliefern, auf die Initialisierung warten und die exportierten JavaScript-
Funktionen hinter einem kleinen Client kapseln. Grosse Ergebnisse verbleiben im
Browser-Speicher; Eingabegroessen und Laufzeiten deshalb begrenzen.

Die englischen Abschnitte oben enthalten die vollstaendige API- und
Beispielreferenz. Fuer RAG siehe den [RAG-Leitfaden](rag-guide.md), fuer Streams
den [API-Stabilitaetsleitfaden](api-stability.md).
