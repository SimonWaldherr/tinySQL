# tinySQL Developer Integration Guide

How to embed tinySQL in three setups:

1. Native Go applications
2. Browser/WASM applications
3. Custom web frontends talking to a WASM-backed tinySQL runtime

Sources: `example_test.go`, `import_example_test.go`, `cmd/demo`, `cmd/ragdemo`,
`cmd/query_files_wasm`, `cmd/wasm_browser`, `example_showcase.sql`.

> **Vector search & RAG:** tinySQL is also a retrieval backend (vector k-NN,
> BM25 full-text, hybrid retrieval, RAG context expansion). For AI/RAG use,
> start with the [RAG / AI usage guide](./rag-guide.md) and `cmd/ragdemo`.

## English

### 1. Overview

Three integration levels:

- **Directly in Go**: import `github.com/SimonWaldherr/tinySQL` and call the
  parser, execution, and import helpers.
- **Through `database/sql`**: import the public package
  `github.com/SimonWaldherr/tinySQL/driver` and open tinySQL through a DSN.
- **In the browser via WASM**: `cmd/query_files_wasm` runs tinySQL as a
  WebAssembly module driven from JavaScript.

Example files, each covering a different level:

- `example_test.go` — direct Go usage with `NewDB`, `NewParser`, `Execute`
- `import_example_test.go` — CSV, JSON, file, and auto-detection imports
- `cmd/demo/main.go` — the `database/sql` integration
- `cmd/query_files_wasm/main.go` and `app.js` — browser integration
- `example_showcase.sql` — the SQL surface tinySQL supports

### 2. Integrating tinySQL into Go projects

#### Formatting and minifying SQL text

tinySQL can format SQL without a database instance — useful for editors, logs,
review tools, and cache keys. No parser roundtrip is involved: literals, quoted
identifiers, and comments stay unchanged.

```go
source := "select id,name from users where note = 'two  spaces' and id=42"

pretty := tinysql.BeautifySQL(source)
// SELECT id, name
// FROM users
// WHERE note = 'two  spaces'
// AND id = 42

compact := tinysql.MinifySQL(pretty)
// SELECT id,name FROM users WHERE note='two  spaces' AND id=42
```

`BeautifySQL` structures the main clauses and upcases known keywords.
`MinifySQL` removes only insignificant whitespace. For `--` comments the line
break is preserved, otherwise the following SQL would become part of the
comment. Neither function validates SQL; use `ParseSQL` or `sqltools validate`
for syntax checks.

#### Direct API embedding

Use this when you want full control over parsing and execution:

```go
package main

import (
    "context"
    "fmt"

    tinysql "github.com/SimonWaldherr/tinySQL"
)

func main() {
    db := tinysql.NewDB()

    stmt, err := tinysql.ParseSQL(`CREATE TABLE users (id INT, name TEXT)`)
    if err != nil {
        panic(err)
    }
    _, err = tinysql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    stmt, _ = tinysql.ParseSQL(`INSERT INTO users VALUES (1, 'Alice')`)
    _, _ = tinysql.Execute(context.Background(), db, "default", stmt)

    stmt, _ = tinysql.ParseSQL(`SELECT id, name FROM users`)
    rs, err := tinysql.Execute(context.Background(), db, "default", stmt)
    if err != nil {
        panic(err)
    }

    for _, row := range rs.Rows {
        id, _ := tinysql.GetVal(row, "id")
        name, _ := tinysql.GetVal(row, "name")
        fmt.Println(id, name)
    }
}
```

Key points:

- `tinysql.NewDB()` creates a fresh in-memory database.
- The tenant in all examples is `default`, also the standard tenant in the
  browser and CLI code.
- `tinysql.ParseSQL(...)` and `tinysql.Execute(...)` are the lowest-level
  primitives. For multiple statements, split the SQL text yourself or reuse the
  helpers from the WASM tool.

For large result sets, use `ExecSQLStream` and always close a stream when
stopping early. `StreamOptions{Buffer: 0}` provides strict backpressure, and
`stream.Stats()` exposes progress without racing the consumer. See the
[API stability guide](./api-stability.md) for the compatibility and streaming
contract.

#### Using `database/sql`

If your code already expects a `database/sql` handle,
`github.com/SimonWaldherr/tinySQL/driver` is the supported entry point.
Anything below `internal/` can only be imported from inside this module, so
external tools and applications cannot import
`github.com/SimonWaldherr/tinySQL/internal/driver`.

Minimal DSN-based open:

```go
package main

import (
    "database/sql"
    "fmt"

    tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

func main() {
    db, err := sql.Open(tsqldriver.DriverName, "mem://?tenant=default")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    _, _ = db.Exec(`CREATE TABLE users (id INT, name TEXT)`)
    _, _ = db.Exec(`INSERT INTO users VALUES (?, ?)`, 1, "Alice")

    row := db.QueryRow(`SELECT name FROM users WHERE id = ?`, 1)
    var name string
    if err := row.Scan(&name); err != nil {
        panic(err)
    }

    fmt.Println(name)
}
```

Same thing with an explicit config instead of a hand-built DSN:

```go
// needs "context" and "time" in addition to the imports above
cfg := tsqldriver.DefaultOpenConfig()
cfg.Tenant = "default"
cfg.BusyTimeout = 500 * time.Millisecond

db, err := tsqldriver.OpenWithConfig(context.Background(), cfg)
```

DSN patterns from the repo:

- In-memory: `mem://?tenant=default`
- File-backed: `file:/path/to/db.dat?tenant=default&autosave=1`
- Pooling and busy timeout:
  `mem://?tenant=default&pool_readers=4&pool_writers=1&busy_timeout=250ms`

Recognized DSN options (an unknown or malformed option is an error, not a
silent default): `tenant`, `autosave`, `pool_readers` (aliases `read_pool`,
`reader_pool`), `pool_writers` (aliases `write_pool`, `writer_pool`),
`busy_timeout` (alias `busytimeout`), `mode`, `max_memory_bytes`, `read_only`,
`sync_on_mutate`, `compress_files`, `checkpoint_every`, `checkpoint_interval`,
`checkpoint_max_bytes`, `wal_sync`. Booleans accept `1/true/yes/on` and
`0/false/no/off`. `wal_sync` accepts `full` (the default, strongest available
flush) or `normal` (ordinary fsync on every WAL commit). On macOS, `normal`
matches SQLite `synchronous=FULL` without SQLite's separate `fullfsync=ON`;
it is not equivalent to SQLite `synchronous=NORMAL`.

Helpers in the public driver package:

- `driver.DriverName` — the registered `database/sql` driver name, `tinysql`
- `driver.Open(dsn)` for direct `database/sql` integration
- `driver.DefaultOpenConfig()` + `driver.OpenWithConfig(ctx, cfg)` for separated
  settings (DSN, pooling, ping timeout)
- `driver.OpenInMemory("default")` for tests and short-lived tools
- `driver.OpenFile("/path/to/db.dat")` for file-backed tools
- `driver.OpenWithDB(db)` to wrap an existing `*tinysql.DB`

Use the DSN for tinySQL-specific options, `database/sql` for pool parameters
(`MaxOpenConns`, `MaxIdleConns`, `ConnMaxLifetime`, `ConnMaxIdleTime`), and
`context.WithTimeout(...)` with `ExecContext`/`QueryContext`/`PingContext` per
query.

`QueryContext` now streams ordinary `SELECT` results through `sql.Rows`; do
not call `rows.Close` late when you only need a prefix, because it is the
signal that releases the query's producer, reader slot, and prepared-statement
binding. `ORDER BY`, grouping, joins, and other global query shapes preserve
their exact semantics by materializing before their first row.

```go
rows, err := db.QueryContext(ctx, `SELECT id, name FROM users WHERE active = ?`, true)
if err != nil { panic(err) }
defer rows.Close()
for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil { panic(err) }
    fmt.Println(id, name)
}
if err := rows.Err(); err != nil { panic(err) }
```

#### Transactions and concurrent writes

`BeginTx` takes a snapshot. Changes to different tables can commit in parallel;
if another connection modifies the same table between `BEGIN` and `COMMIT`, the
commit is rejected instead of overwriting the foreign change. Detect this with
`errors.Is(err, driver.ErrTransactionConflict)` and, where it matters, re-read
and retry the business operation.

```go
if err := tx.Commit(); errors.Is(err, tsqldriver.ErrTransactionConflict) {
    // Re-read the data and repeat the operation if needed.
}
```

#### Building your own tools and extensions

Keep imports on the public surface: `github.com/SimonWaldherr/tinySQL` for the
engine, parser, importers, and stable types, plus
`github.com/SimonWaldherr/tinySQL/driver` for `database/sql`. The root package
deliberately re-exports the important internal types so external projects need
no `internal/...` imports. For custom table-valued functions use
`tinysql.RegisterExternalTableFunc(...)` instead of depending on
`internal/engine`.

#### Importing files

Use the import helpers to load CSV, JSON, or XML into tinySQL.
`import_example_test.go` is the reference.

```go
result, err := tinysql.ImportCSV(ctx, db, "default", "users",
    strings.NewReader(csvData), &tinysql.ImportOptions{
        CreateTable:   true,
        TypeInference: true,
        HeaderMode:    "present",
    })
```

Helpers: `ImportCSV(...)`, `ImportJSON(...)`, `ImportFile(...)`,
`OpenFile(...)`.

Recommended options:

- `CreateTable: true` to create tables from external files automatically.
- `TypeInference: true` for automatic column typing.
- `HeaderMode: "present"` when the input definitely has a header row.

### 3. Integrating tinySQL into WASM projects

`cmd/query_files_wasm` demonstrates the full pattern:

1. Compile Go with `GOOS=js GOARCH=wasm`.
2. Ship `wasm_exec.js` with the page.
3. Load the WASM module from a small HTML/JS app.
4. Expose tinySQL functions on `window`.

```bash
cd cmd/query_files_wasm
./build.sh --build-only    # build only (-b)
./build.sh --serve         # build, then serve on http://localhost:8080 (-s)
./build.sh --skip-build --serve
```

Serving needs `python3` or `php` on the machine.

The module must be served over HTTP or HTTPS. `file://` URLs generally do not
work for browser WASM because `fetch()` and MIME types are required.

On static hosts the bundled loader first tries `query_files.wasm.gz` and
streams it through `DecompressionStream` into the WebAssembly compiler. It falls
back to `query_files.wasm` when that API is unavailable or the host already
applies HTTP compression. Reuse the loader from `cmd/query_files_wasm/app.js`
for a custom frontend.

#### Exported JS functions

`cmd/query_files_wasm/main.go` binds these globals on `window`:

- `importFile(fileName, fileContent, tableName)`
- `executeQuery(sql)`
- `executeMulti(sql)`
- `clearDatabase()`
- `dropTable(tableName)`
- `listTables()`
- `exportResults(format)`
- `getTableSchema(tableName)`
- `exportDatabase()`
- `importDatabase(...)`

Return values are JSON-like objects:

- Query success: `success`, `columns`, `rows`, `durationMs`, optional
  `statementsRun`
- Failure: `success: false`, `error`
- Import: `tableName`, `rowsImported`, `rowsSkipped`, `columns`, `warnings`,
  `delimiter`, `hadHeader`
- Export: `data`, `mimeType`, `ext`

#### Minimal browser bootstrap

```html
<script src="wasm_exec.js"></script>
<script>
  async function bootTinySQL() {
    const go = new Go();
    const result = await WebAssembly.instantiateStreaming(
      fetch("query_files.wasm"),
      go.importObject
    );
    go.run(result.instance);

    const queryResult = window.executeQuery("SELECT 1 AS one");
    console.log(queryResult);
  }

  bootTinySQL();
</script>
```

#### Practical frontend pattern

The UI in `cmd/query_files_wasm/app.js` follows the flow you would use yourself:

- Wait for WASM initialization before enabling the editor.
- Send uploaded file contents (JSON or text) to `importFile(...)`.
- Use `executeMulti(...)` when the SQL text contains multiple statements.
- Call `getTableSchema(...)` for schema panels and metadata views.
- Use `exportResults("csv" | "json" | "xml")` for download buttons.

The `Load Demo + Large Tables` button is a good example for larger datasets:
seed the demo tables first, then run a query combining joins, grouping, and
aggregation.

### 4. Integrating tinySQL into custom web frontends

For a React, Vue, Svelte, or vanilla JS frontend:

1. Create a small WASM bootstrap module.
2. Wrap the exported functions in a client object.
3. Keep UI state and database state separate.
4. Handle loading, errors, and empty results explicitly.

Suggested structure:

```text
src/
  wasm/
    boot.js
  components/
    QueryEditor.tsx
    ResultTable.tsx
  services/
    tinySqlClient.ts
```

A client wrapper:

```js
export async function initTinySql() {
  const go = new Go();
  const wasm = await WebAssembly.instantiateStreaming(
    fetch("/tinySQL.wasm"),
    go.importObject
  );
  go.run(wasm.instance);

  return {
    execute: (sql) => window.executeQuery(sql),
    executeMulti: (sql) => window.executeMulti(sql),
    importFile: (fileName, content, tableName) =>
      window.importFile(fileName, content, tableName),
    exportResults: (format) => window.exportResults(format),
    schema: (tableName) => window.getTableSchema(tableName)
  };
}
```

#### Frontend considerations

- Do not enable query actions before WASM is ready.
- Treat large result sets carefully: everything lives in browser memory.
- Use `executeMulti` if your editor accepts multiple SQL statements.
- For CSV/JSON/XML uploads, read the file in the browser and pass the text to
  `importFile(...)`.
- In the reference UI, Excel files are read with SheetJS and each sheet is
  imported as JSON.

### 5. What the repository examples demonstrate

`example_showcase.sql` covers more than simple `SELECT` queries: date and time
functions (`NOW`, `DATE_TRUNC`, `EOMONTH`), string functions (`UPPER`, `LOWER`,
`LENGTH`, `SPLIT`), regex and array helpers, joins, grouping, `HAVING`, temp
tables, and JSON expressions and updates.

### 6. Practical limits and recommendations

- The WASM examples bound SQL input; in `cmd/query_files_wasm` the limit is
  256 KiB (`maxSQLBytes`).
- Query timeouts are enabled by default in the browser tool.
- Browser setups have no filesystem persistence by default; implement it in the
  frontend or in the Go host if you need it.
- Test against every entry point you use: Go API, `database/sql`, WASM API, UI.

For Go-only apps prefer the direct package API or the `database/sql` driver. For
browser apps follow the `cmd/query_files_wasm` pattern: compile to WASM, expose
a small JS API, let the frontend own UI state while tinySQL owns data and query
execution.

## Deutsch

Der englische Abschnitt ist die vollstaendige Referenz und enthaelt alle
Codebeispiele; dieser Abschnitt fasst dieselben Fakten zusammen.

### 1. Ueberblick

tinySQL kann auf drei Ebenen integriert werden:

- **Direkt in Go**: Paket `github.com/SimonWaldherr/tinySQL`, Parser,
  Ausfuehrung und Importfunktionen direkt aufrufen.
- **Ueber `database/sql`**: oeffentliches Paket
  `github.com/SimonWaldherr/tinySQL/driver` importieren und tinySQL per DSN
  ansprechen.
- **Im Browser via WASM**: `cmd/query_files_wasm` laeuft als WebAssembly-Modul
  und wird aus JavaScript gesteuert.

### 2. Integration in Go-Projekte

- `BeautifySQL` / `MinifySQL`: SQL formatieren bzw. komprimieren ohne
  Datenbankinstanz und ohne Parser-Roundtrip. Kein SQL-Check — dafuer
  `ParseSQL` oder `sqltools validate`.
- `NewDB` / `ParseSQL` / `Execute`: niedrigste Ebene mit voller Kontrolle,
  Standard-Tenant `default`. Mehrere Statements vorher selbst splitten.
- `database/sql`: nur ueber `github.com/SimonWaldherr/tinySQL/driver`. Pakete
  unter `internal/` duerfen nur aus demselben Modul importiert werden, externe
  Tools koennen `internal/driver` also nicht nutzen. DSN-Muster, DSN-Optionen
  und Helfer siehe englischer Abschnitt 2.
- Transaktionen: `BeginTx` arbeitet auf einem Snapshot; Commit-Konflikte mit
  `errors.Is(err, driver.ErrTransactionConflict)` erkennen und die fachliche
  Operation wiederholen.
- Eigene tabellenwertige Funktionen ueber
  `tinysql.RegisterExternalTableFunc(...)` registrieren, nicht gegen
  `internal/engine` entwickeln.
- Import von CSV/JSON/XML: `ImportCSV`, `ImportJSON`, `ImportFile`, `OpenFile`;
  Referenz ist `import_example_test.go`.

### 3. Integration in WASM- und Frontend-Projekte

Mit `GOOS=js GOARCH=wasm` kompilieren, `wasm_exec.js` mit ausliefern, Modul aus
einer kleinen HTML/JS-App laden, Funktionen an `window` exportieren. Build in
`cmd/query_files_wasm` ueber `./build.sh --build-only` bzw. `./build.sh --serve`.

Wichtig: Das Modul muss ueber HTTP oder HTTPS laufen. `file://` funktioniert
fuer Browser-WASM in der Regel nicht sauber, weil `fetch()` und MIME-Typen
benoetigt werden.

Der mitgelieferte Loader versucht auf statischen Hosts zuerst
`query_files.wasm.gz` und streamt ihn mit `DecompressionStream` in den
WebAssembly-Compiler. Fehlt diese Browser-API oder liefert der Host bereits
HTTP-Kompression, faellt er auf `query_files.wasm` zurueck. Fuer ein eigenes
Frontend kann der Loader aus `cmd/query_files_wasm/app.js` uebernommen werden.

Exportierte JS-Funktionen, Rueckgabeobjekte und der Wrapper-Code stehen in den
englischen Abschnitten 3 und 4.

### 4. Grenzen und Empfehlungen

- Der Browser-Code begrenzt SQL-Text auf 256 KiB (`maxSQLBytes`).
- Query-Timeouts sind im Browser-Tool standardmaessig aktiv.
- In Browser-Setups gibt es keine Dateisystem-Persistenz; sie muss im Frontend
  oder im Go-Host selbst umgesetzt werden.
- Grosse Ergebnisse liegen komplett im Browser-Speicher; UI-Aktionen erst nach
  abgeschlossener WASM-Initialisierung freigeben.
- In der Referenz-UI werden Excel-Dateien mit SheetJS gelesen und pro Sheet als
  JSON importiert.
- Integrations-Tests gegen alle genutzten Einstiegspunkte fahren: Go-API,
  `database/sql`, WASM-API und UI.
