# TinySQL Developer Integration Guide

This guide explains how to embed TinySQL in three common setups:

1. Native Go applications
2. Browser/WASM applications
3. Custom web frontends that talk to a WASM-backed TinySQL runtime

It is based on the repo examples in `example_test.go`, `import_example_test.go`, `cmd/demo`, `cmd/query_files_wasm`, and `cmd/wasm_browser`.

## Deutsch

### 1. Ueberblick

TinySQL kann auf drei Ebenen integriert werden:

- **Direkt in Go**: Du arbeitest mit dem Paket `github.com/SimonWaldherr/tinySQL` und rufst Parser, Ausfuehrung und Importfunktionen direkt auf.
- **Ueber `database/sql`**: Wenn du bereits ein SQL-typisches Projekt hast, importiere das oeffentliche Paket `github.com/SimonWaldherr/tinySQL/driver` und sprich TinySQL wie eine Datenbank per DSN an.
- **Im Browser via WASM**: Der Build `cmd/query_files_wasm` zeigt, wie TinySQL als WebAssembly-Modul laeuft und aus JavaScript angesprochen wird.

Die Beispiele in diesem Repository sind bewusst unterschiedlich aufgebaut:

- `example_test.go` zeigt die direkte Go-Nutzung mit `NewDB`, `NewParser` und `Execute`.
- `import_example_test.go` zeigt CSV-, JSON-, Datei- und Auto-Importe.
- `cmd/demo/main.go` zeigt die Nutzung ueber `database/sql`.
- `cmd/query_files_wasm/main.go` und `cmd/query_files_wasm/app.js` zeigen die Browser-Integration.
- `example_showcase.sql` zeigt typische SQL-Funktionen, die TinySQL bereits abdeckt.

### 2. Integration in Go-Projekte

#### Direktes API-Embedding

Wenn du TinySQL als Engine in deinem Go-Projekt verwenden willst, ist das die direkteste Variante:

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

Wichtige Punkte:

- `tinysql.NewDB()` erzeugt eine neue In-Memory-Datenbank.
- Der Tenant-Name ist im Beispiel `default`. Das ist auch der Standard in den Browser- und CLI-Beispielen.
- `tinysql.ParseSQL(...)` und `tinysql.Execute(...)` sind die niedrigste gemeinsame Ebene, wenn du volle Kontrolle willst.
- Wenn du mehrere Statements ausfuehren willst, kannst du den SQL-Text vorher splitten oder die vorhandenen Helfer aus dem WASM-Tool als Vorbild nehmen.

#### `database/sql` verwenden

Wenn du schon mit `database/sql` arbeitest, ist das oeffentliche Paket `github.com/SimonWaldherr/tinySQL/driver` die sauberste Integration.

Wichtig fuer Go-Projekte: `internal/` ist ein Sprachfeature. Pakete unter `internal/` duerfen nur aus demselben Modul importiert werden. Deshalb koennen die Beispiele unter `cmd/` zwar `github.com/SimonWaldherr/tinySQL/internal/driver` nutzen, externe Tools und Anwendungen aber nicht. Fuer andere Module ist `github.com/SimonWaldherr/tinySQL/driver` der stabile Einstiegspunkt.

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

DSN-Muster aus dem Repo:

- In-Memory: `mem://?tenant=default`
- Datei-basiert: `file:/pfad/zur/db.dat?tenant=default&autosave=1`

Nuetzliche Helfer aus dem oeffentlichen Driver-Paket:

- `driver.Open(dsn)` fuer den direkten `database/sql`-Einstieg
- `driver.OpenInMemory("default")` fuer kurzlebige Tests oder Werkzeuge
- `driver.OpenFile("/pfad/zur/db.dat")` fuer dateibasierte Tools

#### Eigene Werkzeuge und Erweiterungen bauen

Wenn du ein eigenes Tool auf tinySQL aufsetzt, halte deine Imports auf der oeffentlichen API:

- `github.com/SimonWaldherr/tinySQL` fuer Engine, Parser, Importer und stabile Typen
- `github.com/SimonWaldherr/tinySQL/driver` fuer `database/sql`

Die Root-API re-exportiert bewusst wichtige Typen aus internen Paketen, damit andere Projekte keine `internal/...`-Imports brauchen. Wenn dein Tool eigene tabellenwertige Funktionen bereitstellen soll, nutze `tinysql.RegisterExternalTableFunc(...)` als Erweiterungspunkt statt direkt gegen `internal/engine` zu entwickeln.

#### Dateien importieren

Die Import-Helfer sind ideal, wenn deine Anwendung CSV, JSON oder XML nach tinySQL laden soll. Die Beispiele in `import_example_test.go` sind die beste Referenz.

```go
result, err := tinysql.ImportCSV(ctx, db, "default", "users",
    strings.NewReader(csvData), &tinysql.ImportOptions{
        CreateTable:   true,
        TypeInference: true,
        HeaderMode:    "present",
    })
```

Typische Helfer:

- `ImportCSV(...)`
- `ImportJSON(...)`
- `ImportFile(...)`
- `OpenFile(...)`

Empfehlungen:

- Nutze `CreateTable: true`, wenn du aus externen Dateien direkt Tabellen erzeugen willst.
- Nutze `TypeInference: true`, wenn Spalten automatisch typisiert werden sollen.
- Nutze `HeaderMode: "present"`, wenn das Format garantiert Kopfzeilen hat.
- Nutze die Options aus den Beispielen als Startpunkt, statt die Importlogik neu zu schreiben.

### 3. Integration in WASM-Projekte

Der Browser-Build `cmd/query_files_wasm` zeigt das komplette Muster:

1. Go wird mit `GOOS=js GOARCH=wasm` kompiliert.
2. `wasm_exec.js` wird mit ausgeliefert.
3. Eine kleine HTML/JS-App laedt das WASM-Modul.
4. TinySQL exportiert globale Funktionen an `window`.

Build:

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

Oder lokal mit Server:

```bash
cd cmd/query_files_wasm
./build.sh --serve
```

Wichtig: Das Modul muss ueber HTTP oder HTTPS laufen. `file://` funktioniert fuer Browser-WASM in der Regel nicht sauber, weil `fetch()` und MIME-Typen benoetigt werden.

#### Exportierte JS-Funktionen

`cmd/query_files_wasm/main.go` bindet diese Funktionen an `window`:

- `importFile(fileName, fileContent, tableName)`
- `executeQuery(sql)`
- `executeMulti(sql)`
- `clearDatabase()`
- `dropTable(tableName)`
- `listTables()`
- `exportResults(format)`
- `getTableSchema(tableName)`

Die Rueckgaben sind JSON-objektartige Strukturen. Beispiele aus dem Code:

- Erfolg bei Query: `success`, `columns`, `rows`, `durationMs`, optional `statementsRun`
- Fehler: `success: false`, `error`
- Import: `tableName`, `rowsImported`, `rowsSkipped`, `columns`, `warnings`, `delimiter`, `hadHeader`
- Export: `data`, `mimeType`, `ext`

#### Minimaler Browser-Start

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

#### Praktisches Frontend-Muster

Die UI in `cmd/query_files_wasm/app.js` nutzt denselben Ablauf wie ein eigenes Projekt:

- Erst warten, bis die WASM-Funktionen bereit sind.
- Dann Datei-Uploads als JSON- oder Textinhalt an `importFile(...)` uebergeben.
- Fuer SQL mit mehreren Statements `executeMulti(...)` verwenden.
- Fuer Schema-Ansichten `getTableSchema(...)` aufrufen.
- Fuer Export-Buttons `exportResults("csv" | "json" | "xml")` verwenden.

Das `Load Demo + Large Tables`-Beispiel in der UI zeigt ausserdem einen guten Integrationsfall fuer groessere Datensaetze: erst Demo-Tabellen laden, dann eine Abfrage ausfuehren, die JOINs, Aggregationen und Gruppierungen kombiniert.

### 4. Integration in eigene Web-Frontends

Wenn du ein eigenes React-, Vue-, Svelte- oder Vanilla-JS-Frontend baust, ist die Architektur einfach:

1. Baue ein kleines WASM-Init-Modul.
2. Speichere die exportierten Funktionen in einem Wrapper-Objekt.
3. Trenne UI-State und DB-State klar.
4. Reagiere auf Ladezustand, Fehler und leere Resultate.

Empfohlene Struktur:

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

Ein robuster Wrapper kann so aussehen:

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

#### Was du im Frontend beachten solltest

- Warte mit UI-Aktionen, bis die WASM-Initialisierung abgeschlossen ist.
- Behandle grosse Ergebnisse bewusst, weil alles im Browser-Speicher liegt.
- Nutze `executeMulti`, wenn dein Editor mehrere Statements erlauben soll.
- Wenn du CSV/JSON/XML hochlaedst, kannst du die Browser-Datei zuerst lesen und den Text an `importFile(...)` weiterreichen.
- In der referenzierten UI werden Excel-Dateien im Browser mit SheetJS gelesen und pro Sheet als JSON importiert.

### 5. Was die Beispiele im Repo zeigen

`example_showcase.sql` zeigt, dass TinySQL im Projekt fuer mehr als nur einfache SELECTs genutzt wird:

- Datumsfunktionen wie `NOW`, `DATE_TRUNC`, `EOMONTH`
- Stringfunktionen wie `UPPER`, `LOWER`, `LENGTH`, `SPLIT`
- Regex- und Array-Funktionen
- JOINs, GROUP BY, HAVING und Temp Tables
- JSON-Ausdruecke und Aktualisierungen

Das ist wichtig fuer Entwickler: TinySQL ist nicht nur eine Lern-Demo, sondern kann als kleiner Embedded-SQL-Kern fuer realistische Arbeitsablaeufe dienen.

### 6. Praktische Grenzen und Empfehlungen

- Die WASM-Beispiele begrenzen SQL-Text auf eine groessere, aber endliche Maximalgroesse. Im Browser-Code liegt sie bei 256 KiB.
- Query-Timeouts sind im Browser-Tool standardmaessig aktiv.
- In Browser-Setups gibt es standardmaessig keine Dateisystem-Persistenz; wenn du Persistenz brauchst, musst du sie im Frontend oder im Go-Host selbst umsetzen.
- Fuehre echte Integrations-Tests gegen die jeweiligen Einstiegspunkte aus: Go-API, `database/sql`, WASM-API und UI.

## English

### 1. Overview

TinySQL can be integrated at three levels:

- **Directly in Go**: use the package `github.com/SimonWaldherr/tinySQL` and call the parser, execution, and import helpers directly.
- **Through `database/sql`**: if your project already uses SQL-style APIs, import the public package `github.com/SimonWaldherr/tinySQL/driver` and access TinySQL through a DSN.
- **In the browser via WASM**: `cmd/query_files_wasm` shows how TinySQL runs as a WebAssembly module and is controlled from JavaScript.

The repository examples are intentionally complementary:

- `example_test.go` shows direct Go usage with `NewDB`, `NewParser`, and `Execute`.
- `import_example_test.go` shows CSV, JSON, file, and auto-detection imports.
- `cmd/demo/main.go` shows the `database/sql` integration.
- `cmd/query_files_wasm/main.go` and `cmd/query_files_wasm/app.js` show browser integration.
- `example_showcase.sql` demonstrates the SQL surface TinySQL already supports.

### 2. Integrating TinySQL into Go projects

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
- The tenant name in the examples is `default`, which is also the standard tenant in the browser and CLI code.
- `tinysql.ParseSQL(...)` and `tinysql.Execute(...)` are the lowest-level primitives if you want exact control.

#### Using `database/sql`

If your code already expects a `database/sql` handle, the public package `github.com/SimonWaldherr/tinySQL/driver` is usually the cleanest route.

Important Go detail: `internal/` is one of Go's package visibility rules. Anything below `internal/` can only be imported from within the same module tree. That is why the repository's own commands can use `github.com/SimonWaldherr/tinySQL/internal/driver`, while external tools cannot. For other modules, `github.com/SimonWaldherr/tinySQL/driver` is the supported entry point.

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

DSN patterns from the repo:

- In-memory: `mem://?tenant=default`
- File-backed: `file:/path/to/db.dat?tenant=default&autosave=1`

Useful helpers from the public driver package:

- `driver.Open(dsn)` for direct `database/sql` integration
- `driver.OpenInMemory("default")` for tests and short-lived tools
- `driver.OpenFile("/path/to/db.dat")` for file-backed tools

#### Building your own tools and extensions

When you build tooling on top of tinySQL, keep your imports on the public surface:

- `github.com/SimonWaldherr/tinySQL` for the engine, parser, importers, and stable re-exported types
- `github.com/SimonWaldherr/tinySQL/driver` for `database/sql`

The root package deliberately re-exports the important types from internal packages so external projects do not need `internal/...` imports. If your tool needs custom table-valued functions, use `tinysql.RegisterExternalTableFunc(...)` instead of depending on `internal/engine`.

#### Importing files

Use the import helpers when your application needs to load CSV, JSON, or XML into TinySQL. The examples in `import_example_test.go` are the best reference.

```go
result, err := tinysql.ImportCSV(ctx, db, "default", "users",
    strings.NewReader(csvData), &tinysql.ImportOptions{
        CreateTable:   true,
        TypeInference: true,
        HeaderMode:    "present",
    })
```

Typical helpers:

- `ImportCSV(...)`
- `ImportJSON(...)`
- `ImportFile(...)`
- `OpenFile(...)`

Recommended defaults:

- `CreateTable: true` when you want tables created automatically.
- `TypeInference: true` when you want automatic column typing.
- `HeaderMode: "present"` when the input format definitely has a header row.

### 3. Integrating TinySQL into WASM projects

The browser build in `cmd/query_files_wasm` demonstrates the full pattern:

1. Compile Go with `GOOS=js GOARCH=wasm`.
2. Ship `wasm_exec.js` with the page.
3. Load the WASM module from a small HTML/JS app.
4. Expose TinySQL functions on `window`.

Build:

```bash
cd cmd/query_files_wasm
./build.sh --build-only
```

Or serve locally:

```bash
cd cmd/query_files_wasm
./build.sh --serve
```

The module should be served over HTTP or HTTPS. Browsers generally do not handle WASM initialization reliably from `file://` URLs because `fetch()` and MIME types matter.

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

The returned values are JSON-like objects. From the code you can expect:

- Query success: `success`, `columns`, `rows`, `durationMs`, optional `statementsRun`
- Failure: `success: false`, `error`
- Import: `tableName`, `rowsImported`, `rowsSkipped`, `columns`, `warnings`, `delimiter`, `hadHeader`
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

The UI in `cmd/query_files_wasm/app.js` follows the same flow you would use in your own application:

- Wait for WASM initialization before enabling the editor.
- Send uploaded file contents to `importFile(...)`.
- Use `executeMulti(...)` when the SQL text contains multiple statements.
- Call `getTableSchema(...)` for schema panels or metadata views.
- Use `exportResults("csv" | "json" | "xml")` for download buttons.

The `Load Demo + Large Tables` button is a good integration example for larger datasets: seed tables first, then run a query that combines joins, grouping, and aggregation.

### 4. Integrating TinySQL into custom web frontends

If you build a React, Vue, Svelte, or vanilla JS frontend, the architecture stays simple:

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

A robust client wrapper can look like this:

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
- Treat large result sets carefully because everything lives in browser memory.
- Use `executeMulti` if your editor accepts multiple SQL statements.
- For CSV/JSON/XML uploads, read the file in the browser and pass the text to `importFile(...)`.
- In the reference UI, Excel files are read with SheetJS and each sheet is imported as JSON.

### 5. What the repository examples demonstrate

`example_showcase.sql` shows that TinySQL already covers much more than simple `SELECT` queries:

- date and time functions such as `NOW`, `DATE_TRUNC`, and `EOMONTH`
- string functions such as `UPPER`, `LOWER`, `LENGTH`, and `SPLIT`
- regex and array helpers
- joins, grouping, `HAVING`, and temp tables
- JSON expressions and updates

This matters for developers: TinySQL is not just a teaching demo; it can serve as a compact embedded SQL core for realistic workflows.

### 6. Practical limits and recommendations

- The WASM examples keep SQL input bounded; in `cmd/query_files_wasm` the limit is 256 KiB.
- Query timeouts are enabled by default in the browser tool.
- Browser setups do not provide persistence by default; if you need persistence, implement it in the frontend or host Go process.
- Test against all relevant entry points: Go API, `database/sql`, WASM API, and UI.

## Summary

For Go-only apps, prefer the direct package API or the `database/sql` driver. For browser apps, follow the `cmd/query_files_wasm` pattern: compile to WASM, expose a small JS API, then let your frontend own the UI state while TinySQL owns the data and query execution.
