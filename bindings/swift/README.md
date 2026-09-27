# tinySQL for Swift and Xcode

A Swift package backed by a static Go XCFramework. It provides independent
`Database` actors, bound SQL parameters, typed values, durable storage modes,
scripts and batches, explicit snapshots, and automatic native resource cleanup.
The package wraps the shared [C ABI](../c/include/tinysql.h) that the Python
and Rust bindings use as well; see the
[language bindings guide](../../docs/language-bindings.md).

## Build and add to Xcode

Prerequisites: macOS, full Xcode with the iOS SDK selected by `xcode-select`,
and the Go toolchain required by the repository's `go.mod`.

```sh
# From the repository root:
make build-apple
```

This creates `bindings/swift/CTinySQL.xcframework` with these slices:

| Platform | Architectures | Deployment target |
| --- | --- | --- |
| macOS | arm64, x86_64 | 12.0 |
| iOS / iPadOS | arm64 | 15.0 |
| iOS Simulator | arm64, x86_64 | 15.0 |

In Xcode, choose **File → Add Package Dependencies → Add Local**, select
`bindings/swift`, and add the **TinySQL** product to your application target.
Then `import TinySQL`. SwiftPM supplies the C module and system linker settings;
no bridging header, Go installation on the device, server, or runtime download
is needed. The Go runtime is statically included in the application.

For a consuming Swift package, add `.package(path: "../tinySQL/bindings/swift")`
and `.product(name: "TinySQL", package: "swift")` to its target dependencies.
The package identity is the directory name `swift` for this local path.

For macOS-only development, use `make build-apple-macos`. Before building an iOS
app, run `make build-apple` again: each build replaces the generated framework
with exactly the selected platforms. Generated artifacts are ignored by Git;
build before resolving the local package. This repository does not currently
provide a hosted binary package for remote SwiftPM resolution. For distribution,
ship this package directory together with the generated XCFramework, or publish a
versioned XCFramework ZIP and use SwiftPM's URL/checksum binary target mechanism.

The packaging follows Apple's [XCFramework guide](https://developer.apple.com/documentation/xcode/creating-a-multi-platform-binary-framework-bundle)
and [binary Swift package guide](https://developer.apple.com/documentation/xcode/distributing-binary-frameworks-as-swift-packages).

## Use from Swift / SwiftUI

```swift
import Foundation
import TinySQL

// Call from a Task or another async context, including SwiftUI's .task.
let database = try await Database.open()
try await database.execute("CREATE TABLE notes (id INT, body TEXT)")
try await database.execute(
    "INSERT INTO notes VALUES (?, ?)",
    parameters: [.integer(1), .text("Hello from Swift 👋")]
)
let result = try await database.query(
    "SELECT id, body FROM notes WHERE id = ?",
    parameters: [.integer(1)]
)
// result.columns == ["id", "body"]
// result.rows == [[.integer(1), .text("Hello from Swift 👋")]]

let directory = try FileManager.default.url(
    for: .applicationSupportDirectory, in: .userDomainMask,
    appropriateFor: nil, create: true
)
let snapshot = directory.appendingPathComponent("notes.tinysql")
try await database.save(to: snapshot)
try await database.close()

let restored = try await Database.open(snapshot: snapshot)
let notes = try await restored.query("SELECT * FROM notes")
try await restored.close()
```

### Durable databases, scripts and batches

```swift
let directory = try FileManager.default.url(
    for: .applicationSupportDirectory, in: .userDomainMask,
    appropriateFor: nil, create: true
).appendingPathComponent("notes-db", isDirectory: true)

// Every acknowledged write is persisted; no explicit save is needed.
let database = try await Database.open(directory: directory, storage: StorageOptions(mode: .wal))
try await database.executeScript("""
    CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body TEXT);
    CREATE INDEX IF NOT EXISTS notes_body ON notes (body);
    """)
// One native call, statement prepared once:
try await database.executeBatch(
    "INSERT INTO notes VALUES (?, ?)",
    parameterSets: [[1, "first"], [2, "second"]]
)
let result = try await database.query("SELECT id, body FROM notes ORDER BY id")
let body = result.value(row: 0, column: "body")?.stringValue   // "first"
try await database.close()
```

| `StorageMode` | Use it for |
| --- | --- |
| `.wal` | Small to medium app databases: in-memory tables, write-ahead log, checkpoints |
| `.advancedWAL` | Row-level WAL with grouped transaction commits |
| `.disk` / `.json` | One file per table (binary or readable JSON) |
| `.index` / `.hybrid` | Larger datasets with a bounded cache (`maxMemoryBytes`) |
| `.pagedIndex` | Read-mostly artifacts such as offline map tiles |

`StorageOptions` also sets `readOnly`, `walSync` (`.full` or the faster
`.normal`) and a 32-byte `encryptionKey` (AES-256-GCM for table files of
`.disk`, `.json`, `.index` and `.hybrid`; keep the key in the Keychain).
`sync()` flushes table-file modes explicitly; `close()` does so too.

`SQLValue` is expressible by integer, floating-point, string and Boolean
literals, so `parameters: [1, "Ada", 2.5, true]` works. Read values with
`int64Value`, `doubleValue`, `stringValue`, `boolValue`, `dataValue` and
`isNull`, or address result columns by name with `value(row:column:)` and
`columnIndex(_:)`. `await database.isInTransaction` reflects the engine's
transaction state, including `BEGIN` issued as SQL.

Keep the actor in a model or service to share it across views. Separate actors
own separate databases. Actor methods serialize access on a dedicated Dispatch
queue, keeping blocking native calls off Swift's cooperative executor. This uses
Swift's [custom actor executor model](https://github.com/swiftlang/swift-evolution/blob/main/proposals/0392-custom-actor-executors.md).
Prefer `await Database.open(snapshot:)` when opening from UI code; it also moves
snapshot loading off the main actor. The synchronous `Database(snapshot:)`
initializer remains available for callers already running on a suitable worker.
Errors are thrown, with database messages exposed
through `TinySQLError.database` and `LocalizedError`.

`SQLValue` supports NULL, signed 64-bit integers, finite doubles, Unicode text,
booleans and `Data` BLOBs. Integer arithmetic and `SUM` over integer columns
return `.integer`; `/` and `AVG` return `.real`. Bind values with `?`, `$1` or `:1` placeholders instead
of interpolating user text into SQL. Rows are positional arrays in column order;
integers are not passed through `Double`, and BLOBs retain zero bytes. Other
engine values follow the existing database/sql driver's conversion rules
(for example, timestamps become JSON strings). The bridge tags doubles explicitly
so `.real(1)` and large integral doubles retain their type across JSON encoding.
SQL column coercion still follows the engine's rules.

## Lifetime, concurrency and persistence

- `close()` is idempotent in Swift; further operations throw `.closed`. `deinit`
  also releases the native handle. Explicit close lets you observe close errors.
- Every call uses a pinned SQL connection, so `BEGIN`, `COMMIT`, and `ROLLBACK`
  span calls. Do not share an actor with unrelated tasks during a multi-call
  transaction: suspension between calls allows those tasks to join it. Use a
  dedicated actor for that unit of work.
- `Database()` is in-memory. `Database(directory:storage:)` opens or creates a
  durable database directory. `Database(snapshot:)` loads an **existing tinySQL
  snapshot**, not a SQLite file. A missing/corrupt file throws. `save(to:)` is
  explicit and saves committed state only. Closing does not automatically save.
  Create the parent directory and use a writable URL in the application sandbox.
- Queries currently materialize the full result and cross the ABI as JSON.
  Use SQL limits/pagination for large results. A running query is synchronous
  within its actor. Cancelled tasks are rejected before SQL or save operations;
  Swift task cancellation does not interrupt an already running native operation.
  `close()` remains available in cancelled tasks for cleanup and discards any
  unfinished transaction. The async opener closes its handle if cancelled while
  loading.
- Supported targets are macOS and iOS/iPadOS, including iOS Simulator. Catalyst,
  watchOS, tvOS and visionOS are not included. The default engine build does not
  enable the optional `sqliteimport` backend.

## Test

```sh
make test-swift
# After a full framework build, preserve all slices and just run tests:
go test -race ./bindings/c
swift test --package-path bindings/swift
```

Tests exercise the real native library: instance isolation, Unicode and quoted
parameters, Int64/Double precision, NULL/BLOB values, save/load, gzip integrity,
transactions, durable storage modes, scripts, batches, literal parameters,
concurrent calls, task cancellation, errors and close handling.
CI also compiles in Swift 6 mode and builds with Xcode's iOS SDK.
The C ABI is documented in
[`tinysql.h`](../c/include/tinysql.h) (module `CTinySQL`). Each returned C
buffer must be freed with `TinySQLDatabaseFree`; the Swift wrapper does this
with `defer`, including error paths.
