# tinySQL for Swift and Xcode

A Swift package backed by a static Go XCFramework. It provides independent
`Database` actors, bound SQL parameters, typed values, `Codable` models,
live queries for SwiftUI, durable storage modes with the right app directories
and Keychain-held keys, scripts and batches, explicit snapshots, and automatic
native resource cleanup.
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
build before resolving the local package.

### Distribute without a local build

SwiftPM resolves remote packages from a `Package.swift` at the repository root and
downloads binary targets from a URL, so a release is a zipped XCFramework plus a
manifest that names its URL and checksum. One command produces both:

```sh
make package-apple-release URL=https://github.com/you/tinysql-swift/releases/download/1.0.0/CTinySQL.xcframework.zip
```

It builds all slices and writes `bindings/apple/build/release/` with
`CTinySQL.xcframework.zip`, a ready `Package.swift` (URL and checksum filled in),
`Sources/` and `LICENSE`. Upload the zip to that URL, commit the other three to the
repository that serves the package, and tag a version. Apps then use
**File → Add Package Dependencies…** with your repository URL, with no Go
toolchain and no local build. The archive contains the macOS, iOS and simulator
variants; Xcode selects the relevant platform and architecture for the app.

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

## Typed models

Query results decode into `Codable` types, and records insert from them. No
mapping code, no manual column indexes:

```swift
struct Note: Codable, Sendable, Identifiable {
    var id: Int
    var body: String
    var createdAt: Date
    var pinned: Bool
    var tags: [String]          // stored as JSON text
    var attachment: URL?        // NULL <-> nil
}

try await database.execute("""
    CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body TEXT, created_at TIMESTAMP,
                                      pinned BOOL, tags TEXT, attachment TEXT)
    """)
try await database.insert(
    Note(id: 1, body: "Hello", createdAt: .now, pinned: true, tags: ["a"], attachment: nil),
    into: "notes", columnNaming: .snakeCase)               // createdAt -> created_at
let pinned = try await database.query(
    "SELECT * FROM notes WHERE pinned = ? ORDER BY created_at DESC", parameters: [true], as: Note.self)
let total = try await database.query("SELECT COUNT(*) FROM notes").decodeFirst(Int.self)
```

- **Columns to properties**: names match ignoring case and underscores, so
  `created_at`, `CREATED_AT` and `createdAt` all fill `createdAt`. `CodingKeys` work
  as usual and are the way to rename a column.
- **Types**: all fixed-width integers (checked for overflow), `Float`/`Double`,
  `Bool`, `String`, `Data` (BLOB), `Date`, `UUID`, `URL` and `RawRepresentable`
  enums of those. Any other `Codable` value (arrays, nested structs, dictionaries)
  is stored as JSON text in a TEXT column. `UInt` and `UInt64` insert only when
  the value fits a signed 64-bit integer, otherwise `insert` throws.
- **Dates** are RFC 3339 text in UTC with microseconds
  (`2026-11-09T17:14:57.250000Z`), compatible with TIMESTAMP columns. In TEXT
  columns, consistently using this fixed UTC format preserves chronological
  order for `ORDER BY` and `BETWEEN`.
  Reading also accepts offsets, a date alone, a space instead of `T`, and numbers
  as seconds since 1970.
- **Parameters** accept `SQLValue(date)`, `.date(_:)`, `.uuid(_:)`, `.url(_:)`
  and literals including `nil`: `parameters: [1, "Ada", nil, .date(.now)]`.
- **Inserting**: `insert` leaves nil properties out so column defaults apply;
  `insertAll` writes one prepared statement for many records, atomically, using the
  union of their columns (NULL where a record has none). Table and column names must
  be plain identifiers because they are part of the SQL text; values are always bound.
- A decoding error names the offending column and expected type through
  `DecodingError`; a database error stays a `TinySQLError`.

## SwiftUI and live queries

`changes()` yields after writes settle (after each `execute`, `executeBatch` and
`executeScript` outside a transaction, and after the `COMMIT` or `ROLLBACK` that
ends one), `observe` turns a query into a stream that re-runs on those signals, and
`LiveQuery` publishes it to SwiftUI:

```swift
struct NotesView: View {
    @StateObject private var notes: LiveQuery<Note>

    init(database: Database) {
        _notes = StateObject(wrappedValue: LiveQuery(
            database: database, sql: "SELECT * FROM notes ORDER BY created_at DESC"))
    }

    var body: some View {
        List(notes.rows) { Text($0.body) }
            .overlay { if !notes.hasLoaded { ProgressView() } }
            .task { await notes.run() }        // follows changes while the view is on screen
    }
}
```

Without SwiftUI, iterate directly: `for try await notes in database.observe("SELECT * FROM notes", as: Note.self) { ... }`.
Details:

- The current result arrives first; a re-run that produces an identical result is
  not yielded, so a write to an unrelated table does not redraw a view.
- Queries run on the database's executor and the stream throws if the query fails
  (for example because its table was dropped); it finishes when the database closes
  or the consumer stops. `LiveQuery.error` holds that error.
- The signal is "something may have changed", not a per-table log. Writes issued
  through `query(_:)` (such as `INSERT ... RETURNING`) and writes by other processes
  do not notify; use `execute` for writes.
- Bursts coalesce, so a consumer that is busy never builds a backlog, and a
  transaction notifies once, after it ends.

## Where to put the database

`DatabaseDirectory` creates the folder and applies Apple's conventions:

```swift
// <Application Support>/<bundle id>/notes, usable in the background after first unlock
let directory = try DatabaseDirectory.applicationSupport("notes")
let database = try await Database.open(directory: directory)

// Shared between the app, its widgets and extensions
let shared = try DatabaseDirectory.appGroup("group.com.example.app", "notes")
```

| Option | Default | Meaning |
| --- | --- | --- |
| `fileProtection` | `.completeUntilFirstUserAuthentication` | iOS Data Protection class of the directory, inherited by the files in it. `.complete` makes the database unreadable to background work on a locked device. Ignored on macOS. |
| `excludeFromBackup` | `false` | Keep a rebuildable cache out of iCloud and device backups; leave it off for user data. |

`Database.openInApplicationSupport("notes")` does both steps. For `.wal` and
`.advancedWAL`, `directory:` is the database *file* unless a directory already exists
there, in which case the file `database.tinysql` lives inside it (so a folder
prepared by `DatabaseDirectory` works for every mode). One process owns writes to a
durable database; widgets and extensions should read through the app or open it
read-only in a table-file mode.

### Encryption keys in the Keychain

```swift
let key = try KeychainKeyStore(service: "com.example.app").loadOrCreateKey()   // 32 random bytes, created once
let database = try await Database.open(
    directory: try DatabaseDirectory.applicationSupport("vault"),
    storage: StorageOptions(mode: .disk, encryptionKey: key))
```

The key comes from the system's secure random generator and is not synchronized
through iCloud Keychain. On iOS and the macOS data protection keychain it is
available after first unlock (`ThisDeviceOnly`); backup restoration can restore it
to the same device, but cannot migrate it to another device. The default macOS
login keychain follows its own access and backup policies. Losing the key loses
the data. Use `accessGroup` to share it with your other apps, and
`useDataProtectionKeychain: true` on macOS for signed or sandboxed apps with a
keychain access group. See [Apple's keychain implementation notes](https://developer.apple.com/documentation/technotes/tn3137-on-mac-keychains)
and [accessibility rules](https://developer.apple.com/documentation/security/ksecattraccessible).
Unit tests without a host app cannot reach the
Keychain on iOS (`errSecMissingEntitlement`), so run tests that use it inside an
app target.

## Transaction scopes

Use an isolated synchronous closure to keep a unit of work on the database's
serial executor without actor reentrancy:

```swift
let inserted = try await database.transaction { db in
    try db.executeBatch("INSERT INTO notes VALUES (?, ?)",
                        parameterSets: [[3, "three"], [4, "four"]])
}
```

The closure receives `isolated Database`, so its database calls need no `await`.
It cannot suspend for network or UI work; prepare that data before entering.
The helper commits on success and attempts rollback on a thrown error or
cancellation observed before commit, while the transaction is still active. Cleanup still runs in cancelled tasks. Nested scopes
are rejected. Do not issue transaction-control SQL inside the closure.
A cancellation arriving after the native commit has started cannot undo it.

## Lifetime, concurrency and persistence

- `close()` is idempotent in Swift; further operations throw `.closed`. `deinit`
  also releases the native handle. Explicit close lets you observe close errors.
- Every call uses a pinned SQL connection, so `BEGIN`, `COMMIT`, and `ROLLBACK`
  span calls. Prefer `transaction { db in ... }` for an isolated synchronous
  unit of work. Manual multi-call transactions still need a dedicated actor:
  suspension between calls allows other tasks to join them.
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
# The same suite on an iOS Simulator (the package scheme builds for iOS):
xcodebuild test -scheme TinySQL -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
    -derivedDataPath .build/xcode-sim CODE_SIGNING_ALLOWED=NO   # run in bindings/swift
```

Tests exercise the real native library: instance isolation, Unicode and quoted
parameters, Int64/Double precision, NULL/BLOB values, save/load, gzip integrity,
transactions, durable storage modes, scripts, batches, literal parameters,
concurrent calls, task cancellation, errors and close handling, `Codable` models,
dates and change notification. A few tests skip where the environment cannot
provide App Groups or a host-less Keychain (the iOS Simulator).
CI also compiles in Swift 6 mode and builds with Xcode's iOS SDK.
The C ABI is documented in
[`tinysql.h`](../c/include/tinysql.h) (module `CTinySQL`). Each returned C
buffer must be freed with `TinySQLDatabaseFree`; the Swift wrapper does this
with `defer`, including error paths.

## Large imports and wide results

`executeBatchChunked` accepts a `Sequence`, including a lazy producer.
Construct lazy inputs inside the isolated transaction closure to keep generation
and ingestion on the database executor:

```swift
try await database.execute("CREATE TABLE events (id INT PRIMARY KEY, body TEXT)")
try await database.transaction { db in
    try db.executeBatchChunked("INSERT INTO events VALUES (?, ?)",
        parameterSets: (0..<100_000).lazy.map { [.integer(Int64($0)), .text("event")] },
        batchSize: 1000)
}
let result = try await database.query("SELECT body FROM events LIMIT 100")
if let column = result.columnIndex("BODY") {
    for row in result.rows.indices {
        let value = result.valueAt(row: row, column: column)
        print(value as Any)
    }
}
```

Existing array-based `executeBatch` calls retain their single native call.
`batchSize` must be positive; each chunk prepares the statement separately.
Earlier chunks remain applied on errors or cancellation outside a transaction.
Native error row numbers are relative to the failing chunk. Parameter memory
scales with chunk size and row width; query results still materialize in full.
Resolve a column name once and use the checked integer accessor in row loops;
invalid row or column positions return nil, as named access does.
