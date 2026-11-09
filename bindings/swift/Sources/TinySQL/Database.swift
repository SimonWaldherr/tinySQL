import CTinySQL
import Foundation

/// An independently owned database. Actor isolation serializes operations away
/// from the main actor on a dedicated serial Dispatch queue. Cancellation is
/// checked before starting work; it does not interrupt a running native query.
public actor Database {
    private var handle: UInt64
    private let executor = DatabaseExecutor()
    private let encoder = JSONEncoder()

    /// True while the database is inside `BEGIN` ... `COMMIT`/`ROLLBACK`,
    /// including transactions started by SQL text or scripts.
    public private(set) var isInTransaction = false

    // Change notification: `changes()` subscribers are told after writes have
    // settled (see `publishChanges`). A write inside a transaction is held back
    // until the transaction ends.
    private var observers: [Int: AsyncStream<Void>.Continuation] = [:]
    private var nextObserverID = 0
    private var hasUnpublishedWrite = false

    public nonisolated var unownedExecutor: UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }

    /// Creates an in-memory database, or loads an existing tinySQL snapshot.
    /// Saving is explicit; opening a snapshot does not enable automatic saving.
    public init(snapshot: URL? = nil) throws {
        let path = try snapshot.map(Self.filePath) ?? ""
        let result = try path.withCString { try Self.consume(TinySQLDatabaseOpen($0)) }
        guard let handle = result.handle, handle != 0 else { throw TinySQLError.invalidResponse }
        self.handle = handle
    }

    /// Loads on a Dispatch worker, including when called from the main actor.
    public static func open(snapshot: URL? = nil) async throws -> Database {
        try Task.checkCancellation()
        let database: Database = try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .utility).async {
                continuation.resume(with: Result { try Database(snapshot: snapshot) })
            }
        }
        if Task.isCancelled {
            try? await database.close()
            throw CancellationError()
        }
        return database
    }

    /// Opens or creates a durable database at `directory`. Every acknowledged
    /// write is persisted by the selected storage mode. The table-file modes keep
    /// their files inside that directory. `.wal` and `.advancedWAL` use the URL as
    /// the database file unless a directory already exists there, in which case
    /// they keep `StorageOptions.logDatabaseFileName` inside it.
    public init(directory: URL, storage: StorageOptions = StorageOptions()) throws {
        let options = try storage.json(path: Self.storagePath(directory, mode: storage.mode))
        let result = try options.withCString { try Self.consume(TinySQLDatabaseOpenWithOptions($0)) }
        guard let handle = result.handle, handle != 0 else { throw TinySQLError.invalidResponse }
        self.handle = handle
    }

    /// Opens a durable database on a Dispatch worker, including when called
    /// from the main actor.
    public static func open(directory: URL, storage: StorageOptions = StorageOptions()) async throws -> Database {
        try Task.checkCancellation()
        let database: Database = try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .utility).async {
                continuation.resume(with: Result { try Database(directory: directory, storage: storage) })
            }
        }
        if Task.isCancelled {
            try? await database.close()
            throw CancellationError()
        }
        return database
    }

    deinit {
        if handle != 0 { TinySQLDatabaseFree(TinySQLDatabaseClose(handle)) }
    }

    /// Executes one statement using bound parameters. Use query for RETURNING.
    @discardableResult
    public func execute(_ sql: String, parameters: [SQLValue] = []) throws -> Int64 {
        let result = try call(sql, parameters: parameters, query: false)
        return result.rowsAffected ?? 0
    }

    public func query(_ sql: String, parameters: [SQLValue] = []) throws -> QueryResult {
        let result = try call(sql, parameters: parameters, query: true)
        return QueryResult(columns: result.columns ?? [], rows: result.rows ?? [])
    }

    /// Runs synchronous database work atomically on this actor's executor.
    /// The isolated closure cannot suspend, so other actor calls cannot join
    /// the transaction. Do not issue BEGIN/COMMIT/ROLLBACK inside the closure.
    /// Errors and cancellation attempt rollback while the transaction is active;
    /// the original error is preserved.
    public func transaction<T: Sendable>(
        _ body: @Sendable (isolated Database) throws -> T
    ) throws -> T {
        try requireOpen()
        try Task.checkCancellation()
        guard !isInTransaction else {
            throw TinySQLError.invalidInput("A transaction is already active")
        }
        try execute("BEGIN")
        do {
            let value = try body(self)
            try Task.checkCancellation()
            try execute("COMMIT")
            return value
        } catch {
            // Cleanup must still run on a cancelled task. execute() rejects
            // cancellation before invoking the C ABI, so bypass that check.
            if handle != 0 && isInTransaction {
                _ = try? "ROLLBACK".withCString {
                    try statementResult(Self.decode(TinySQLDatabaseExecute(handle, $0, nil)))
                }
                publishChanges()
            }
            throw error
        }
    }

    /// Runs a multi-statement script. Semicolons inside literals, comments and
    /// `CREATE TRIGGER ... BEGIN ... END` bodies do not split statements. Stops
    /// at the first failing statement; earlier statements stay applied.
    /// Returns the number of rows changed by the script.
    @discardableResult
    public func executeScript(_ script: String) throws -> Int64 {
        try requireOpen()
        try Task.checkCancellation()
        guard !script.utf8.contains(0) else { throw TinySQLError.invalidInput("SQL must not contain NUL") }
        // A failing script keeps its earlier statements, so it counts as a write.
        hasUnpublishedWrite = true
        defer { publishChanges() }
        let result = try script.withCString { value in
            try Self.decode(TinySQLDatabaseExecuteScript(handle, value))
        }
        return try statementResult(result).rowsAffected ?? 0
    }

    /// Prepares `sql` once and executes it for every parameter array in one
    /// native call. Wrap it in `BEGIN`/`COMMIT` to make the batch atomic.
    /// Returns the number of changed rows.
    @discardableResult
    public func executeBatch(_ sql: String, parameterSets: [[SQLValue]]) throws -> Int64 {
        try requireOpen()
        try Task.checkCancellation()
        guard !parameterSets.isEmpty else { return 0 }
        guard !sql.utf8.contains(0) else { throw TinySQLError.invalidInput("SQL must not contain NUL") }
        let data = try encoder.encode(parameterSets)
        let json = String(decoding: data, as: UTF8.self)
        // Earlier rows of a failing batch stay applied outside a transaction.
        hasUnpublishedWrite = true
        defer { publishChanges() }
        let result = try sql.withCString { statement in
            try json.withCString { values in
                try Self.decode(TinySQLDatabaseExecuteBatch(handle, statement, values))
            }
        }
        return try statementResult(result).rowsAffected ?? 0
    }

    /// Consumes a sequence in bounded native batches, including lazy sequences.
    /// Earlier chunks remain applied on failure unless wrapped in transaction.
    /// Native row numbers in errors are relative to the failing chunk.
    @discardableResult
    public func executeBatchChunked<S: Sequence>(
        _ sql: String, parameterSets: S, batchSize: Int
    ) throws -> Int64 where S.Element == [SQLValue] {
        try requireOpen()
        try Task.checkCancellation()
        guard batchSize > 0 else {
            throw TinySQLError.invalidInput("batchSize must be positive")
        }
        guard !sql.utf8.contains(0) else { throw TinySQLError.invalidInput("SQL must not contain NUL") }
        var chunk: [[SQLValue]] = []
        var affected: Int64 = 0
        for row in parameterSets {
            try Task.checkCancellation()
            chunk.append(row)
            if chunk.count == batchSize {
                affected += try executeBatch(sql, parameterSets: chunk)
                chunk.removeAll(keepingCapacity: true)
            }
        }
        if !chunk.isEmpty {
            affected += try executeBatch(sql, parameterSets: chunk)
        }
        return affected
    }

    /// Flushes durable storage modes to disk; a no-op for other databases.
    public func sync() throws {
        try requireOpen()
        _ = try Self.consume(TinySQLDatabaseSync(handle))
    }

    /// Saves committed state. Uncommitted SQL transactions are not included.
    public func save(to url: URL) throws {
        try requireOpen()
        try Task.checkCancellation()
        let path = try Self.filePath(url)
        _ = try path.withCString { try Self.consume(TinySQLDatabaseSave(handle, $0)) }
    }

    /// Releases native resources. Repeated calls are harmless. An unfinished
    /// transaction is discarded; durable storage modes are flushed.
    public func close() throws {
        guard handle != 0 else { return }
        let closing = handle
        handle = 0
        isInTransaction = false
        hasUnpublishedWrite = false
        let finished = observers.values
        observers.removeAll()
        for continuation in finished { continuation.finish() }
        _ = try Self.consume(TinySQLDatabaseClose(closing))
    }

    /// A stream that yields once after writes made through this actor have
    /// settled: after each `execute`, `executeBatch` and `executeScript` outside a
    /// transaction, and after the `COMMIT` (or `ROLLBACK`) that ends one. Bursts
    /// coalesce into a single pending element, so a slow consumer never builds a
    /// backlog. The stream finishes when the database closes.
    ///
    /// It is a coarse "something may have changed" signal, not a per-table log:
    /// re-run your queries (see `observe(_:parameters:)`) when it fires. Writes
    /// issued through `query(_:)` (for example `INSERT ... RETURNING`) and writes
    /// made by other processes do not notify.
    public func changes() -> AsyncStream<Void> {
        let (stream, continuation) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        guard handle != 0 else {
            continuation.finish()
            return stream
        }
        let id = nextObserverID
        nextObserverID += 1
        observers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            guard let self else { return }
            Task { await self.removeObserver(id) }
        }
        return stream
    }

    private func removeObserver(_ id: Int) {
        observers[id] = nil
    }

    private func publishChanges() {
        guard hasUnpublishedWrite, !isInTransaction else { return }
        hasUnpublishedWrite = false
        for continuation in observers.values { continuation.yield(()) }
    }

    private func requireOpen() throws {
        guard handle != 0 else { throw TinySQLError.closed }
    }

    private func call(_ sql: String, parameters: [SQLValue], query: Bool) throws -> Envelope {
        try requireOpen()
        try Task.checkCancellation()
        guard !sql.utf8.contains(0) else { throw TinySQLError.invalidInput("SQL must not contain NUL") }
        let data = try encoder.encode(parameters)
        let json = String(decoding: data, as: UTF8.self)
        if !query {
            hasUnpublishedWrite = true
        }
        defer { if !query { publishChanges() } }
        let result = try sql.withCString { statement in
            try json.withCString { values in
                try Self.decode(query
                    ? TinySQLDatabaseQuery(handle, statement, values)
                    : TinySQLDatabaseExecute(handle, statement, values))
            }
        }
        return try statementResult(result)
    }

    /// Records the transaction state reported with every statement response,
    /// including failed ones, then surfaces a database error.
    private func statementResult(_ result: Envelope) throws -> Envelope {
        isInTransaction = result.inTransaction ?? false
        if let error = result.error, !error.isEmpty { throw TinySQLError.database(error) }
        return result
    }

    /// The path handed to the engine. The table-file modes use `url` as their
    /// storage directory. The log-based modes (`.wal`, `.advancedWAL`) use it as
    /// the database *file* (with the log beside it), which fails if a directory is
    /// already there, so an existing directory holds the file instead. That makes
    /// a folder prepared by `DatabaseDirectory` work for every mode.
    private nonisolated static func storagePath(_ url: URL, mode: StorageMode) throws -> String {
        let path = try filePath(url)
        guard mode == .wal || mode == .advancedWAL else { return path }
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory), isDirectory.boolValue else {
            return path
        }
        return url.appendingPathComponent(StorageOptions.logDatabaseFileName).path
    }

    private nonisolated static func filePath(_ url: URL) throws -> String {
        guard url.isFileURL, !url.path.isEmpty, !url.path.utf8.contains(0) else {
            throw TinySQLError.invalidInput("Expected a nonempty file URL without NUL")
        }
        return url.path
    }

    private struct Envelope: Decodable {
        let error: String?
        let handle: UInt64?
        let columns: [String]?
        let rows: [[SQLValue]]?
        let rowsAffected: Int64?
        let inTransaction: Bool?
    }

    private nonisolated static func consume(_ pointer: UnsafeMutablePointer<CChar>?) throws -> Envelope {
        let result = try decode(pointer)
        if let error = result.error, !error.isEmpty { throw TinySQLError.database(error) }
        return result
    }

    /// Takes ownership of a response buffer and decodes it without
    /// interpreting its error member.
    private nonisolated static func decode(_ pointer: UnsafeMutablePointer<CChar>?) throws -> Envelope {
        guard let pointer else { throw TinySQLError.invalidResponse }
        defer { TinySQLDatabaseFree(pointer) }
        // The decoder borrows the C buffer synchronously. `defer` releases it
        // only after all Swift values have been decoded, without a full copy.
        let data = Data(bytesNoCopy: pointer, count: strlen(pointer), deallocator: .none)
        let result: Envelope
        do {
            result = try JSONDecoder().decode(Envelope.self, from: data)
        } catch {
            throw TinySQLError.invalidResponse
        }
        return result
    }
}
