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

    /// Opens or creates a durable database in `directory`. Every acknowledged
    /// write is persisted by the selected storage mode.
    public init(directory: URL, storage: StorageOptions = StorageOptions()) throws {
        let options = try storage.json(path: Self.filePath(directory))
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

    /// Runs a multi-statement script. Semicolons inside literals, comments and
    /// `CREATE TRIGGER ... BEGIN ... END` bodies do not split statements. Stops
    /// at the first failing statement; earlier statements stay applied.
    /// Returns the number of rows changed by the script.
    @discardableResult
    public func executeScript(_ script: String) throws -> Int64 {
        try requireOpen()
        try Task.checkCancellation()
        guard !script.utf8.contains(0) else { throw TinySQLError.invalidInput("SQL must not contain NUL") }
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
        let result = try sql.withCString { statement in
            try json.withCString { values in
                try Self.decode(TinySQLDatabaseExecuteBatch(handle, statement, values))
            }
        }
        return try statementResult(result).rowsAffected ?? 0
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
        _ = try Self.consume(TinySQLDatabaseClose(closing))
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
