import CTinySQL
import Foundation

/// An independently owned database. Actor isolation serializes operations away
/// from the main actor on a dedicated serial Dispatch queue. Cancellation is
/// checked before starting work; it does not interrupt a running native query.
public actor Database {
    private var handle: UInt64
    private let executor = DatabaseExecutor()
    private let encoder = JSONEncoder()

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

    /// Saves committed state. Uncommitted SQL transactions are not included.
    public func save(to url: URL) throws {
        try requireOpen()
        try Task.checkCancellation()
        let path = try Self.filePath(url)
        _ = try path.withCString { try Self.consume(TinySQLDatabaseSave(handle, $0)) }
    }

    /// Releases native resources. Repeated calls are harmless.
    public func close() throws {
        guard handle != 0 else { return }
        let closing = handle
        handle = 0
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
        return try sql.withCString { statement in
            try json.withCString { values in
                try Self.consume(query
                    ? TinySQLDatabaseQuery(handle, statement, values)
                    : TinySQLDatabaseExecute(handle, statement, values))
            }
        }
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
    }

    private nonisolated static func consume(_ pointer: UnsafeMutablePointer<CChar>?) throws -> Envelope {
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
        if let error = result.error, !error.isEmpty { throw TinySQLError.database(error) }
        return result
    }
}
