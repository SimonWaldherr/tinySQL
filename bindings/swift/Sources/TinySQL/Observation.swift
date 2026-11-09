import Combine
import Foundation

// Live queries: results that re-run when the database changes.
//
// SwiftUI:
//
//     @StateObject private var notes = LiveQuery<Note>(database: db, sql: "SELECT * FROM notes ORDER BY id")
//
//     List(notes.rows) { Text($0.body) }
//         .task { await notes.run() }     // starts on appear, cancels on disappear

extension Database {
    /// A stream of query results: the current result first, then a new element
    /// whenever a write has changed it. Unchanged results are not repeated, so a
    /// write to an unrelated table does not redraw a view.
    ///
    /// The stream finishes when the database closes or the consumer stops
    /// iterating, and throws if the query itself fails (for example because the
    /// table was dropped). The query runs on the database's executor, never on
    /// the consumer's.
    public nonisolated func observe(
        _ sql: String, parameters: [SQLValue] = []
    ) -> AsyncThrowingStream<QueryResult, Error> {
        observeMapped(sql, parameters: parameters) { $0 }
    }

    /// Like `observe(_:parameters:)`, decoding every result into `[T]`.
    public nonisolated func observe<T: Decodable & Sendable>(
        _ sql: String, parameters: [SQLValue] = [], as type: T.Type
    ) -> AsyncThrowingStream<[T], Error> {
        observeMapped(sql, parameters: parameters) { try $0.decode(type) }
    }

    nonisolated func observeMapped<Value: Sendable>(
        _ sql: String, parameters: [SQLValue],
        transform: @escaping @Sendable (QueryResult) throws -> Value
    ) -> AsyncThrowingStream<Value, Error> {
        let (stream, continuation) = AsyncThrowingStream<Value, Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
        let task = Task { [self] in
            do {
                // Subscribe before the first read, so a write between the two
                // cannot be missed.
                let changes = await self.changes()
                var previous: QueryResult?
                func emit() async throws {
                    let result = try await self.query(sql, parameters: parameters)
                    if result != previous {
                        previous = result
                        continuation.yield(try transform(result))
                    }
                }
                try await emit()
                for await _ in changes {
                    try Task.checkCancellation()
                    try await emit()
                }
                continuation.finish()
            } catch is CancellationError {
                continuation.finish()
            } catch TinySQLError.closed {
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
        continuation.onTermination = { _ in task.cancel() }
        return stream
    }
}

/// An `ObservableObject` that keeps the rows of one query current, for SwiftUI.
///
/// Call `run()` from `.task`: it loads the rows, then refreshes them after every
/// write until the view disappears. `rows` and `error` are published on the main
/// actor.
@MainActor
public final class LiveQuery<Element: Sendable>: ObservableObject {
    /// The latest rows; empty until the first load completes.
    @Published public private(set) var rows: [Element] = []
    /// True once the first result has arrived.
    @Published public private(set) var hasLoaded = false
    /// The error that ended the last `run()`, or nil.
    @Published public private(set) var error: (any Error)?

    private let makeStream: @Sendable () -> AsyncThrowingStream<[Element], Error>

    /// Observes `sql` on `database` and maps each result with `transform`.
    public init(
        database: Database, sql: String, parameters: [SQLValue] = [],
        transform: @escaping @Sendable (QueryResult) throws -> [Element]
    ) {
        makeStream = { database.observeMapped(sql, parameters: parameters, transform: transform) }
    }

    /// Loads rows and then follows changes until the surrounding task is
    /// cancelled, the database closes or the query fails (see `error`).
    public func run() async {
        error = nil
        do {
            for try await rows in makeStream() {
                self.rows = rows
                hasLoaded = true
            }
        } catch {
            self.error = error
        }
    }
}

extension LiveQuery where Element: Decodable {
    /// Observes `sql` and decodes each row into `Element`.
    public convenience init(database: Database, sql: String, parameters: [SQLValue] = []) {
        self.init(database: database, sql: sql, parameters: parameters) { try $0.decode(Element.self) }
    }
}
