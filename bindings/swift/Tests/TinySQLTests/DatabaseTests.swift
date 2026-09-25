import Foundation
import XCTest
@testable import TinySQL

final class DatabaseTests: XCTestCase {
    func testBindingPersistenceAndIsolation() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT, name TEXT, payload BLOB, optional TEXT)")
        let values: [SQLValue] = [.integer(Int64.max), .text("Grüße ' 🦊"), .blob(Data([0, 1, 255])), .null]
        try await database.execute("INSERT INTO items VALUES (?, ?, ?, ?)", parameters: values)
        let result = try await database.query("SELECT id, name, payload, optional FROM items WHERE id = ?", parameters: [.integer(Int64.max)])
        XCTAssertEqual(result.columns, ["id", "name", "payload", "optional"])
        XCTAssertEqual(result.rows, [values])

        let other = try Database()
        do {
            _ = try await other.query("SELECT * FROM items")
            XCTFail("Instances must be independent")
        } catch TinySQLError.database { }
        try await other.close()

        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let snapshot = directory.appendingPathComponent("Grüße.snapshot")
        try await database.save(to: snapshot)
        let restored = try Database(snapshot: snapshot)
        let loaded = try await restored.query("SELECT id, name, payload, optional FROM items")
        XCTAssertEqual(loaded.rows, [values])
        try await restored.close()
        try await database.close()
        try await database.close()
        do {
            _ = try await database.query("SELECT 1")
            XCTFail("Closed database accepted query")
        } catch TinySQLError.closed { }
    }

    func testTransactionsEmptyResultsAndValidation() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT)")
        try await database.execute("BEGIN")
        try await database.execute("INSERT INTO items VALUES (?)", parameters: [.integer(42)])
        try await database.execute("ROLLBACK")
        let empty = try await database.query("SELECT id FROM items")
        XCTAssertEqual(empty.columns, ["id"])
        XCTAssertEqual(empty.rows, [])
        do {
            _ = try await database.query("SELECT 1\0ignored")
            XCTFail("NUL was accepted")
        } catch TinySQLError.invalidInput { }
        do {
            try await database.execute("INSERT INTO items VALUES (?)", parameters: [.real(.infinity)])
            XCTFail("Non-finite number was accepted")
        } catch TinySQLError.invalidInput { }
        do {
            try await database.save(to: URL(string: "https://example.com/db")!)
            XCTFail("Non-file URL was accepted")
        } catch TinySQLError.invalidInput { }
        try await database.close()
    }

    func testConcurrentCalls() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT)")
        try await withThrowingTaskGroup(of: Void.self) { group in
            for id in 0..<25 {
                group.addTask {
                    try await database.execute("INSERT INTO items VALUES (?)", parameters: [.integer(Int64(id))])
                }
            }
            try await group.waitForAll()
        }
        let rows = try await database.query("SELECT id FROM items ORDER BY id")
        XCTAssertEqual(rows.rows, (0..<25).map { [.integer(Int64($0))] })
        try await database.close()
    }

    func testValuesAndMissingSnapshot() throws {
        let values: [SQLValue] = [.null, .boolean(true), .integer(Int64.min), .real(1.25), .real(1), .real(1e20), .text("a\0b"), .blob(Data())]
        XCTAssertEqual(try JSONDecoder().decode([SQLValue].self, from: JSONEncoder().encode(values)), values)
        for json in ["{\"real\":1,\"unknown\":2}", "{\"blob\":\"!\"}", "{\"real\":\"1\"}", "{}"] {
            XCTAssertThrowsError(try JSONDecoder().decode(SQLValue.self, from: Data(json.utf8)))
        }
        let missing = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        XCTAssertThrowsError(try Database(snapshot: missing))
    }

    func testIntegralRealValuesAcrossNativeBridge() async throws {
        let database = try await Database.open()
        try await database.execute("CREATE TABLE numbers (value FLOAT)")
        let values: [SQLValue] = [.real(1), .real(1.25), .real(1e20)]
        for value in values {
            try await database.execute("INSERT INTO numbers VALUES (?)", parameters: [value])
        }
        let result = try await database.query("SELECT value FROM numbers ORDER BY value")
        XCTAssertEqual(result.rows, values.map { [$0] })
        try await database.close()
    }

    func testCancellationDoesNotWriteAndStillAllowsClose() async throws {
        let database = try await Database.open()
        try await database.execute("CREATE TABLE items (id INT)")
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            do {
                try await database.execute("INSERT INTO items VALUES (1)")
                XCTFail("Cancelled task performed a write")
            } catch is CancellationError { }
        }
        try await task.value
        let result = try await database.query("SELECT * FROM items")
        XCTAssertTrue(result.rows.isEmpty)
        let cleanup = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await database.close()
        }
        try await cleanup.value
    }

    @MainActor
    func testExecutorRunsAwayFromMainActor() async throws {
        let database = try await Database.open()
        let onMainThread = await database.isOnMainThreadForTesting()
        XCTAssertFalse(onMainThread)
        try await database.close()
    }
}

private extension Database {
    func isOnMainThreadForTesting() -> Bool { Thread.isMainThread }
}
